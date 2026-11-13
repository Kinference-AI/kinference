//! `Recorded`: the API server over objects the test records up front.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use futures::StreamExt as _;
use futures::stream::{self, BoxStream};
use kube::runtime::watcher;

use super::{ApiServer, Object};

/// (group/Kind, namespace, name).
pub type Key = (String, String, String);

/// The key of `K`'s object `name` in `namespace`.
pub fn key<K: Object>(namespace: &str, name: &str) -> Key {
    (kind::<K>(), namespace.to_string(), name.to_string())
}

fn kind<K: Object>() -> String {
    format!("{}/{}", K::group(&()), K::kind(&()))
}

/// One write, in the order made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Write {
    Create(Key),
    Replace(Key),
    Apply { key: Key, manager: String },
    Delete(Key),
    PatchStatus { key: Key, status: serde_json::Value },
}

/// Reads answer from the recording; writes change it and are kept in
/// order. A missing object, or a kind marked `without`, is a 404; a
/// `create` of an existing object or a `replace` with a stale
/// `resourceVersion` is a 409. Every write stamps a fresh `resourceVersion`.
/// A watch lists the recording as of the call and then stays open. Label
/// selectors are equality terms (`k=v`, comma-separated), the only form
/// the loops use.
#[derive(Clone, Default)]
pub struct Recorded(Arc<Mutex<Inner>>);

#[derive(Default)]
struct Inner {
    objects: BTreeMap<Key, serde_json::Value>,
    /// Kinds whose CRD is not installed.
    absent: HashSet<String>,
    /// Objects whose writes answer 500.
    failing: HashSet<Key>,
    writes: Vec<Write>,
    version: u64,
}

impl Recorded {
    /// Record `objects` as the API server's current state.
    pub fn with<K: Object>(self, objects: impl IntoIterator<Item = K>) -> Self {
        {
            let mut inner = self.lock();
            for object in objects {
                let namespace = object.meta().namespace.clone().unwrap_or_default();
                let name = object.meta().name.clone().unwrap_or_default();
                inner.store(key::<K>(&namespace, &name), encode(&object));
            }
        }
        self
    }

    /// `K`'s CRD is not installed: its list and watch answer 404.
    pub fn without<K: Object>(self) -> Self {
        self.lock().absent.insert(kind::<K>());
        self
    }

    /// Writes to this object answer 500 until `heal`.
    pub fn failing<K: Object>(&self, namespace: &str, name: &str) {
        self.lock().failing.insert(key::<K>(namespace, name));
    }

    pub fn heal(&self) {
        self.lock().failing.clear();
    }

    pub fn writes(&self) -> Vec<Write> {
        self.lock().writes.clone()
    }

    /// The object as the recording now holds it.
    pub fn object<K: Object>(&self, namespace: &str, name: &str) -> Option<K> {
        self.lock()
            .objects
            .get(&key::<K>(namespace, name))
            .cloned()
            .map(decode)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Inner {
    /// Stamp a fresh `resourceVersion` on `value` and store it under `key`.
    fn store(&mut self, key: Key, mut value: serde_json::Value) -> serde_json::Value {
        self.version += 1;
        value["metadata"]["resourceVersion"] = serde_json::Value::String(self.version.to_string());
        self.objects.insert(key, value.clone());
        value
    }

    fn check_failing(&self, key: &Key) -> Result<(), kube::Error> {
        if self.failing.contains(key) {
            return Err(api_error(500, format!("{} is failing writes", key.2)));
        }
        Ok(())
    }
}

fn encode<K: Object>(object: &K) -> serde_json::Value {
    serde_json::to_value(object).expect("a recorded object serializes")
}

fn decode<K: Object>(value: serde_json::Value) -> K {
    serde_json::from_value(value).expect("a recorded object deserializes")
}

fn resource_version(value: &serde_json::Value) -> Option<&str> {
    value
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
}

fn api_error(code: u16, message: String) -> kube::Error {
    kube::Error::Api(Box::new(kube::core::Status {
        code,
        message,
        ..Default::default()
    }))
}

fn not_found(key: &Key) -> kube::Error {
    api_error(404, format!("{} {}/{} not found", key.0, key.1, key.2))
}

fn no_such_kind(kind: &str) -> kube::Error {
    api_error(
        404,
        format!("the server could not find the resource {kind}"),
    )
}

/// The `k=v` terms of a label selector.
fn selector_terms(selector: Option<&str>) -> Vec<(String, String)> {
    selector
        .unwrap_or_default()
        .split(',')
        .filter_map(|term| term.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

fn matches_labels(value: &serde_json::Value, terms: &[(String, String)]) -> bool {
    terms.iter().all(|(k, v)| {
        value["metadata"]["labels"].get(k).and_then(|l| l.as_str()) == Some(v.as_str())
    })
}

impl ApiServer for Recorded {
    fn watch<K: Object>(
        &self,
        namespace: Option<&str>,
        cfg: watcher::Config,
    ) -> BoxStream<'static, Result<watcher::Event<K>, watcher::Error>> {
        let kind = kind::<K>();
        let inner = self.lock();
        if inner.absent.contains(&kind) {
            let failed = async move { Err(watcher::Error::InitialListFailed(no_such_kind(&kind))) };
            return stream::once(failed).chain(stream::pending()).boxed();
        }
        let terms = selector_terms(cfg.label_selector.as_deref());
        let mut events = vec![Ok(watcher::Event::Init)];
        events.extend(
            inner
                .objects
                .iter()
                .filter(|(k, v)| {
                    k.0 == kind && namespace.is_none_or(|ns| k.1 == ns) && matches_labels(v, &terms)
                })
                .map(|(_, v)| Ok(watcher::Event::InitApply(decode::<K>(v.clone())))),
        );
        events.push(Ok(watcher::Event::InitDone));
        stream::iter(events).chain(stream::pending()).boxed()
    }

    async fn list<K: Object>(&self, namespace: Option<&str>) -> Result<Vec<K>, kube::Error> {
        let kind = kind::<K>();
        let inner = self.lock();
        if inner.absent.contains(&kind) {
            return Err(no_such_kind(&kind));
        }
        Ok(inner
            .objects
            .iter()
            .filter(|(k, _)| k.0 == kind && namespace.is_none_or(|ns| k.1 == ns))
            .map(|(_, v)| decode(v.clone()))
            .collect())
    }

    async fn get<K: Object>(&self, namespace: &str, name: &str) -> Result<K, kube::Error> {
        let key = key::<K>(namespace, name);
        self.lock()
            .objects
            .get(&key)
            .cloned()
            .map(decode)
            .ok_or_else(|| not_found(&key))
    }

    async fn create<K: Object>(&self, namespace: &str, object: &K) -> Result<K, kube::Error> {
        let name = object.meta().name.clone().unwrap_or_default();
        let key = key::<K>(namespace, &name);
        let mut inner = self.lock();
        inner.check_failing(&key)?;
        if inner.objects.contains_key(&key) {
            return Err(api_error(409, format!("{} already exists", key.2)));
        }
        let mut value = encode(object);
        value["metadata"]["namespace"] = serde_json::Value::String(namespace.to_string());
        inner.writes.push(Write::Create(key.clone()));
        Ok(decode(inner.store(key, value)))
    }

    async fn replace<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        object: &K,
    ) -> Result<K, kube::Error> {
        let key = key::<K>(namespace, name);
        let mut inner = self.lock();
        inner.check_failing(&key)?;
        let Some(current) = inner.objects.get(&key) else {
            return Err(not_found(&key));
        };
        let value = encode(object);
        if let Some(sent) = resource_version(&value)
            && Some(sent) != resource_version(current)
        {
            return Err(api_error(
                409,
                format!(
                    "{} has been modified; resourceVersion {sent} is stale",
                    key.2
                ),
            ));
        }
        inner.writes.push(Write::Replace(key.clone()));
        Ok(decode(inner.store(key, value)))
    }

    async fn apply<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        manager: &str,
        object: &K,
    ) -> Result<(), kube::Error> {
        let key = key::<K>(namespace, name);
        let mut inner = self.lock();
        inner.check_failing(&key)?;
        let mut value = encode(object);
        value["metadata"]["name"] = serde_json::Value::String(name.to_string());
        value["metadata"]["namespace"] = serde_json::Value::String(namespace.to_string());
        inner.writes.push(Write::Apply {
            key: key.clone(),
            manager: manager.to_string(),
        });
        inner.store(key, value);
        Ok(())
    }

    async fn delete<K: Object>(&self, namespace: &str, name: &str) -> Result<(), kube::Error> {
        let key = key::<K>(namespace, name);
        let mut inner = self.lock();
        inner.check_failing(&key)?;
        if inner.objects.remove(&key).is_none() {
            return Err(not_found(&key));
        }
        inner.writes.push(Write::Delete(key));
        Ok(())
    }

    /// Merge-patches `status`, retaining omitted fields and removing nulls.
    async fn patch_status<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        status: &serde_json::Value,
    ) -> Result<(), kube::Error> {
        let key = key::<K>(namespace, name);
        let mut inner = self.lock();
        inner.check_failing(&key)?;
        let Some(current) = inner.objects.get(&key) else {
            return Err(not_found(&key));
        };
        let mut value = current.clone();
        json_patch::merge(&mut value, &serde_json::json!({"status": status}));
        inner.writes.push(Write::PatchStatus {
            key: key.clone(),
            status: status.clone(),
        });
        inner.store(key, value);
        Ok(())
    }
}
