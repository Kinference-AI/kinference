use super::*;

const ME: &str = "cp-0";
const OTHER: &str = "cp-1";
/// Same as the `leader_election` default, so the boundary arithmetic in
/// these tests still describes the shipped behavior.
const GRACE: Duration = Duration::from_secs(10);

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn ts(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

fn spec(holder: &str, renewed_at: i64) -> LeaseSpec {
    LeaseSpec {
        holder_identity: Some(holder.to_string()),
        lease_duration_seconds: Some(15),
        renew_time: Some(MicroTime(ts(renewed_at))),
        acquire_time: Some(MicroTime(ts(renewed_at))),
        lease_transitions: Some(1),
        ..Default::default()
    }
}

fn lease(spec: LeaseSpec) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some("kinference".into()),
            namespace: Some("infra".into()),
            ..Default::default()
        },
        spec: Some(spec),
    }
}

/// `holder`'s claim, first seen at `since`.
fn seen(holder: &str, since: Instant, ours: bool) -> Seen {
    Seen {
        lease: lease(spec(holder, 100)),
        since,
        ours,
    }
}

#[test]
fn free_lease_is_claimed() {
    let t0 = Instant::now();
    let free = Seen {
        lease: lease(LeaseSpec::default()),
        since: t0,
        ours: false,
    };
    assert_eq!(next_action(t0, &free, ME, GRACE), Action::Claim);
    let empty_holder = Seen {
        lease: lease(LeaseSpec {
            holder_identity: Some(String::new()),
            ..Default::default()
        }),
        since: t0,
        ours: false,
    };
    assert_eq!(next_action(t0, &empty_holder, ME, GRACE), Action::Claim);
}

#[test]
fn malformed_claim_is_claimed() {
    let t0 = Instant::now();
    // Holder set but no duration: unenforceable.
    let s = Seen {
        lease: lease(LeaseSpec {
            holder_identity: Some(OTHER.to_string()),
            ..Default::default()
        }),
        since: t0,
        ours: false,
    };
    assert_eq!(next_action(t0, &s, ME, GRACE), Action::Claim);
}

#[test]
fn rivals_current_claim_waits_out_its_expiry() {
    let t0 = Instant::now();
    let s = seen(OTHER, t0, false);
    assert_eq!(
        next_action(t0, &s, ME, GRACE),
        Action::WaitUntil(t0 + secs(15))
    );
    assert_eq!(next_action(t0 + secs(15), &s, ME, GRACE), Action::Claim);
    assert_eq!(next_action(t0 + secs(100), &s, ME, GRACE), Action::Claim);
}

/// A rival's claim lasts one lease duration from when it was first seen,
/// whatever `renewTime` its holder wrote.
#[test]
fn rivals_claim_is_timed_from_when_it_was_seen() {
    let t0 = Instant::now();
    let now = Timestamp::now().as_second();
    for renewed_at in [0, now - 3600, now + 3600] {
        let s = Seen {
            lease: lease(spec(OTHER, renewed_at)),
            since: t0,
            ours: false,
        };
        assert_eq!(
            next_action(t0, &s, ME, GRACE),
            Action::WaitUntil(t0 + secs(15)),
            "renewTime {renewed_at}"
        );
    }
}

/// Re-reading an unchanged spec keeps its first sighting; a changed spec
/// restarts the clock and is not ours.
#[test]
fn observe_keeps_the_first_sighting_of_an_unchanged_claim() {
    let t0 = Instant::now();
    let mine = seen(ME, t0, true);
    let again = Seen::observe(Some(mine), lease(spec(ME, 100)), t0 + secs(3));
    assert_eq!((again.since, again.ours), (t0, true));

    let renewed = Seen::observe(Some(again), lease(spec(ME, 101)), t0 + secs(4));
    assert_eq!((renewed.since, renewed.ours), (t0 + secs(4), false));
}

#[test]
fn own_fresh_claim_waits_until_renew_point() {
    // Expires at t0+15, grace 10s: renew from t0+5.
    let t0 = Instant::now();
    let s = seen(ME, t0, true);
    assert_eq!(
        next_action(t0, &s, ME, GRACE),
        Action::WaitUntil(t0 + secs(5))
    );
}

#[test]
fn own_claim_inside_grace_is_renewed() {
    let t0 = Instant::now();
    let s = seen(ME, t0, true);
    assert_eq!(next_action(t0 + secs(5), &s, ME, GRACE), Action::Claim);
    assert_eq!(next_action(t0 + secs(14), &s, ME, GRACE), Action::Claim);
    // Even past expiry it's still ours to re-take.
    assert_eq!(next_action(t0 + secs(20), &s, ME, GRACE), Action::Claim);
}

/// A current claim under our identity that this process didn't write is
/// re-taken at once and doesn't count as leading.
#[test]
fn a_claim_left_under_our_identity_is_re_taken() {
    let t0 = Instant::now();
    let s = seen(ME, t0, false);
    assert_eq!(next_action(t0, &s, ME, GRACE), Action::Claim);
    assert!(!s.leads(t0));
}

#[test]
fn leading_respects_expiry() {
    let t0 = Instant::now();
    let s = seen(ME, t0, true);
    assert!(s.leads(t0 + secs(10)));
    assert!(!s.leads(t0 + secs(15)));
    assert!(!seen(OTHER, t0, false).leads(t0));
}

/// The loop over a recorded API server: a free Lease is created, claimed,
/// and vacated when shutdown fires.
#[tokio::test]
async fn a_free_lease_is_claimed_and_vacated_on_shutdown() {
    use crate::api::recorded::{Recorded, Write, key};

    let api = Recorded::default();
    let (tx, mut rx) = watch::channel(false);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let params = LeaseParams {
        name: "kinference".into(),
        lease_duration: Duration::from_secs(15),
        renew_grace: GRACE,
        retry: Duration::from_millis(10),
    };
    let task = tokio::spawn({
        let api = api.clone();
        async move {
            run(api, "infra", ME.to_string(), params, tx, async {
                let _ = stopped.await;
            })
            .await
        }
    });

    tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|leader| *leader))
        .await
        .expect("elected within the timeout")
        .expect("the elector holds the sender");
    stop.send(()).expect("the elector is waiting on shutdown");
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the elector stops on shutdown")
        .expect("the elector does not panic");

    let lease = key::<Lease>("infra", "kinference");
    assert_eq!(
        api.writes(),
        vec![
            Write::Create(lease.clone()),
            Write::Replace(lease.clone()),
            Write::Replace(lease),
        ],
        "created empty, claimed, vacated"
    );
    let spec = api
        .object::<Lease>("infra", "kinference")
        .expect("the lease is still there")
        .spec
        .expect("vacating keeps a spec");
    assert_eq!(spec.holder_identity, None);
    assert_eq!(spec.lease_transitions, Some(1));
    assert!(!*rx.borrow(), "demoted on the way out");
}

/// Spawn an elector for `ME` over `api` that stops when the returned sender
/// fires.
fn spawn_elector<A: ApiServer>(
    api: A,
) -> (
    watch::Receiver<bool>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = watch::channel(false);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let params = LeaseParams {
        name: "kinference".into(),
        lease_duration: secs(15),
        renew_grace: GRACE,
        retry: Duration::from_millis(10),
    };
    let task = tokio::spawn(async move {
        run(api, "infra", ME.to_string(), params, tx, async {
            let _ = stopped.await;
        })
        .await
    });
    (rx, stop, task)
}

/// A rival whose clock runs an hour behind keeps its claim while it renews.
#[tokio::test(start_paused = true)]
async fn a_renewing_rival_with_a_slow_clock_keeps_the_lease() {
    use crate::api::recorded::Recorded;

    let behind = Timestamp::now().as_second() - 3600;
    let api = Recorded::default().with([lease(spec(OTHER, behind))]);
    let rival = tokio::spawn({
        let api = api.clone();
        async move {
            for n in 1.. {
                tokio::time::sleep(secs(5)).await;
                let mut l: Lease = api.get("infra", "kinference").await.unwrap();
                l.spec = Some(spec(OTHER, behind + n));
                api.replace("infra", "kinference", &l).await.unwrap();
            }
        }
    });
    let (mut rx, _stop, _task) = spawn_elector(api);

    let promoted = tokio::time::timeout(secs(60), rx.wait_for(|leader| *leader)).await;
    assert!(promoted.is_err(), "never claims a renewed lease");
    rival.abort();
}

/// A stale claim whose `renewTime` is an hour ahead lapses one lease
/// duration after the elector first sees it.
#[tokio::test(start_paused = true)]
async fn a_dead_rival_with_a_fast_clock_loses_the_lease_after_one_duration() {
    use crate::api::recorded::Recorded;

    let ahead = Timestamp::now().as_second() + 3600;
    let api = Recorded::default().with([lease(spec(OTHER, ahead))]);
    let start = Instant::now();
    let (mut rx, _stop, _task) = spawn_elector(api);

    tokio::time::timeout(secs(20), rx.wait_for(|leader| *leader))
        .await
        .expect("claims after one lease duration")
        .expect("the elector holds the sender");
    let waited = start.elapsed();
    assert!(waited >= secs(15), "claimed after {waited:?}");
}
