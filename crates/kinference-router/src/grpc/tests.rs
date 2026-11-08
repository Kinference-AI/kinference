use super::*;

/// A provider that declares no path sends the empty string, meaning "leave
/// the path alone".
#[test]
fn a_provider_destination_carries_the_path_and_empty_means_unchanged() {
    let pick = |path: Option<&str>| crate::service::ProviderPick {
        provider: kinference_policy::engine::ObjectKey::new("infra", "together"),
        backend_ref: kinference_policy::engine::ObjectKey::new("infra", "together-backend"),
        path: path.map(str::to_string),
        upstream_model: "acme/llama-70b".into(),
        canonical_model: "bar".into(),
    };

    let proto::pick_response::Destination::Provider(t) =
        provider_destination(&pick(Some("/serving-endpoints/chat/completions")))
    else {
        panic!("a provider destination");
    };
    assert_eq!(t.path, "/serving-endpoints/chat/completions");
    // The backend is still named by reference and fully qualified; the path
    // adds to that rather than replacing any part of it.
    let backend = t.backend_ref.expect("backendRef");
    assert_eq!(backend.namespace, "infra");
    assert_eq!(backend.name, "together-backend");

    let proto::pick_response::Destination::Provider(t) = provider_destination(&pick(None)) else {
        panic!("a provider destination");
    };
    assert_eq!(t.path, "", "no declared path sends no path");
}

const MB: usize = 1024 * 1024;

/// A store holding `body` as an outbound body, and the lease that a
/// `PullBody` for it takes.
fn leased(body: &[u8]) -> (Arc<crate::body_store::BodyStore>, BodyLease) {
    let store = crate::body_store::shared(8 * MB as u64, 8 * MB as u64, Duration::from_secs(60));
    let mut r = store.reservation();
    r.charge(body.len() as u64).expect("within budget");
    let stream_id = store.commit(r, body.to_vec());
    let response_id = 7;
    store
        .begin_pick(stream_id)
        .expect("ready body")
        .commit(response_id, Arc::new(body.to_vec()));
    let lease = store.take_outbound(response_id).expect("outbound body");
    (store, lease)
}

/// A caller that reads the whole stream gets exactly the body, and the
/// bytes go back to the budget.
#[tokio::test]
async fn a_pull_streams_the_whole_body_then_releases_it() {
    use futures::StreamExt as _;
    let body: Vec<u8> = (0..MB).map(|i| i as u8).collect();
    let (store, lease) = leased(&body);

    let mut out = Vec::new();
    let mut stream = stream_body(lease, 7, Duration::from_secs(5));
    while let Some(chunk) = stream.next().await {
        out.extend(chunk.expect("no error").chunk);
    }
    assert_eq!(out, body);
    assert_eq!(store.stats().1, 0);
}

/// A caller that keeps the stream open but stops reading loses the stream
/// with `DEADLINE_EXCEEDED`, and the body's bytes go back to the budget.
#[tokio::test]
async fn a_stalled_pull_releases_the_body() {
    use futures::StreamExt as _;
    let body = vec![0u8; MB];
    let (store, lease) = leased(&body);

    let mut stream = stream_body(lease, 7, Duration::from_millis(20));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while store.stats().1 != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "an unread pull still holds {} bytes",
            store.stats().1
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut received = 0;
    let status = loop {
        match stream.next().await.expect("the stream ends with an error") {
            Ok(chunk) => received += chunk.chunk.len(),
            Err(status) => break status,
        }
    };
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert_eq!(received, PULL_CHANNEL_DEPTH * PULL_CHUNK_BYTES);
    assert!(stream.next().await.is_none());
}

async fn push_one_chunk(
    client: &mut crate::proto::picker_client::PickerClient<tonic::transport::Channel>,
    len: usize,
) -> Result<PushBodyResponse, Status> {
    let chunk = BodyChunk {
        chunk: vec![b'x'; len],
    };
    let response = client.push_body(futures::stream::iter([chunk])).await?;
    Ok(response.into_inner())
}

/// Over a real transport, a chunk above the proto's 1 MiB limit fails with
/// `RESOURCE_EXHAUSTED` even when the body fits the per-request budget.
#[tokio::test]
async fn an_oversized_chunk_is_resource_exhausted() {
    use crate::service::test_support::{Render, ServiceBuilder};
    let t = ServiceBuilder::new()
        .render(Render::Absent)
        .body_bytes(8 * MB as u64)
        .build()
        .await;
    let svc = Arc::new(t.svc);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let addr = listener.local_addr().expect("an address");
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .expect("a listener");
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(server(svc.clone()))
            .serve_with_incoming(incoming),
    );
    let mut client = crate::proto::picker_client::PickerClient::connect(format!("http://{addr}"))
        .await
        .expect("a connection");

    let status = push_one_chunk(&mut client, 2 * MB)
        .await
        .expect_err("a 2 MiB chunk is refused");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert_eq!(svc.bodies.store.stats().1, 0);

    let pushed = push_one_chunk(&mut client, MB)
        .await
        .expect("a 1 MiB chunk is accepted");
    assert_eq!(pushed.total_bytes, MB as u64);
}
