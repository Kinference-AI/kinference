use super::*;

/// The mean damps a spike; the raw gauge does not. One spike in thirty idle
/// samples lands far below one sustained prompt.
#[test]
fn a_queue_spike_is_damped_by_the_window_but_a_sustained_queue_is_not() {
    let reg = MetricsRegistry::new();
    let t0 = Instant::now();
    let tick = |n: u32| t0 + Duration::from_secs(n as u64);

    // 29 idle seconds and one spike to 5.
    let mut spike = 0.0;
    for i in 0..30 {
        let waiting = if i == 14 { 5 } else { 0 };
        spike = reg.record_and_compute_means(1, tick(i), waiting, 0.0).0;
    }
    assert!(
        spike > 0.0 && spike < 0.5,
        "one spike in 30 samples must be well under a single sustained prompt, got {spike}"
    );

    // A different worker, genuinely backed up the whole window.
    let mut sustained = 0.0;
    for i in 0..30 {
        sustained = reg.record_and_compute_means(2, tick(i), 5, 0.0).0;
    }
    assert!(
        (sustained - 5.0).abs() < 0.01,
        "a queue of 5 held for the window reads ~5, got {sustained}"
    );

    // And the separation is what a threshold exploits: `greaterThan: 1` admits
    // the spiky worker and drops the backed-up one.
    assert!(spike < 1.0 && sustained > 1.0, "{spike} vs {sustained}");
}

/// A single sample is a valid mean, with no warm-up zero.
///
/// Unlike the rates, which need two points, so a new saturated worker doesn't
/// read as idle.
#[test]
fn the_first_sample_is_already_a_usable_mean() {
    let reg = MetricsRegistry::new();
    let (q, kv) = reg.record_and_compute_means(1, Instant::now(), 7, 0.93);
    assert_eq!(q, 7.0, "no two-sample floor");
    assert!((kv - 0.93).abs() < 1e-6, "got {kv}");
}

/// Samples older than the window leave, so the mean tracks a recovery.
#[test]
fn the_window_evicts_so_a_recovered_worker_stops_reading_saturated() {
    let reg = MetricsRegistry::new();
    let t0 = Instant::now();
    // Saturated for a full window...
    for i in 0..30 {
        reg.record_and_compute_means(1, t0 + Duration::from_secs(i), 10, 0.99);
    }
    // ...then idle for longer than the window.
    let mut q = f32::NAN;
    let mut kv = f32::NAN;
    for i in 31..=70 {
        let (a, b) = reg.record_and_compute_means(1, t0 + Duration::from_secs(i), 0, 0.0);
        q = a;
        kv = b;
    }
    assert_eq!(q, 0.0, "the saturated samples must have aged out, got {q}");
    assert_eq!(kv, 0.0, "got {kv}");
}

/// `forget` drops the history too.
///
/// So a replacement worker doesn't inherit its predecessor's readings.
#[test]
fn forgetting_a_worker_clears_its_window() {
    let reg = MetricsRegistry::new();
    let t0 = Instant::now();
    for i in 0..30 {
        reg.record_and_compute_means(1, t0 + Duration::from_secs(i), 10, 0.99);
    }
    reg.forget(1);
    let (q, kv) = reg.record_and_compute_means(1, t0 + Duration::from_secs(30), 0, 0.0);
    assert_eq!(q, 0.0, "history survived forget(), got {q}");
    assert_eq!(kv, 0.0, "got {kv}");
}

/// A worker that has never been scraped is unknown, not idle. `latest` still
/// flattens it to zeros for display; `sample` is what decisions read.
#[test]
fn sample_is_none_until_the_first_scrape() {
    let reg = MetricsRegistry::new();
    assert!(reg.sample(7).is_none());
    assert_eq!(reg.latest(7).waiting_mean, 0.0);
    reg.record_sample_for_test(
        7,
        MetricsSample {
            waiting_mean: 2.5,
            scraped_at: Some(Instant::now()),
            ..Default::default()
        },
    );
    assert_eq!(reg.sample(7).map(|s| s.waiting_mean), Some(2.5));
}

/// A non-finite KV cache usage reads as a fraction in `[0, 1]`.
#[tokio::test]
async fn a_non_finite_kv_cache_usage_is_clamped() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (reported, want) in [("+Inf", 1.0), ("NaN", 0.0), ("0.25", 0.25)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/metrics", listener.local_addr().unwrap());
        let body = format!("vllm:kv_cache_usage_perc {reported}\n");
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
        let sample = scrape_once(
            &reqwest::Client::new(),
            &url,
            &ALL_NAMES,
            "",
            "w",
            false,
            &mut false,
        )
        .await
        .unwrap();
        assert_eq!(sample.kv_cache_usage, want, "{reported}");
    }
}
