use super::*;

/// A latched key logs again only after `clear`.
#[test]
fn a_key_logs_again_only_after_it_clears() {
    let latch = WarnOnce::<String>::default();
    assert!(latch.first("a"));
    assert!(!latch.first("a"));
    assert!(latch.first("b"));
    latch.clear("a");
    assert!(latch.first("a"));
}

/// A full latch doesn't log a new key.
#[test]
fn a_full_latch_stops_logging_new_keys() {
    let latch = WarnOnce::<usize>::default();
    for k in 0..MAX_KEYS {
        assert!(latch.first(&k));
    }
    assert!(!latch.first(&MAX_KEYS));
}
