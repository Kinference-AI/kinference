use dynamo_kv_router::protocols::{BlockHashOptions, compute_block_hash_for_seq};

/// Helper for the smoke test: given a comma-separated list of u32 tokens and a
/// block size, print the `LocalBlockHash` values that the indexer would
/// compute and store as `tokens_hash` for those tokens.
fn main() {
    let mut args = std::env::args().skip(1);
    let block_size: u32 = args
        .next()
        .expect("usage: compute_hash <block_size> <tok,tok,...>")
        .parse()
        .expect("block_size must be u32");
    let tokens_csv = args.next().expect("missing tokens csv");
    let tokens: Vec<u32> = tokens_csv
        .split(',')
        .map(|s| s.trim().parse::<u32>().expect("bad token"))
        .collect();

    let opts = BlockHashOptions::default();
    let hashes = compute_block_hash_for_seq(&tokens, block_size, opts);
    for h in hashes {
        println!("{}", h.0);
    }
}
