use anyhow::{bail, Context, Result};
use chrono::{Months, TimeZone, Utc};
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use parity_scale_codec::Encode;
use sp_core::crypto::{Ss58AddressFormat, Ss58Codec};
use sp_core::{ed25519, sr25519, H256};
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;
use subxt::{OnlineClient, PolkadotConfig};
use tokio::sync::Mutex;
use std::str::FromStr;


// ====== Subxt metadata (must match endpoint) ======
#[subxt::subxt(runtime_metadata_path = "metadata/kusama-people.scale")]
pub mod people {}

#[subxt::subxt(runtime_metadata_path = "metadata/asset-hub-polkadot.scale")]
pub mod ahp {}

// ---------------- config ----------------
#[derive(Clone, Copy, Debug)]
enum Chain { People, Ahp }

impl Chain {
    fn ws(self) -> &'static str {
        match self {
            Chain::People => "wss://rpc-people-kusama.luckyfriday.io",
            Chain::Ahp    => "wss://rpc-asset-hub-polkadot.luckyfriday.io",
        }
    }
    fn ss58_prefix(self) -> u16 {
        match self {
            Chain::People => 2, // Kusama
            Chain::Ahp    => 0, // Polkadot
        }
    }
    fn name(self) -> &'static str {
        match self {
            Chain::People => "Kusama People",
            Chain::Ahp    => "Asset Hub Polkadot",
        }
    }
}

// knobs
const REWARD_USD: f64 = 300.0;
const CHUNK_SIZE: usize = 10_000;
const CONCURRENCY: usize = 32;
const CHUNK_CONCURRENCY: usize = 20;
const CALL_TIMEOUT_SECS: u64 = 20;

// ---------------- main ----------------
#[tokio::main]
async fn main() -> Result<()> {
    let chain = prompt_chain()?;
    let cfg_ws = chain.ws();
    let ss58_prefix = chain.ss58_prefix();

    let Inputs { month, year, ema, fiat_opt } = prompt_inputs()?;

    // compute window (capped at now)
    let now = Utc::now();
    let start_dt = Utc.with_ymd_and_hms(year, month as u32, 1, 0, 0, 0).unwrap();
    if start_dt >= now {
        bail!("Selected month/year ({}) is in the future vs now ({}).", start_dt, now);
    }
    let mut end_dt = start_dt + Months::new(1);
    if end_dt > now { end_dt = now; }
    if end_dt <= start_dt { bail!("Empty window: start {} >= end {}.", start_dt, end_dt); }
    let start_ms = start_dt.timestamp_millis() as u64;
    let end_ms = end_dt.timestamp_millis() as u64;

    println!(
        "==> Chain: {}  |  RPC: {}\n==> Window: [{} .. {})  |  EMA: {}{}",
        chain.name(),
        cfg_ws,
        start_dt.to_rfc3339(),
        end_dt.to_rfc3339(),
        ema,
        fiat_opt.map(|f| format!("  |  Example: ${:.2} ⇒ {:.8} units", f, f / ema)).unwrap_or_default()
    );

    // connections
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(cfg_ws)
        .await
        .with_context(|| format!("connect subxt to {}", cfg_ws))?;
    let rpc = Arc::new(
        WsClientBuilder::default()
            .build(cfg_ws)
            .await
            .with_context(|| format!("connect rpc ws to {}", cfg_ws))?,
    );

    // latest
    let latest = api.blocks().at_latest().await?;
    let latest_num = latest.number();
    let latest_ts = block_timestamp(&api, latest.hash(), chain).await?.unwrap_or(0);
    println!("==> Latest: #{} ts={}", latest_num, fmt_ts(latest_ts));
    if latest_ts < start_ms {
        bail!("Latest {} is before window start {}.", fmt_ts(latest_ts), fmt_ts(start_ms));
    }

    // bounds via binary search
    println!("==> Locating first block ≥ {} (binary search)…", fmt_ts(start_ms));
    let first_num = bin_search_first_ge(&api, &rpc, 0, latest_num, start_ms, chain).await?;
    println!("   -> first in window: #{}", first_num);

    println!("==> Locating last block < {} (binary search)…", fmt_ts(end_ms));
    let ub = bin_search_first_ge(&api, &rpc, first_num, latest_num, end_ms, chain).await?;
    let last_num = ub.saturating_sub(1);
    println!("   -> last in window:  #{}", last_num);
    if last_num < first_num { bail!("Empty window: last({last_num}) < first({first_num})."); }

    let total_blocks = (last_num - first_num + 1) as usize;
    println!(
        "==> Full scan: total blocks = {total_blocks}, chunk size = {CHUNK_SIZE}, outer chunk concurrency = {CHUNK_CONCURRENCY}, inner per-chunk concurrency = {CONCURRENCY}"
    );

    // progress bars
    let mp = MultiProgress::new();
    mp.set_draw_target(ProgressDrawTarget::stderr_with_hz(2));
    let overall_pb = mp.add(ProgressBar::new(total_blocks as u64));
    overall_pb.set_style(
        ProgressStyle::with_template("[overall] {bar:50.cyan/blue} {pos}/{len} ({percent}%) ETA {eta}")
            .unwrap()
            .progress_chars("##-"),
    );

    // build chunks
    let mut chunk_specs: Vec<(u32, u32, ProgressBar)> = Vec::new();
    let mut s = first_num;
    while s <= last_num {
        let e = std::cmp::min(s.saturating_add((CHUNK_SIZE as u32) - 1), last_num);
        let len = (e - s + 1) as u64;
        let pb = mp.add(ProgressBar::new(len));
        pb.set_style(
            ProgressStyle::with_template("[{pos}/{len}] {bar:40.green/black} ETA {eta}")
                .unwrap()
                .progress_chars("=>-"),
        );
        chunk_specs.push((s, e, pb));
        s = e.saturating_add(1);
    }

    // stats keyed by OWNER AccountId32 raw
    let stats: Arc<Mutex<HashMap<[u8; 32], usize>>> = Arc::new(Mutex::new(HashMap::new()));
    // keep last-seen session key per owner (to display)
    let owner_to_session: Arc<Mutex<HashMap<[u8;32], [u8;32]>>> = Arc::new(Mutex::new(HashMap::new()));
    let unknowns = Arc::new(Mutex::new(0usize));
    let block_errors = Arc::new(Mutex::new(0usize));

    // run
    futures::stream::iter(
        chunk_specs
            .into_iter()
            .map(|(c_start, c_end, chunk_pb)| {
                let api = api.clone();
                let rpc = rpc.clone();
                let stats = stats.clone();
                let overall_pb = overall_pb.clone();
                let pb_for_tasks = chunk_pb.clone();
                let unknowns = unknowns.clone();
                let block_errors = block_errors.clone();
                let owner_to_session = owner_to_session.clone();

                async move {
                    let numbers: Vec<u32> = (c_start..=c_end).collect();

                    futures::stream::iter(numbers.into_iter().map(move |n| {
                        let api = api.clone();
                        let rpc = rpc.clone();
                        let stats = stats.clone();
                        let chunk_pb = pb_for_tasks.clone();
                        let overall_pb = overall_pb.clone();
                        let unknowns = unknowns.clone();
                        let block_errors = block_errors.clone();
                        let owner_to_session = owner_to_session.clone();

                        async move {
                            let res: Result<()> = async {
                                let h = block_hash_by_number(&rpc, n).await?;

                                // 1) derive aura session key (slot % authorities)
                                let session_key_opt: Option<[u8; 32]> = derive_session_key(&api, h, chain).await?;

                                // 2) resolve owner via session.key_owner((KeyTypeId("aura"), key_bytes))
                                if let Some(sess_key) = session_key_opt {
                                    if let Some(owner_raw) = session_key_owner_account(&api, h, chain, sess_key).await? {
                                        // tally
                                        {
                                            let mut sm = stats.lock().await;
                                            *sm.entry(owner_raw).or_insert(0) += 1;
                                        }
                                        {
                                            let mut map = owner_to_session.lock().await;
                                            map.entry(owner_raw).or_insert(sess_key);
                                        }
                                    } else {
                                        // count as unknown for visibility
                                        {
                                            let mut u = unknowns.lock().await;
                                            *u += 1;
                                        }
                                        {
                                            let mut sm = stats.lock().await;
                                            *sm.entry([0u8;32]).or_insert(0) += 1;
                                        }
                                    }
                                } else {
                                    {
                                        let mut u = unknowns.lock().await;
                                        *u += 1;
                                    }
                                    {
                                        let mut sm = stats.lock().await;
                                        *sm.entry([0u8;32]).or_insert(0) += 1;
                                    }
                                }

                                // progress
                                chunk_pb.inc(1);
                                overall_pb.inc(1);
                                Ok(())
                            }.await;

                            if let Err(e) = res {
                                {
                                    let mut be = block_errors.lock().await;
                                    *be += 1;
                                }
                                eprintln!("  [block #{n} error] {e:#}");
                            }
                            Ok::<(), anyhow::Error>(())
                        }
                    }))
                        .buffer_unordered(CONCURRENCY)
                        .for_each(|res| async {
                            if let Err(e) = res {
                                eprintln!("  [block task join error] {e:#}");
                            }
                        })
                        .await;

                    chunk_pb.finish_with_message("done");
                    Ok::<(), anyhow::Error>(())
                }
            }),
    )
        .buffer_unordered(CHUNK_CONCURRENCY)
        .for_each(|res| async {
            if let Err(e) = res {
                eprintln!("[chunk error] {e:#}");
            }
        })
        .await;

    overall_pb.finish_with_message("full scan complete");
    mp.clear()?;

    // summary
    let stats = Arc::try_unwrap(stats).unwrap().into_inner();
    let session_map = Arc::try_unwrap(owner_to_session).unwrap().into_inner();
    let total_scanned: usize = stats.values().copied().sum();

    // rows: (owner_ss58, count, %total, session_key_hex, owner_hex)
    let mut rows: Vec<(String, usize, f64, String, String)> = stats
        .into_iter()
        .map(|(owner_raw, cnt)| {
            if owner_raw == [0u8; 32] {
                let pct_total = if total_scanned > 0 {
                    (cnt as f64) * 100.0 / (total_scanned as f64)
                } else { 0.0 };
                (
                    "UNKNOWN".to_string(),
                    cnt,
                    pct_total,
                    "0x".to_string() + &hex::encode([0u8;32]),
                    "0x".to_string() + &hex::encode([0u8;32]),
                )
            } else {
                let ss58 = ss58_from_raw32_with_prefix(owner_raw, ss58_prefix, chain);
                let owner_hex = format!("0x{}", hex::encode(owner_raw));
                let sess_hex = session_map
                    .get(&owner_raw)
                    .map(|s| format!("0x{}", hex::encode(s)))
                    .unwrap_or_else(|| "-".to_string());

                let pct_total = if total_scanned > 0 { (cnt as f64) * 100.0 / (total_scanned as f64) } else { 0.0 };
                (ss58, cnt, pct_total, sess_hex, owner_hex)
            }
        })
        .collect();

    rows.sort_by(|a, b| {
        if a.0 == "UNKNOWN" && b.0 != "UNKNOWN" {
            std::cmp::Ordering::Greater
        } else if b.0 == "UNKNOWN" && a.0 != "UNKNOWN" {
            std::cmp::Ordering::Less
        } else {
            b.1.cmp(&a.1)
        }
    });

    let max_count = rows.iter().map(|(_, c, _, _, _)| *c).max().unwrap_or(0);

    println!("\n================ SUMMARY (full scan) ================");
    println!("Chain:      {}", chain.name());
    println!("Chain RPC:  {}", cfg_ws);
    println!("Window:     [{} .. {})", start_dt.to_rfc3339(), end_dt.to_rfc3339());
    println!("Blocks scanned: {}", total_scanned);
    println!(
        "{:<6}  {:<58}  {:>8}  {:>7}  {:>7}  {:>9}  {:>11}2\
        ",
        "Rank", "Author (Owner SS58)", "Blocks", "%", "%Top", "Payout", "Payout/EMA"
    );
    println!("{}", "-".repeat(220));

    for (i, (author, cnt, pct_total, sess_hex, acc_hex)) in rows.iter().enumerate() {
        let pct_top = if max_count > 0 { (*cnt as f64) * 100.0 / (max_count as f64) } else { 0.0 };
        let payout = REWARD_USD * (pct_top / 100.0);
        let payout_per_ema = payout / ema;

        println!(
            "{:<6}  {:<58}  {:>8}  {:>7.2}  {:>7.2}  {:>9.2}  {:>11.6}",
            i + 1, author, cnt, pct_total, pct_top, payout, payout_per_ema
        );
    }
    println!("{}", "-".repeat(220));
    println!("Note: '%' is share of all blocks in window; '%Top' is relative to the top producer.");
    println!("Assumed reward pool for '%Top' payout: ${:.2}", REWARD_USD);
    println!("EMA used: {}", ema);

    // diagnostics
    let unknowns = Arc::try_unwrap(unknowns).unwrap().into_inner();
    let block_errors = Arc::try_unwrap(block_errors).unwrap().into_inner();
    eprintln!("Diagnostics: unknown-authors={}, block-errors={}", unknowns, block_errors);

    println!("==> Done.");
    Ok(())
}

// ---------------- interactive ----------------
struct Inputs { month: u8, year: i32, ema: f64, fiat_opt: Option<f64> }

fn prompt_chain() -> Result<Chain> {
    loop {
        println!("Select chain:");
        println!("  1) Kusama People");
        println!("  2) Asset Hub Polkadot");
        print!("Enter selection (1 or 2): ");
        io::stdout().flush().ok();
        let mut s = String::new();
        io::stdin().read_line(&mut s)?;
        match s.trim() {
            "1" => return Ok(Chain::People),
            "2" => return Ok(Chain::Ahp),
            _ => { eprintln!("  -> Please enter 1 or 2."); }
        }
    }
}

fn prompt_inputs() -> Result<Inputs> {
    let month = loop {
        print!("Enter month (1-12): ");
        io::stdout().flush().ok();
        let mut s = String::new();
        io::stdin().read_line(&mut s)?;
        match s.trim().parse::<u8>() {
            Ok(m) if (1..=12).contains(&m) => break m,
            _ => { eprintln!("  -> Please enter an integer 1..12."); continue; }
        }
    };
    let year = loop {
        print!("Enter year (>= 2024): ");
        io::stdout().flush().ok();
        let mut s = String::new();
        io::stdin().read_line(&mut s)?;
        match s.trim().parse::<i32>() {
            Ok(y) if y >= 2024 => break y,
            _ => { eprintln!("  -> Please enter a valid year >= 2024."); continue; }
        }
    };
    let ema = loop {
        print!("Enter EMA (> 0): ");
        io::stdout().flush().ok();
        let mut s = String::new();
        io::stdin().read_line(&mut s)?;
        match s.trim().parse::<f64>() {
            Ok(v) if v > 0.0 => break v,
            _ => { eprintln!("  -> Please enter a positive number."); continue; }
        }
    };
    let fiat_opt = loop {
        print!("Enter fiat amount for example conversion (optional, press Enter to skip): ");
        io::stdout().flush().ok();
        let mut s = String::new();
        io::stdin().read_line(&mut s)?;
        let t = s.trim();
        if t.is_empty() { break None; }
        match t.parse::<f64>() {
            Ok(v) if v >= 0.0 => break Some(v),
            _ => { eprintln!("  -> Enter a non-negative number or just press Enter to skip."); continue; }
        }
    };
    Ok(Inputs { month, year, ema, fiat_opt })
}

// ---------------- helpers ----------------

async fn block_hash_by_number(rpc: &Arc<jsonrpsee::ws_client::WsClient>, number: u32) -> Result<H256> {
    // Using jsonrpsee for speed
    let hex: String = tokio::time::timeout(
        Duration::from_secs(CALL_TIMEOUT_SECS),
        rpc.request("chain_getBlockHash", rpc_params![number]),
    )
        .await
        .map_err(|_| anyhow::anyhow!("timeout chain_getBlockHash({number})"))??;

    // H256 hex from node is already 0x-prefixed; sp_core::H256 FromStr handles it
    let h = H256::from_str(hex.trim()).map_err(|e| anyhow::anyhow!("bad hash from rpc for #{number}: {e}"))?;
    Ok(h)
}

async fn block_timestamp(api: &OnlineClient<PolkadotConfig>, at: H256, chain: Chain) -> Result<Option<u64>> {
    match chain {
        Chain::People => {
            let ts: Option<u64> = api.storage().at(at).fetch(&people::storage().timestamp().now()).await?;
            Ok(ts)
        }
        Chain::Ahp => {
            let ts: Option<u64> = api.storage().at(at).fetch(&ahp::storage().timestamp().now()).await?;
            Ok(ts)
        }
    }
}

async fn bin_search_first_ge(
    api: &OnlineClient<PolkadotConfig>,
    rpc: &Arc<jsonrpsee::ws_client::WsClient>,
    mut lo: u32,
    mut hi: u32,
    target_ms: u64,
    chain: Chain,
) -> Result<u32> {
    let lo_ts = block_timestamp(api, block_hash_by_number(rpc, lo).await?, chain).await?.unwrap_or(0);
    let hi_ts = block_timestamp(api, block_hash_by_number(rpc, hi).await?, chain).await?.unwrap_or(0);

    if hi_ts < target_ms {
        bail!("bin_search_first_ge: hi(#{} ts={}) < target {}", hi, fmt_ts(hi_ts), fmt_ts(target_ms));
    }
    if lo_ts >= target_ms { return Ok(lo); }

    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_h = block_hash_by_number(rpc, mid).await?;
        let mid_ts = block_timestamp(api, mid_h, chain).await?.unwrap_or(0);
        if mid_ts >= target_ms { hi = mid; } else { lo = mid; }
    }
    Ok(hi)
}

fn fmt_ts(ts_ms: u64) -> String {
    let i = ts_ms as i64;
    match chrono::Utc.timestamp_millis_opt(i).single() {
        Some(dt) => dt.to_rfc3339(),
        None => format!("{} (invalid)", ts_ms),
    }
}

fn ss58_from_raw32_with_prefix(raw: [u8; 32], prefix: u16, chain: Chain) -> String {
    let fmt = Ss58AddressFormat::custom(prefix);
    // AccountId SS58 doesn’t depend on ed/sr; either type encodes the same 32B.
    match chain {
        Chain::Ahp => ed25519::Public::from_raw(raw).to_ss58check_with_version(fmt),
        Chain::People => sr25519::Public::from_raw(raw).to_ss58check_with_version(fmt),
    }
}

// ---- author resolution pieces ----

async fn derive_session_key(
    api: &OnlineClient<PolkadotConfig>,
    at: H256,
    chain: Chain,
) -> Result<Option<[u8; 32]>> {
    match chain {
        // People uses Aura sr25519
        Chain::People => {
            let slot: Option<people::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&people::storage().aura().current_slot()).await?;
            let auths: Option<
                people::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    people::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&people::storage().aura().authorities()).await?;

            let slot_num_opt = slot.as_ref().map(|s| s.0);
            let alen = auths.as_ref().map(|b| b.0.len()).unwrap_or(0);

            if let (Some(slot_num), Some(bv)) = (slot_num_opt, auths.as_ref()) {
                if alen == 0 { return Ok(None); }
                let idx = (slot_num as usize) % alen;
                Ok(Some(bv.0[idx].0))
            } else {
                Ok(None)
            }
        }
        // AHP uses Aura ed25519
        Chain::Ahp => {
            let slot: Option<ahp::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&ahp::storage().aura().current_slot()).await?;
            let auths: Option<
                ahp::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    ahp::runtime_types::sp_consensus_aura::ed25519::app_ed25519::Public
                >
            > = api.storage().at(at).fetch(&ahp::storage().aura().authorities()).await?;

            let slot_num_opt = slot.as_ref().map(|s| s.0);
            let alen = auths.as_ref().map(|b| b.0.len()).unwrap_or(0);

            if let (Some(slot_num), Some(bv)) = (slot_num_opt, auths.as_ref()) {
                if alen == 0 { return Ok(None); }
                let idx = (slot_num as usize) % alen;
                Ok(Some(bv.0[idx].0))
            } else {
                Ok(None)
            }
        }
    }
}

async fn session_key_owner_account(
    api: &OnlineClient<PolkadotConfig>,
    at: H256,
    chain: Chain,
    session_key_raw32: [u8; 32],
) -> Result<Option<[u8; 32]>> {
    // Build tuple (KeyTypeId("aura"), Bytes(session_key))
    match chain {
        Chain::People => {
            // NOTE: Depending on metadata version, KeyTypeId path may differ slightly.
            let kt = people::runtime_types::sp_core::crypto::KeyTypeId(*b"aura");
            let call = people::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            let owner_opt = api.storage().at(at).fetch(&call).await?;
            Ok(owner_opt.map(account_to_raw32))
        }
        Chain::Ahp => {
            let kt = ahp::runtime_types::sp_core::crypto::KeyTypeId(*b"aura");
            let call = ahp::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            let owner_opt = api.storage().at(at).fetch(&call).await?;
            Ok(owner_opt.map(account_to_raw32))
        }
    }
}

/// Convert a runtime AccountId32 (opaque newtype) into [u8;32] by SCALE-encoding.
fn account_to_raw32<T: Encode>(acc: T) -> [u8; 32] {
    let bytes = acc.encode();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes[..32]);
    out
}
