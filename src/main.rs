use anyhow::{anyhow, bail, Context, Result};
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
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use subxt::{OnlineClient, PolkadotConfig};
use tokio::sync::Mutex;

// ===== Subxt metadata (must match endpoints) =====
#[subxt::subxt(runtime_metadata_path = "metadata/asset-hub-polkadot.scale")]
pub mod ahp {}

#[subxt::subxt(runtime_metadata_path = "metadata/bridge-hub-polkadot.scale")]
pub mod bhp {}

#[subxt::subxt(runtime_metadata_path = "metadata/coretime-polkadot.scale")]
pub mod ctp {}

#[subxt::subxt(runtime_metadata_path = "metadata/collectives-polkadot.scale")]
pub mod clp {}

#[subxt::subxt(runtime_metadata_path = "metadata/people-polkadot.scale")]
pub mod ppldot {}

#[subxt::subxt(runtime_metadata_path = "metadata/asset-hub-kusama.scale")]
pub mod ahk {}

#[subxt::subxt(runtime_metadata_path = "metadata/bridge-hub-kusama.scale")]
pub mod bhk {}

#[subxt::subxt(runtime_metadata_path = "metadata/coretime-kusama.scale")]
pub mod ctk {}

#[subxt::subxt(runtime_metadata_path = "metadata/people-kusama.scale")]
pub mod pplksm {}

#[subxt::subxt(runtime_metadata_path = "metadata/encointer-kusama.scale")]
pub mod enk {}

// ---------------- config ----------------
#[derive(Clone, Copy, Debug)]
enum Chain {
    // Polkadot system chains
    Ahp, Bhp, Ctp, Clp, PplDot,
    // Kusama system chains
    Ahk, Bhk, Ctk, PplKsm, Enk,
}

#[derive(Clone)]
struct ChainCfg {
    name: &'static str,
    ws:   &'static str,
    // Which palette flavor for Aura (& which identity hub to use)
    family: Family,
    ss58_prefix: u16,
    aura_kind: AuraKind,
}

#[derive(Clone, Copy, Debug)]
enum Family { Polkadot, Kusama }

#[derive(Clone, Copy, Debug)]
enum AuraKind { Ed25519, Sr25519 }

const CHAINS: &[(Chain, ChainCfg)] = &[
    // Polkadot family
    (Chain::Ahp,    ChainCfg { name: "Polkadot Asset Hub",   ws: "wss://rpc-asset-hub-polkadot.luckyfriday.io",  family: Family::Polkadot, ss58_prefix: 0, aura_kind: AuraKind::Ed25519 }),
    (Chain::Bhp,    ChainCfg { name: "Polkadot Bridge Hub",  ws: "wss://rpc-bridge-hub-polkadot.luckyfriday.io", family: Family::Polkadot, ss58_prefix: 0, aura_kind: AuraKind::Ed25519 }),
    (Chain::Ctp,    ChainCfg { name: "Polkadot Coretime",    ws: "wss://rpc-coretime-polkadot.luckyfriday.io",   family: Family::Polkadot, ss58_prefix: 0, aura_kind: AuraKind::Ed25519 }),
    (Chain::Clp,    ChainCfg { name: "Polkadot Collectives", ws: "wss://rpc-collectives-polkadot.luckyfriday.io",family: Family::Polkadot, ss58_prefix: 0, aura_kind: AuraKind::Ed25519 }),
    (Chain::PplDot, ChainCfg { name: "Polkadot People",      ws: "wss://rpc-people-polkadot.luckyfriday.io",     family: Family::Polkadot, ss58_prefix: 0, aura_kind: AuraKind::Sr25519 }),
    // Kusama family
    (Chain::Ahk,    ChainCfg { name: "Kusama Asset Hub",     ws: "wss://rpc-asset-hub-kusama.luckyfriday.io",    family: Family::Kusama,   ss58_prefix: 2, aura_kind: AuraKind::Sr25519 }),
    (Chain::Bhk,    ChainCfg { name: "Kusama Bridge Hub",    ws: "wss://rpc-bridge-hub-kusama.luckyfriday.io",   family: Family::Kusama,   ss58_prefix: 2, aura_kind: AuraKind::Sr25519 }),
    (Chain::Ctk,    ChainCfg { name: "Kusama Coretime",      ws: "wss://rpc-coretime-kusama.luckyfriday.io",     family: Family::Kusama,   ss58_prefix: 2, aura_kind: AuraKind::Sr25519 }),
    (Chain::PplKsm, ChainCfg { name: "Kusama People",        ws: "wss://rpc-people-kusama.luckyfriday.io",       family: Family::Kusama,   ss58_prefix: 2, aura_kind: AuraKind::Sr25519 }),
    (Chain::Enk,    ChainCfg { name: "Kusama Encointer",     ws: "wss://rpc-encointer-kusama.luckyfriday.io",    family: Family::Kusama,   ss58_prefix: 2, aura_kind: AuraKind::Sr25519 }),
];

const REWARD_USD: f64 = 300.0;
const CHUNK_SIZE: usize = 10_000;
const CONCURRENCY: usize = 32;
const CHUNK_CONCURRENCY: usize = 20;
const CALL_TIMEOUT_SECS: u64 = 20;

// ---------------- main ----------------
#[tokio::main]
async fn main() -> Result<()> {
    println!("Select chain:");
    for (i, (c, cfg)) in CHAINS.iter().enumerate() {
        println!(" {:>2}) {}", i + 1, cfg.name);
    }
    let chain = loop {
        print!("Enter selection (1-{}): ", CHAINS.len());
        io::stdout().flush().ok();
        let mut s = String::new();
        io::stdin().read_line(&mut s)?;
        if let Ok(idx) = s.trim().parse::<usize>() {
            if (1..=CHAINS.len()).contains(&idx) {
                break CHAINS[idx - 1].clone();
            }
        }
        eprintln!("  -> Invalid selection.");
    };
    let cfg = chain.1;

    let Inputs { month, year, ema, fiat_opt } = prompt_inputs()?;

    // compute window
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
        cfg.name,
        cfg.ws,
        start_dt.to_rfc3339(),
        end_dt.to_rfc3339(),
        ema,
        fiat_opt.map(|f| format!("  |  Example: ${:.2} ⇒ {:.8} units", f, f / ema)).unwrap_or_default()
    );

    // connections
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(cfg.ws)
        .await
        .with_context(|| format!("connect subxt to {}", cfg.ws))?;
    let rpc = Arc::new(
        WsClientBuilder::default()
            .build(cfg.ws)
            .await
            .with_context(|| format!("connect rpc ws to {}", cfg.ws))?,
    );

    // latest
    let latest = api.blocks().at_latest().await?;
    let latest_num = latest.number();
    let latest_ts = block_timestamp(&api, latest.hash(), &cfg).await?.unwrap_or(0);
    println!("==> Latest: #{} ts={}", latest_num, fmt_ts(latest_ts));
    if latest_ts < start_ms {
        bail!("Latest {} is before window start {}.", fmt_ts(latest_ts), fmt_ts(start_ms));
    }

    // bounds via binary search
    println!("==> Locating first block ≥ {} (binary search)…", fmt_ts(start_ms));
    let first_num = bin_search_first_ge(&api, &rpc, 0, latest_num, start_ms, &cfg).await?;
    println!("   -> first in window: #{}", first_num);

    println!("==> Locating last block < {} (binary search)…", fmt_ts(end_ms));
    let ub = bin_search_first_ge(&api, &rpc, first_num, latest_num, end_ms, &cfg).await?;
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
                let cfg = cfg.clone();

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
                        let cfg = cfg.clone();

                        async move {
                            let res: Result<()> = async {
                                let h = block_hash_by_number(&rpc, n).await?;

                                // 1) derive aura session key (slot % authorities)
                                let session_key_opt: Option<[u8; 32]> = derive_session_key(&api, h, &cfg).await?;

                                // 2) resolve owner via session.key_owner((KeyTypeId("aura"), key_bytes))
                                if let Some(sess_key) = session_key_opt {
                                    if let Some(owner_raw) = session_key_owner_account(&api, h, &cfg, sess_key).await? {
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

    // ===== Identity resolution (People chain) =====
    // Connect to the correct People chain ONLY once, then label each account.
    let people_ws = match cfg.family {
        Family::Polkadot => "wss://rpc-people-polkadot.luckyfriday.io",
        Family::Kusama   => "wss://rpc-people-kusama.luckyfriday.io",
    };
    let id_api = OnlineClient::<PolkadotConfig>::from_insecure_url(people_ws)
        .await
        .with_context(|| format!("connect subxt (identity) to {}", people_ws))?;

    // summary
    let stats = Arc::try_unwrap(stats).unwrap().into_inner();
    let session_map = Arc::try_unwrap(owner_to_session).unwrap().into_inner();
    let total_scanned: usize = stats.values().copied().sum();

    // rows: (label, count, %total, session_key_hex, owner_hex)
    let mut rows: Vec<(String, usize, f64, String, String)> = Vec::new();
    for (owner_raw, cnt) in stats.into_iter() {
        let pct_total = if total_scanned > 0 {
            (cnt as f64) * 100.0 / (total_scanned as f64)
        } else { 0.0 };

        if owner_raw == [0u8; 32] {
            rows.push((
                "UNKNOWN".to_string(),
                cnt,
                pct_total,
                format!("0x{}", hex::encode([0u8;32])),
                format!("0x{}", hex::encode([0u8;32])),
            ));
            continue;
        }

        let owner_hex = format!("0x{}", hex::encode(owner_raw));
        let sess_hex = session_map
            .get(&owner_raw)
            .map(|s| format!("0x{}", hex::encode(s)))
            .unwrap_or_else(|| "-".to_string());

        // Resolve identity label (sub or primary) from People
        let label = resolve_identity_label(&id_api, &cfg, owner_raw, cfg.ss58_prefix).await
            .unwrap_or_else(|_| ss58_from_raw32_with_prefix(owner_raw, cfg.ss58_prefix, cfg.aura_kind));

        rows.push((label, cnt, pct_total, sess_hex, owner_hex));
    }

    // Sort: UNKNOWN last, else by count desc
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
    println!("Chain:      {}", cfg.name);
    println!("Chain RPC:  {}", cfg.ws);
    println!("Window:     [{} .. {})", start_dt.to_rfc3339(), end_dt.to_rfc3339());
    println!("Blocks scanned: {}", total_scanned);
    println!(
        "{:<6}  {:<58}  {:>8}  {:>7}  {:>7}  {:>9}  {:>11}",
        "Rank", "Author (Identity / SS58)", "Blocks", "%", "%Top", "Payout", "Payout/EMA"
    );
    println!("{}", "-".repeat(120));

    for (i, (label, cnt, pct_total, _sess_hex, _acc_hex)) in rows.iter().enumerate() {
        let pct_top = if max_count > 0 { (*cnt as f64) * 100.0 / (max_count as f64) } else { 0.0 };
        let payout = REWARD_USD * (pct_top / 100.0);
        let payout_per_ema = payout / ema;

        println!(
            "{:<6}  {:<58}  {:>8}  {:>7.2}  {:>7.2}  {:>9.2}  {:>11.6}",
            i + 1, label, cnt, pct_total, pct_top, payout, payout_per_ema
        );
    }
    println!("{}", "-".repeat(120));
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
    let hex: String = tokio::time::timeout(
        Duration::from_secs(CALL_TIMEOUT_SECS),
        rpc.request("chain_getBlockHash", rpc_params![number]),
    )
        .await
        .map_err(|_| anyhow!("timeout chain_getBlockHash({number})"))??;

    let h = H256::from_str(hex.trim()).map_err(|e| anyhow!("bad hash from rpc for #{number}: {e}"))?;
    Ok(h)
}

async fn block_timestamp(api: &OnlineClient<PolkadotConfig>, at: H256, cfg: &ChainCfg) -> Result<Option<u64>> {
    let ts: Option<u64> = match cfg.name {
        // Polkadot
        "Polkadot Asset Hub"   => api.storage().at(at).fetch(&ahp::storage().timestamp().now()).await?,
        "Polkadot Bridge Hub"  => api.storage().at(at).fetch(&bhp::storage().timestamp().now()).await?,
        "Polkadot Coretime"    => api.storage().at(at).fetch(&ctp::storage().timestamp().now()).await?,
        "Polkadot Collectives" => api.storage().at(at).fetch(&clp::storage().timestamp().now()).await?,
        "Polkadot People"      => api.storage().at(at).fetch(&ppldot::storage().timestamp().now()).await?,
        // Kusama
        "Kusama Asset Hub"     => api.storage().at(at).fetch(&ahk::storage().timestamp().now()).await?,
        "Kusama Bridge Hub"    => api.storage().at(at).fetch(&bhk::storage().timestamp().now()).await?,
        "Kusama Coretime"      => api.storage().at(at).fetch(&ctk::storage().timestamp().now()).await?,
        "Kusama People"        => api.storage().at(at).fetch(&pplksm::storage().timestamp().now()).await?,
        "Kusama Encointer"     => api.storage().at(at).fetch(&enk::storage().timestamp().now()).await?,
        _ => None,
    };
    Ok(ts)
}

async fn bin_search_first_ge(
    api: &OnlineClient<PolkadotConfig>,
    rpc: &Arc<jsonrpsee::ws_client::WsClient>,
    mut lo: u32,
    mut hi: u32,
    target_ms: u64,
    cfg: &ChainCfg,
) -> Result<u32> {
    let lo_ts = block_timestamp(api, block_hash_by_number(rpc, lo).await?, cfg).await?.unwrap_or(0);
    let hi_ts = block_timestamp(api, block_hash_by_number(rpc, hi).await?, cfg).await?.unwrap_or(0);

    if hi_ts < target_ms {
        bail!("bin_search_first_ge: hi(#{} ts={}) < target {}", hi, fmt_ts(hi_ts), fmt_ts(target_ms));
    }
    if lo_ts >= target_ms { return Ok(lo); }

    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_h = block_hash_by_number(rpc, mid).await?;
        let mid_ts = block_timestamp(api, mid_h, cfg).await?.unwrap_or(0);
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

fn ss58_from_raw32_with_prefix(raw: [u8; 32], prefix: u16, aura: AuraKind) -> String {
    let fmt = Ss58AddressFormat::custom(prefix);
    match aura {
        AuraKind::Ed25519 => ed25519::Public::from_raw(raw).to_ss58check_with_version(fmt),
        AuraKind::Sr25519 => sr25519::Public::from_raw(raw).to_ss58check_with_version(fmt),
    }
}

// ---- author resolution pieces ----

async fn derive_session_key(
    api: &OnlineClient<PolkadotConfig>,
    at: H256,
    cfg: &ChainCfg,
) -> Result<Option<[u8; 32]>> {
    // Each chain has its own module, pull Aura::(current_slot, authorities) typed
    match cfg.name {
        // Polkadot family (Aura is ed25519 except Polkadot People where its sr25519)
        "Polkadot Asset Hub" => {
            let slot: Option<ahp::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&ahp::storage().aura().current_slot()).await?;
            let auths: Option<
                ahp::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    ahp::runtime_types::sp_consensus_aura::ed25519::app_ed25519::Public
                >
            > = api.storage().at(at).fetch(&ahp::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        "Polkadot Bridge Hub" => {
            let slot: Option<bhp::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&bhp::storage().aura().current_slot()).await?;
            let auths: Option<
                bhp::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    bhp::runtime_types::sp_consensus_aura::ed25519::app_ed25519::Public
                >
            > = api.storage().at(at).fetch(&bhp::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        "Polkadot Coretime" => {
            let slot: Option<ctp::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&ctp::storage().aura().current_slot()).await?;
            let auths: Option<
                ctp::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    ctp::runtime_types::sp_consensus_aura::ed25519::app_ed25519::Public
                >
            > = api.storage().at(at).fetch(&ctp::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        "Polkadot Collectives" => {
            let slot: Option<clp::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&clp::storage().aura().current_slot()).await?;
            let auths: Option<
                clp::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    clp::runtime_types::sp_consensus_aura::ed25519::app_ed25519::Public
                >
            > = api.storage().at(at).fetch(&clp::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        "Polkadot People" => {
            let slot: Option<ppldot::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&ppldot::storage().aura().current_slot()).await?;
            let auths: Option<
                ppldot::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    ppldot::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&ppldot::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }

        // Kusama family (Aura sr25519)
        "Kusama Asset Hub" => {
            let slot: Option<ahk::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&ahk::storage().aura().current_slot()).await?;
            let auths: Option<
                ahk::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    ahk::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&ahk::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        "Kusama Bridge Hub" => {
            let slot: Option<bhk::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&bhk::storage().aura().current_slot()).await?;
            let auths: Option<
                bhk::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    bhk::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&bhk::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        "Kusama Coretime" => {
            let slot: Option<ctk::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&ctk::storage().aura().current_slot()).await?;
            let auths: Option<
                ctk::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    ctk::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&ctk::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        "Kusama People" => {
            let slot: Option<pplksm::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&pplksm::storage().aura().current_slot()).await?;
            let auths: Option<
                pplksm::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    pplksm::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&pplksm::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        "Kusama Encointer" => {
            let slot: Option<enk::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&enk::storage().aura().current_slot()).await?;
            let auths: Option<
                enk::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    enk::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&enk::storage().aura().authorities()).await?;
            derive_from_slot_and_auths(slot.as_ref().map(|s| s.0), auths.as_ref().map(|b| &b.0))
        }
        _ => Ok(None),
    }
}

fn derive_from_slot_and_auths<T>(slot_num: Option<u64>, auths: Option<&Vec<T>>) -> Result<Option<[u8; 32]>>
where
    T: core::borrow::Borrow<([u8; 32])> + Clone,
{
    if let (Some(slot), Some(list)) = (slot_num, auths) {
        if list.is_empty() { return Ok(None); }
        let idx = (slot as usize) % list.len();
        // both app_ed25519::Public and app_sr25519::Public are tuple wrappers over [u8;32]
        let raw: [u8; 32] = list[idx].clone().borrow().clone();
        Ok(Some(raw))
    } else {
        Ok(None)
    }
}

async fn session_key_owner_account(
    api: &OnlineClient<PolkadotConfig>,
    at: H256,
    cfg: &ChainCfg,
    session_key_raw32: [u8; 32],
) -> Result<Option<[u8; 32]>> {
    // Build tuple (KeyTypeId("aura"), Bytes(session_key))
    let key_bytes = session_key_raw32.to_vec();

    macro_rules! key_owner {
        ($mod:ident) => {{
            let kt = $mod::runtime_types::sp_core::crypto::KeyTypeId(*b"aura");
            let call = $mod::storage().session().key_owner((kt, key_bytes.clone()));
            let owner_opt = api.storage().at(at).fetch(&call).await?;
            Ok(owner_opt.map(account_to_raw32))
        }};
    }

    match cfg.name {
        // Polkadot
        "Polkadot Asset Hub"   => key_owner!(ahp),
        "Polkadot Bridge Hub"  => key_owner!(bhp),
        "Polkadot Coretime"    => key_owner!(ctp),
        "Polkadot Collectives" => key_owner!(clp),
        "Polkadot People"      => key_owner!(ppldot),
        // Kusama
        "Kusama Asset Hub"     => key_owner!(ahk),
        "Kusama Bridge Hub"    => key_owner!(bhk),
        "Kusama Coretime"      => key_owner!(ctk),
        "Kusama People"        => key_owner!(pplksm),
        "Kusama Encointer"     => key_owner!(enk),
        _ => Ok(None),
    }
}

/// Convert a runtime AccountId32 (opaque newtype) into [u8;32] by SCALE-encoding.
fn account_to_raw32<T: Encode>(acc: T) -> [u8; 32] {
    let bytes = acc.encode();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes[..32]);
    out
}

// ===== Identity resolution on People chains =====

async fn resolve_identity_label(
    id_api: &OnlineClient<PolkadotConfig>,
    cfg: &ChainCfg,
    who: [u8; 32],
    ss58_prefix: u16,
) -> Result<String> {
    let ss58 = ss58_from_raw32_with_prefix(who, ss58_prefix, cfg.aura_kind);

    match cfg.family {
        Family::Polkadot => {
            // First check if it's a sub-identity
            if let Some((parent, sub_data)) = id_super_of_dot(id_api, who).await? {
                let parent_disp = id_display_dot(id_api, parent).await?.unwrap_or_else(|| ss58_from_raw32_with_prefix(parent, ss58_prefix, cfg.aura_kind));
                let sub = data_to_str_dot(&sub_data).unwrap_or_else(|| "<sub>".into());
                return Ok(format!("{parent_disp} / {sub}"));
            }
            // else try primary identity
            if let Some(display) = id_display_dot(id_api, who).await? {
                return Ok(display);
            }
        }
        Family::Kusama => {
            if let Some((parent, sub_data)) = id_super_of_ksm(id_api, who).await? {
                let parent_disp = id_display_ksm(id_api, parent).await?.unwrap_or_else(|| ss58_from_raw32_with_prefix(parent, ss58_prefix, cfg.aura_kind));
                let sub = data_to_str_ksm(&sub_data).unwrap_or_else(|| "<sub>".into());
                return Ok(format!("{parent_disp} / {sub}"));
            }
            if let Some(display) = id_display_ksm(id_api, who).await? {
                return Ok(display);
            }
        }
    }

    // fallback to address
    Ok(ss58)
}

// ---- People Polkadot (ppldot) identity helpers ----

async fn id_super_of_dot(
    api: &OnlineClient<PolkadotConfig>,
    who: [u8;32],
) -> Result<Option<([u8;32], ppldot::runtime_types::pallet_identity::types::Data)>> {
    let acct = ppldot::runtime_types::sp_runtime::account_id32::AccountId32 { network: None, id: who };
    let opt = api.storage().at_latest().await?
        .fetch(&ppldot::storage().identity().super_of(&acct))
        .await?;
    if let Some((parent, data)) = opt {
        Ok(Some((account_to_raw32(parent), data)))
    } else {
        Ok(None)
    }
}

async fn id_display_dot(
    api: &OnlineClient<PolkadotConfig>,
    who: [u8;32],
) -> Result<Option<String>> {
    let acct = ppldot::runtime_types::sp_runtime::account_id32::AccountId32 { network: None, id: who };
    let reg_opt = api.storage().at_latest().await?
        .fetch(&ppldot::storage().identity().identity_of(&acct))
        .await?;
    if let Some(reg) = reg_opt {
        let data = reg.info.display;
        Ok(data_to_str_dot(&data))
    } else {
        Ok(None)
    }
}

fn data_to_str_dot(d: &ppldot::runtime_types::pallet_identity::types::Data) -> Option<String> {
    #[allow(unreachable_patterns)]
    match d {
        // Common modern variants:
        ppldot::runtime_types::pallet_identity::types::Data::Raw(b) => Some(bytes_to_string_lossy(b)),
        ppldot::runtime_types::pallet_identity::types::Data::None => None,
        // Older/other variants – show hex fallback:
        other => Some(format!("0x{}", hex::encode(other.encode()))),
    }
}

// ---- People Kusama (pplksm) identity helpers ----

async fn id_super_of_ksm(
    api: &OnlineClient<PolkadotConfig>,
    who: [u8;32],
) -> Result<Option<([u8;32], pplksm::runtime_types::pallet_identity::types::Data)>> {
    let acct = pplksm::runtime_types::sp_runtime::account_id32::AccountId32 { network: None, id: who };
    let opt = api.storage().at_latest().await?
        .fetch(&pplksm::storage().identity().super_of(&acct))
        .await?;
    if let Some((parent, data)) = opt {
        Ok(Some((account_to_raw32(parent), data)))
    } else {
        Ok(None)
    }
}

async fn id_display_ksm(
    api: &OnlineClient<PolkadotConfig>,
    who: [u8;32],
) -> Result<Option<String>> {
    let acct = pplksm::runtime_types::sp_runtime::account_id32::AccountId32 { network: None, id: who };
    let reg_opt = api.storage().at_latest().await?
        .fetch(&pplksm::storage().identity().identity_of(&acct))
        .await?;
    if let Some(reg) = reg_opt {
        let data = reg.info.display;
        Ok(data_to_str_ksm(&data))
    } else {
        Ok(None)
    }
}

fn data_to_str_ksm(d: &pplksm::runtime_types::pallet_identity::types::Data) -> Option<String> {
    #[allow(unreachable_patterns)]
    match d {
        pplksm::runtime_types::pallet_identity::types::Data::Raw(b) => Some(bytes_to_string_lossy(b)),
        pplksm::runtime_types::pallet_identity::types::Data::None => None,
        other => Some(format!("0x{}", hex::encode(other.encode()))),
    }
}

// ---- small utils for identity text ----

fn bytes_to_string_lossy(v: &Vec<u8>) -> String {
    match std::str::from_utf8(v) {
        Ok(s) => s.to_string(),
        Err(_) => String::from_utf8_lossy(v).to_string(),
    }
}
