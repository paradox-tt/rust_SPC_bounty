use anyhow::{anyhow, bail, Context, Result};
use chrono::{Months, TimeZone, Utc};
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use serde::Deserialize;
use sp_core::crypto::{Ss58AddressFormat, Ss58Codec};
use sp_core::{sr25519, H256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use subxt::{OnlineClient, PolkadotConfig};
use tokio::sync::Mutex;

// ====== Subxt metadata modules (one per chain) ======
#[subxt::subxt(runtime_metadata_path = "metadata/asset-hub-polkadot.scale")]
pub mod ahp_polkadot {}

#[subxt::subxt(runtime_metadata_path = "metadata/asset-hub-kusama.scale")]
pub mod ahp_kusama {}

#[subxt::subxt(runtime_metadata_path = "metadata/bridge-hub-polkadot.scale")]
pub mod bridgehub_polkadot {}
#[subxt::subxt(runtime_metadata_path = "metadata/coretime-polkadot.scale")]
pub mod coretime_polkadot {}
#[subxt::subxt(runtime_metadata_path = "metadata/collectives-polkadot.scale")]
pub mod collectives_polkadot {}
#[subxt::subxt(runtime_metadata_path = "metadata/people-polkadot.scale")]
pub mod people_polkadot {}
#[subxt::subxt(runtime_metadata_path = "metadata/bridge-hub-kusama.scale")]
pub mod bridgehub_kusama {}
#[subxt::subxt(runtime_metadata_path = "metadata/coretime-kusama.scale")]
pub mod coretime_kusama {}
#[subxt::subxt(runtime_metadata_path = "metadata/people-kusama.scale")]
pub mod people_kusama {}
#[subxt::subxt(runtime_metadata_path = "metadata/encointer-kusama.scale")]
pub mod encointer_kusama {}

// ---------------- requested chains ----------------
#[derive(Clone, Copy, Debug)]
struct ChainCfg {
    name: &'static str,
    ws:   &'static str,
    ss58: u16,       // network prefix for AccountId SS58 (0 for DOT, 2 for KSM)
    // true = aura session key is sr25519; false = ed25519 (only used for debugging session key, if needed)
    session_is_sr25519: bool,
}

const CHAINS: &[ChainCfg] = &[
    // Polkadot
    ChainCfg { name: "Polkadot Asset Hub",   ws: "wss://rpc-asset-hub-polkadot.luckyfriday.io",  ss58: 0, session_is_sr25519: false },
    ChainCfg { name: "Polkadot Bridge Hub",  ws: "wss://rpc-bridge-hub-polkadot.luckyfriday.io", ss58: 0, session_is_sr25519: true  },
    ChainCfg { name: "Polkadot Coretime",    ws: "wss://rpc-coretime-polkadot.luckyfriday.io",   ss58: 0, session_is_sr25519: true  },
    ChainCfg { name: "Polkadot Collectives", ws: "wss://rpc-collectives-polkadot.luckyfriday.io",ss58: 0, session_is_sr25519: true  },
    ChainCfg { name: "Polkadot People",      ws: "wss://rpc-people-polkadot.luckyfriday.io",     ss58: 0, session_is_sr25519: true  },
    // Kusama
    ChainCfg { name: "Kusama Asset Hub",     ws: "wss://rpc-asset-hub-kusama.luckyfriday.io",    ss58: 2, session_is_sr25519: true  },
    ChainCfg { name: "Kusama Bridge Hub",    ws: "wss://rpc-bridge-hub-kusama.luckyfriday.io",   ss58: 2, session_is_sr25519: true  },
    ChainCfg { name: "Kusama Coretime",      ws: "wss://rpc-coretime-kusama.luckyfriday.io",     ss58: 2, session_is_sr25519: true  },
    ChainCfg { name: "Kusama People",        ws: "wss://rpc-people-kusama.luckyfriday.io",       ss58: 2, session_is_sr25519: true  },
    ChainCfg { name: "Kusama Encointer",     ws: "wss://rpc-encointer-kusama.luckyfriday.io",    ss58: 2, session_is_sr25519: true  },
];

// ---------------- knobs ----------------
const REWARD_USD: f64 = 300.0;
const CHUNK_SIZE: usize = 10_000;
const CONCURRENCY: usize = 32;
const CHUNK_CONCURRENCY: usize = 20;
const CALL_TIMEOUT_SECS: u64 = 20;

// ---------------- NO-REWARD LIST ----------------

// Parity & Encointer collators over all chains, plus mischief anon collators.
// NOTE: These are chain-prefixed SS58 addresses (0=DOT, 2=KSM), not generic 42.
const NO_REWARD_COLLATORS: &[&str] = &[
    // Polkadot AssetHub
    "12ixt2xmCJKuLXjM3gh1SY7C3aj4gBoBUqExTBTGhLCSATFw",
    "15X2eHehrexKqz6Bs6fQTjptP2ndn39eYdQTeREVeRk32p54",
    // Polkadot BridgeHub
    "134AK3RiMA97Fx9dLj1CvuLJUa8Yo93EeLA1TkP6CCGnWMSd",
    "15dU8Tt7kde2diuHzijGbKGPU5K8BPzrFJfYFozvrS1DdE21",
    // Polkadot Collectives
    "1NvWYSswSt5v95m5z9JycedzTXEWJ9Zcgbu5BMnGAwiWUC9",
    "12n87jggYnvxvdHJaEiTAKZF7ZniJqxafYoKzEqfJCUDvJXP",
    // Polkadot People
    "14QhqUX7kux5PggbBwUFFZNuLvfX2CjzUQ9V56m4d4S67Pgn",
    "14QhqUX7kux5PggbBwUFFZNuLvfX2CjzUQ9V56m4d4S67Pgn",
    // Polkadot Coretime
    "13NAwtroa2efxgtih1oscJqjxcKpWJeQF8waWPTArBewi2CQ",
    "13umUoWwGb765EPzMUrMmYTcEjKfNJiNyCDwdqAvCMzteGzi",
    // Kusama AssetHub
    "EPk1wv1TvVFfsiG73YLuLAtGacfPmojyJKvmifobBzUTxFv",
    "JL21EURyqQxJk9inVW7iuexJNzzuV7HpZJVxQrY8BzwFiTJ",
    // Kusama BridgeHub
    "DQkekNBt8g6D7bPUEqhgfujADxzzfivr1qQZJkeGzAqnEzF",
    "HbUc5qrLtKAZvasioiTSf1CunaN2SyEwvfsgMuYQjXA5sfk",
    // Kusama Coretime
    "Cx9Uu2sxp3Xt1QBUbGQo7j3imTvjWJrqPF1PApDoy6UVkWP",
    "HRn3a4qLmv1ejBHvEbnjaiEWjt154iFi2Wde7bXKGUwGvtL",
    // Kusama People
    // "CbLd7BdUr8DqD4TciR1kH6w12bbHBCW9n2MHGCtbxq4U5ty", // BLD
    "CuLgnS17KwfweeoN9y59YrhDG4pekfiY8qxieDaVTcVCjuP",
    // "E8X4LxU9zEiNVAyM95ERDeomMmwwqn7RBCuRMEZCfgFm3J1", // Openbit Labs
    "HNrgbuMxf7VLwsMd6YjnNQM6fc7VVsaoNVaMYTCCfK3TRWJ",
    // Kusama Encointer
    "FG2C6WJWFdBNgKGDdS6oyhP1K9zHLNNzRtvAJNbmV1FybzD",
    "Fsn4ArZxAtESoGmwnLVbiKPsrgjFNmGLLdVapjVPCD78mRA",
    "G6z6FmKhw6dHJ8a5tetrzarbsVU4jF8LhoRFk211GryqAdw",
    "GwDHvd1aToQRKa2b9rATV5igF99Bwr12Ko7jDZfPdNBTGT4",
    // RAVEN (excluded due to identity theft)
    "FRt6xsJzQp8isxEXTRGVfymNaosKbWihPCqy7XFKd9v5y6X",
];

// ---------------- identity JSON ----------------

#[derive(Debug, Deserialize)]
struct IdentityJson {
    address: String, // 42-prefix SS58
    name: String,
    #[serde(default)]
    sub: String,
}
#[derive(Debug, Default)]
struct IdentityMaps {
    // key: 42-prefix SS58, value: "Primary/Sub" or just "Primary"
    polkadot: HashMap<String, String>,
    kusama: HashMap<String, String>,
}

#[derive(Clone)]
struct Row {
    owner_raw: [u8; 32],
    author_ss58: String,
    identity: String,
    blocks: usize,
    pct_total: f64,
}

impl IdentityMaps {
    fn load() -> Result<Self> {
        let mut m = IdentityMaps::default();

        // Polkadot
        match fs::read_to_string("assets/polkadot-identities.json") {
            Ok(s) => {
                let list: Vec<IdentityJson> = serde_json::from_str(&s)
                    .context("parse assets/polkadot-identities.json")?;
                for e in list {
                    let display = if e.sub.trim().is_empty() {
                        e.name.clone()
                    } else {
                        format!("{}/{}", e.name, e.sub)
                    };
                    m.polkadot.insert(e.address, display);
                }
                eprintln!(
                    "Loaded {} entries from assets/polkadot-identities.json",
                    m.polkadot.len()
                );
            }
            Err(e) => {
                eprintln!("WARN: cannot read assets/polkadot-identities.json: {e}");
            }
        }

        // Kusama
        match fs::read_to_string("assets/kusama-identities.json") {
            Ok(s) => {
                let list: Vec<IdentityJson> = serde_json::from_str(&s)
                    .context("parse assets/kusama-identities.json")?;
                for e in list {
                    let display = if e.sub.trim().is_empty() {
                        e.name.clone()
                    } else {
                        format!("{}/{}", e.name, e.sub)
                    };
                    m.kusama.insert(e.address, display);
                }
                eprintln!(
                    "Loaded {} entries from assets/kusama-identities.json",
                    m.kusama.len()
                );
            }
            Err(e) => {
                eprintln!("WARN: cannot read assets/kusama-identities.json: {e}");
            }
        }

        Ok(m)
    }

    fn lookup(&self, chain: &ChainCfg, owner_raw: [u8; 32]) -> Option<String> {
        // Convert chain-specific AccountId32 (raw) to generic 42-prefix Substrate SS58
        let generic_fmt = Ss58AddressFormat::custom(42); // 42 = generic Substrate prefix
        let generic_ss58 =
            sr25519::Public::from_raw(owner_raw).to_ss58check_with_version(generic_fmt);

        let (which, map) = if chain.ss58 == 0 {
            ("Polkadot", &self.polkadot)
        } else {
            ("Kusama", &self.kusama)
        };

        eprintln!(
            "DEBUG identity: Looking up generic address {} for {} chain, using {} entries",
            generic_ss58,
            which,
            map.len()
        );
        map.get(&generic_ss58).cloned()
    }
}

// ---------------- CSV saving ----------------

/// Save collator data to CSV with organized folder structure
fn save_to_csv(
    rows: &[Row],
    chain: &ChainCfg,
    year: i32,
    month: u8,
    max_count: usize,
    ema: f64,
    no_reward_set: &HashSet<&str>,
) -> Result<()> {
    // Determine relay chain
    let relay_chain = if chain.ss58 == 0 { "polkadot" } else { "kusama" };

    // Create folder: output/{YYYY-MM}/{relay_chain}/
    let folder_name = format!("{:04}-{:02}", year, month);
    let output_dir = PathBuf::from("../SystemCollatorCSVFiles")
        .join(&folder_name)
        .join(relay_chain);

    std::fs::create_dir_all(&output_dir)?;

    // Filename: sanitized chain name
    let chain_name_sanitized = chain.name
        .replace(" ", "_")
        .to_lowercase();
    let csv_path = output_dir.join(format!("{}.csv", chain_name_sanitized));

    // Build CSV
    let mut csv = String::new();
    csv.push_str("address,identity,blocks,pct_total,pct_top,payout_usd,payout_tokens,skip_reason\n");

    for row in rows {
        if row.author_ss58 == "UNKNOWN" {
            continue;
        }

        let pct_top = if max_count > 0 {
            (row.blocks as f64) * 100.0 / (max_count as f64)
        } else { 0.0 };

        let payout_usd = 300.0 * (pct_top / 100.0);
        let payout_tokens = payout_usd / ema;

        let identity = if row.identity.is_empty() {
            String::new()
        } else {
            format!("\"{}\"", row.identity.replace("\"", "\"\""))
        };

        let skip_reason = if no_reward_set.contains(row.author_ss58.as_str()) {
            "no_reward_list"
        } else {
            ""
        };

        csv.push_str(&format!(
            "{},{},{},{:.4},{:.4},{:.2},{:.10},{}\n",
            row.author_ss58,
            identity,
            row.blocks,
            row.pct_total,
            pct_top,
            payout_usd,
            payout_tokens,
            skip_reason
        ));
    }

    std::fs::write(&csv_path, csv)?;
    println!("✅ CSV saved: {}", csv_path.display());

    Ok(())
}

// ---------------- main ----------------
#[tokio::main]
async fn main() -> Result<()> {
    let chain = prompt_chain()?;
    let Inputs { month, year, ema } = prompt_inputs()?;

    // identities
    let identity_maps = IdentityMaps::load().unwrap_or_else(|e| {
        eprintln!("WARN: identity loading failed: {e:#}");
        IdentityMaps::default()
    });

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
        "==> Chain: {}  |  RPC: {}\n==> Window: [{} .. {})  |  EMA: {}",
        chain.name, chain.ws, start_dt.to_rfc3339(), end_dt.to_rfc3339(), ema
    );

    // connections
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(chain.ws)
        .await
        .with_context(|| format!("connect subxt to {}", chain.ws))?;
    let rpc = Arc::new(
        WsClientBuilder::default()
            .build(chain.ws)
            .await
            .with_context(|| format!("connect rpc ws to {}", chain.ws))?,
    );

    // latest
    let latest = api.blocks().at_latest().await?;
    let latest_num = latest.number();
    let latest_ts = block_timestamp_typed(&api, latest.hash(), chain).await?.unwrap_or(0);
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
                let chain = chain;

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
                        let chain = chain;

                        async move {
                            let res: Result<()> = async {
                                let h = block_hash_by_number(&rpc, n).await?;

                                // 1) derive aura session key (slot % authorities) via typed storage
                                let session_key_opt = derive_session_key_typed(&api, h, chain).await?;

                                // 2) resolve owner via Session::KeyOwner((KeyTypeId("aura"), key_bytes))
                                if let Some(sess_key) = session_key_opt {
                                    if let Some(owner_raw) = session_key_owner_account_typed(&api, h, chain, sess_key).await? {
                                        let mut sm = stats.lock().await;
                                        *sm.entry(owner_raw).or_insert(0) += 1;
                                    } else {
                                        let mut u = unknowns.lock().await;
                                        *u += 1;
                                        let mut sm = stats.lock().await;
                                        *sm.entry([0u8; 32]).or_insert(0) += 1;
                                    }
                                } else {
                                    let mut u = unknowns.lock().await;
                                    *u += 1;
                                    let mut sm = stats.lock().await;
                                    *sm.entry([0u8; 32]).or_insert(0) += 1;
                                }

                                // progress
                                chunk_pb.inc(1);
                                overall_pb.inc(1);
                                Ok(())
                            }.await;

                            if let Err(e) = res {
                                let mut be = block_errors.lock().await;
                                *be += 1;
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
    let total_scanned: usize = stats.values().copied().sum();

    // Build NO_REWARD set for quick membership test
    let no_reward_set: HashSet<&'static str> = NO_REWARD_COLLATORS.iter().copied().collect();

    let mut rows: Vec<Row> = stats
        .into_iter()
        .map(|(owner_raw, cnt)| {
            if owner_raw == [0u8; 32] {
                Row {
                    owner_raw,
                    author_ss58: "UNKNOWN".to_string(),
                    identity: "-".to_string(),
                    blocks: cnt,
                    pct_total: if total_scanned > 0 {
                        (cnt as f64) * 100.0 / (total_scanned as f64)
                    } else { 0.0 },
                }
            } else {
                let author_ss58 = ss58_from_raw32_with_prefix(owner_raw, chain.ss58);
                let identity = identity_maps
                    .lookup(&chain, owner_raw)
                    .unwrap_or_else(|| "".to_string());

                Row {
                    owner_raw,
                    author_ss58,
                    identity,
                    blocks: cnt,
                    pct_total: if total_scanned > 0 {
                        (cnt as f64) * 100.0 / (total_scanned as f64)
                    } else { 0.0 },
                }
            }
        })
        .collect();

    // Sort: known authors first by descending blocks, UNKNOWN at bottom
    rows.sort_by(|a, b| {
        if a.author_ss58 == "UNKNOWN" && b.author_ss58 != "UNKNOWN" {
            std::cmp::Ordering::Greater
        } else if b.author_ss58 == "UNKNOWN" && a.author_ss58 != "UNKNOWN" {
            std::cmp::Ordering::Less
        } else {
            b.blocks.cmp(&a.blocks)
        }
    });

    let max_count = rows.iter().map(|r| r.blocks).max().unwrap_or(0);

    println!("\n================ SUMMARY (full scan) ================");
    println!("Chain:      {}", chain.name);
    println!("Chain RPC:  {}", chain.ws);
    println!("Window:     [{} .. {})", start_dt.to_rfc3339(), end_dt.to_rfc3339());
    println!("Blocks scanned: {}", total_scanned);
    println!(
        "{:<6}  {:<48}  {:<28}  {:>8}  {:>7}  {:>7}  {:>9}  {:>11}",
        "Rank", "Author (Owner SS58)", "Identity", "Blocks", "%", "%Top", "Payout", "Payout/EMA"
    );
    println!("{}", "-".repeat(140));

    for (i, row) in rows.iter().enumerate() {
        let pct_top = if max_count > 0 {
            (row.blocks as f64) * 100.0 / (max_count as f64)
        } else { 0.0 };
        let payout = REWARD_USD * (pct_top / 100.0);
        let payout_per_ema = payout / ema;
        let id_display = if row.identity.is_empty() { "-".to_string() } else { row.identity.clone() };

        println!(
            "{:<6}  {:<48}  {:<28}  {:>8}  {:>7.2}  {:>7.2}  {:>9.2}  {:>11.6}",
            i + 1,
            row.author_ss58,
            id_display,
            row.blocks,
            row.pct_total,
            pct_top,
            payout,
            payout_per_ema
        );
    }
    println!("{}", "-".repeat(140));
    println!("Note: '%' is share of all blocks in window; '%Top' is relative to the top producer.");
    println!("Assumed reward pool for '%Top' payout: ${:.2}", REWARD_USD);
    println!("EMA used: {}", ema);

    // diagnostics
    let unknowns = Arc::try_unwrap(unknowns).unwrap().into_inner();
    let block_errors = Arc::try_unwrap(block_errors).unwrap().into_inner();
    eprintln!("Diagnostics: unknown-authors={}, block-errors={}", unknowns, block_errors);

    // Prompt and save CSV
    println!("\n{}", "=".repeat(80));
    print!("Save this data to CSV? (y/n): ");
    io::stdout().flush()?;

    let mut response = String::new();
    io::stdin().read_line(&mut response)?;

    if response.trim().eq_ignore_ascii_case("y") {
        save_to_csv(
            &rows,
            &chain,
            year,
            month as u8,
            max_count,
            ema,
            &no_reward_set,
        )?;
    }

    println!("\n==> Done.");
    Ok(())
}

// ---------------- interactive ----------------
struct Inputs { month: u8, year: i32, ema: f64 }

fn prompt_chain() -> Result<ChainCfg> {
    loop {
        println!("Select chain:");
        println!("  1) Polkadot  Asset Hub");
        println!("  2) Polkadot  Bridge Hub");
        println!("  3) Polkadot  Coretime");
        println!("  4) Polkadot  Collectives");
        println!("  5) Polkadot  People");
        println!("  6) Kusama    Asset Hub");
        println!("  7) Kusama    Bridge Hub");
        println!("  8) Kusama    Coretime");
        println!("  9) Kusama    People");
        println!(" 10) Kusama    Encointer");
        print!("Enter selection (1-10): ");
        io::stdout().flush().ok();
        let mut s = String::new();
        io::stdin().read_line(&mut s)?;
        match s.trim() {
            "1" => return Ok(CHAINS[0]),
            "2" => return Ok(CHAINS[1]),
            "3" => return Ok(CHAINS[2]),
            "4" => return Ok(CHAINS[3]),
            "5" => return Ok(CHAINS[4]),
            "6" => return Ok(CHAINS[5]),
            "7" => return Ok(CHAINS[6]),
            "8" => return Ok(CHAINS[7]),
            "9" => return Ok(CHAINS[8]),
            "10" => return Ok(CHAINS[9]),
            _ => eprintln!("  -> Please enter 1..10."),
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

    Ok(Inputs { month, year, ema })
}

// ---------------- typed storage helpers ----------------

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

async fn block_timestamp_typed(api: &OnlineClient<PolkadotConfig>, at: H256, chain: ChainCfg) -> Result<Option<u64>> {
    Ok(match chain.name {
        "Polkadot Asset Hub"   => api.storage().at(at).fetch(&ahp_polkadot::storage().timestamp().now()).await?,
        "Polkadot Bridge Hub"  => api.storage().at(at).fetch(&bridgehub_polkadot::storage().timestamp().now()).await?,
        "Polkadot Coretime"    => api.storage().at(at).fetch(&coretime_polkadot::storage().timestamp().now()).await?,
        "Polkadot Collectives" => api.storage().at(at).fetch(&collectives_polkadot::storage().timestamp().now()).await?,
        "Polkadot People"      => api.storage().at(at).fetch(&people_polkadot::storage().timestamp().now()).await?,
        "Kusama Asset Hub"     => api.storage().at(at).fetch(&ahp_kusama::storage().timestamp().now()).await?,
        "Kusama Bridge Hub"    => api.storage().at(at).fetch(&bridgehub_kusama::storage().timestamp().now()).await?,
        "Kusama Coretime"      => api.storage().at(at).fetch(&coretime_kusama::storage().timestamp().now()).await?,
        "Kusama People"        => api.storage().at(at).fetch(&people_kusama::storage().timestamp().now()).await?,
        "Kusama Encointer"     => api.storage().at(at).fetch(&encointer_kusama::storage().timestamp().now()).await?,
        _ => None,
    })
}

async fn bin_search_first_ge(
    api: &OnlineClient<PolkadotConfig>,
    rpc: &Arc<jsonrpsee::ws_client::WsClient>,
    mut lo: u32,
    mut hi: u32,
    target_ms: u64,
    chain: ChainCfg,
) -> Result<u32> {
    let lo_ts = block_timestamp_typed(api, block_hash_by_number(rpc, lo).await?, chain).await?.unwrap_or(0);
    let hi_ts = block_timestamp_typed(api, block_hash_by_number(rpc, hi).await?, chain).await?.unwrap_or(0);

    if hi_ts < target_ms {
        bail!("bin_search_first_ge: hi(#{} ts={}) < target {}", hi, fmt_ts(hi_ts), fmt_ts(target_ms));
    }
    if lo_ts >= target_ms { return Ok(lo); }

    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_h = block_hash_by_number(rpc, mid).await?;
        let mid_ts = block_timestamp_typed(api, mid_h, chain).await?.unwrap_or(0);
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

fn ss58_from_raw32_with_prefix(raw: [u8; 32], prefix: u16) -> String {
    let fmt = Ss58AddressFormat::custom(prefix);
    sr25519::Public::from_raw(raw).to_ss58check_with_version(fmt)
}

// ---- author resolution with TYPED metadata ----

async fn derive_session_key_typed(
    api: &OnlineClient<PolkadotConfig>,
    at: H256,
    chain: ChainCfg,
) -> Result<Option<[u8; 32]>> {
    macro_rules! pick_key {
        ($slot_opt:expr, $auths_opt:expr) => {{
            let slot_opt = $slot_opt;
            let auths_opt = $auths_opt;
            if let (Some(slot), Some(bv)) = (slot_opt, auths_opt) {
                let v = bv.0;
                if v.is_empty() { None } else {
                    let idx = (slot.0 as usize) % v.len();
                    Some(v[idx].0)
                }
            } else { None }
        }};
    }

    let key_opt = match chain.name {
        // Polkadot
        "Polkadot Asset Hub" => {
            let slot: Option<ahp_polkadot::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&ahp_polkadot::storage().aura().current_slot()).await?;
            let auths: Option<
                ahp_polkadot::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    ahp_polkadot::runtime_types::sp_consensus_aura::ed25519::app_ed25519::Public
                >
            > = api.storage().at(at).fetch(&ahp_polkadot::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        "Polkadot Bridge Hub" => {
            let slot: Option<bridgehub_polkadot::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&bridgehub_polkadot::storage().aura().current_slot()).await?;
            let auths: Option<
                bridgehub_polkadot::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    bridgehub_polkadot::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&bridgehub_polkadot::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        "Polkadot Coretime" => {
            let slot: Option<coretime_polkadot::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&coretime_polkadot::storage().aura().current_slot()).await?;
            let auths: Option<
                coretime_polkadot::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    coretime_polkadot::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&coretime_polkadot::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        "Polkadot Collectives" => {
            let slot: Option<collectives_polkadot::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&collectives_polkadot::storage().aura().current_slot()).await?;
            let auths: Option<
                collectives_polkadot::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    collectives_polkadot::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&collectives_polkadot::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        "Polkadot People" => {
            let slot: Option<people_polkadot::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&people_polkadot::storage().aura().current_slot()).await?;
            let auths: Option<
                people_polkadot::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    people_polkadot::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&people_polkadot::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        // Kusama
        "Kusama Asset Hub" => {
            let slot: Option<ahp_kusama::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&ahp_kusama::storage().aura().current_slot()).await?;
            let auths: Option<
                ahp_kusama::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    ahp_kusama::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&ahp_kusama::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        "Kusama Bridge Hub" => {
            let slot: Option<bridgehub_kusama::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&bridgehub_kusama::storage().aura().current_slot()).await?;
            let auths: Option<
                bridgehub_kusama::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    bridgehub_kusama::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&bridgehub_kusama::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        "Kusama Coretime" => {
            let slot: Option<coretime_kusama::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&coretime_kusama::storage().aura().current_slot()).await?;
            let auths: Option<
                coretime_kusama::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    coretime_kusama::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&coretime_kusama::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        "Kusama People" => {
            let slot: Option<people_kusama::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&people_kusama::storage().aura().current_slot()).await?;
            let auths: Option<
                people_kusama::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    people_kusama::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&people_kusama::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        "Kusama Encointer" => {
            let slot: Option<encointer_kusama::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&encointer_kusama::storage().aura().current_slot()).await?;
            let auths: Option<
                encointer_kusama::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    encointer_kusama::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&encointer_kusama::storage().aura().authorities()).await?;
            pick_key!(slot, auths)
        }
        _ => None,
    };

    Ok(key_opt)
}

async fn session_key_owner_account_typed(
    api: &OnlineClient<PolkadotConfig>,
    at: H256,
    chain: ChainCfg,
    session_key_raw32: [u8; 32],
) -> Result<Option<[u8; 32]>> {
    let aura = *b"aura";

    macro_rules! fetch_owner {
        ($call:expr) => {{
            let owner_opt = api.storage().at(at).fetch(&$call).await?;
            Ok(owner_opt.map(account_to_raw32))
        }};
    }

    match chain.name {
        // Polkadot
        "Polkadot Asset Hub" => {
            let kt = ahp_polkadot::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = ahp_polkadot::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        "Polkadot Bridge Hub" => {
            let kt = bridgehub_polkadot::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = bridgehub_polkadot::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        "Polkadot Coretime" => {
            let kt = coretime_polkadot::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = coretime_polkadot::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        "Polkadot Collectives" => {
            let kt = collectives_polkadot::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = collectives_polkadot::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        "Polkadot People" => {
            let kt = people_polkadot::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = people_polkadot::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        // Kusama
        "Kusama Asset Hub" => {
            let kt = ahp_kusama::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = ahp_kusama::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        "Kusama Bridge Hub" => {
            let kt = bridgehub_kusama::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = bridgehub_kusama::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        "Kusama Coretime" => {
            let kt = coretime_kusama::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = coretime_kusama::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        "Kusama People" => {
            let kt = people_kusama::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = people_kusama::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        "Kusama Encointer" => {
            let kt = encointer_kusama::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = encointer_kusama::storage().session().key_owner((kt, session_key_raw32.to_vec()));
            fetch_owner!(call)
        }
        _ => Ok(None),
    }
}

/// Convert a runtime AccountId32 (opaque newtype) into [u8;32] by SCALE-encoding then truncating.
fn account_to_raw32<T: parity_scale_codec::Encode>(acc: T) -> [u8; 32] {
    let bytes = acc.encode();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes[..32]);
    out
}