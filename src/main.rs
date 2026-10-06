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
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;
use subxt::{OnlineClient, PolkadotConfig};
use tokio::sync::Mutex;

// ====== Subxt metadata modules (one per chain) ======
#[subxt::subxt(runtime_metadata_path = "metadata/asset-hub-polkadot.scale")]
pub mod ahp_polkadot {}

#[subxt::subxt(runtime_metadata_path = "metadata/asset-hub-kusama.scale")]
pub mod ahp_kusama {}

// Polkadot Bulletin chain
#[subxt::subxt(runtime_metadata_path = "metadata/bulletin-polkadot.scale")]
pub mod bulletin_polkadot {}

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
    // RPC endpoints in order of preference. If a call fails (connection drop, timeout,
    // pruned state...) the same call is retried on the next endpoint.
    rpcs: &'static [&'static str],
    ss58: u16,       // network prefix for AccountId SS58 (0 for DOT, 2 for KSM)
    // true = aura session key is sr25519; false = ed25519 (only used for debugging session key, if needed)
    session_is_sr25519: bool,
}

const CHAINS: &[ChainCfg] = &[
    // Polkadot (internal nodes; ws:// because they're on the LAN, no TLS)
    ChainCfg { name: "Polkadot Asset Hub",   rpcs: &["ws://192.168.250.179:9944", "ws://192.168.250.226:9944"], ss58: 0, session_is_sr25519: false },
    ChainCfg { name: "Polkadot Bridge Hub",  rpcs: &["ws://192.168.250.180:9944", "ws://192.168.250.227:9944"], ss58: 0, session_is_sr25519: true  },
    ChainCfg { name: "Polkadot Coretime",    rpcs: &["ws://192.168.250.220:9944", "ws://192.168.250.230:9944"], ss58: 0, session_is_sr25519: true  },
    ChainCfg { name: "Polkadot Collectives", rpcs: &["ws://192.168.250.181:9944", "ws://192.168.250.228:9944"], ss58: 0, session_is_sr25519: true  },
    ChainCfg { name: "Polkadot People",      rpcs: &["ws://192.168.250.219:9944", "ws://192.168.250.229:9944"], ss58: 0, session_is_sr25519: true  },
    // Polkadot Bulletin chain (ss58 prefix 0; no validators excluded; no_reward list is empty)
    // No internal node yet - public ARCHIVE endpoint (LuckyFriday's Bulletin RPC is pruned).
    // Collators matched to names via assets/bulletin-polkadot-identities.json:
    //   Faraday Nodes  -> 155wHcqJ3fcfgtHsjqKHwNEU24pzRkkmZK865xxHeFTXMU8T  (aura: 0x4e91cfd5145fea6ebd1d1a441b33797e9c19918fa017291c73e56d2590566778)
    //   yaron          -> 1sXuddoUew7f9F9XTVyns8KjCRRLvpvvUsZUyxZhqtH4RZn  (aura: 0x80d6667f725e501088c081ff924dbe1aa50c67618b0984cb996c3d5fa5500f0f)
    //   DPSTK|dapestake-> 1A1WrKowzJD4yQQcETugEV5UWoNo1o7ujuA3f1fBfpxPjZL  (aura: 0x5e9659d151a03a5902e3135c9e361855f6d1caaea6e53a7d8613d7ad410bf507)
    ChainCfg { name: "Polkadot Bulletin",    rpcs: &["ws://192.168.250.241:9944", "ws://192.168.250.242:9944"],                         ss58: 0, session_is_sr25519: true  },
    // Kusama
    ChainCfg { name: "Kusama Asset Hub",     rpcs: &["ws://192.168.250.176:9944", "ws://192.168.250.221:9944"], ss58: 2, session_is_sr25519: true  },
    ChainCfg { name: "Kusama Bridge Hub",    rpcs: &["ws://192.168.250.178:9944", "ws://192.168.250.222:9944"], ss58: 2, session_is_sr25519: true  },
    ChainCfg { name: "Kusama Coretime",      rpcs: &["ws://192.168.250.211:9944", "ws://192.168.250.223:9944"], ss58: 2, session_is_sr25519: true  },
    ChainCfg { name: "Kusama People",        rpcs: &["ws://192.168.250.215:9944", "ws://192.168.250.224:9944"], ss58: 2, session_is_sr25519: true  },
    ChainCfg { name: "Kusama Encointer",     rpcs: &["ws://192.168.250.218:9944", "ws://192.168.250.225:9944"], ss58: 2, session_is_sr25519: true  },
];

// ---------------- knobs ----------------
// Keep these in sync with COLLATOR_REWARD_USD / BULLETIN_COLLATOR_REWARD_USD in the
// TypeScript payout script (index.ts). The TS script recomputes the payout from
// pct_top itself; the CSV's collator_reward_usd column is informational.
const COLLATOR_REWARD_USD: f64 = 250.0;
const BULLETIN_COLLATOR_REWARD_USD: f64 = 500.0;

fn is_bulletin(chain: &ChainCfg) -> bool {
    chain.name == "Polkadot Bulletin"
}

/// Maximum USD reward per collator for this chain (paid in full to the top producer)
fn reward_cap_usd(chain: &ChainCfg) -> f64 {
    if is_bulletin(chain) { BULLETIN_COLLATOR_REWARD_USD } else { COLLATOR_REWARD_USD }
}

const CHUNK_SIZE: usize = 10_000;
const CONCURRENCY: usize = 32;
const CHUNK_CONCURRENCY: usize = 20;
const CALL_TIMEOUT_SECS: u64 = 20;
// Max time for one block's set of reads on one endpoint before trying the next endpoint
const ATTEMPT_TIMEOUT_SECS: u64 = 60;
// Max time to open a connection to one endpoint at startup
const CONNECT_TIMEOUT_SECS: u64 = 15;

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
    // Bulletin chain uses Polkadot SS58 prefix (0) but its own identity file,
    // since it has no on-chain identity pallet — names are recorded manually.
    bulletin: HashMap<String, String>,
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

        // Polkadot Bulletin chain
        // These identities are manually recorded — the Bulletin chain has no on-chain
        // identity pallet.  Addresses are Polkadot SS58 (prefix 0) as shared by the
        // collators themselves; they are stored here in generic 42-prefix form for
        // consistent lookup.
        match fs::read_to_string("assets/bulletin-polkadot-identities.json") {
            Ok(s) => {
                let list: Vec<IdentityJson> = serde_json::from_str(&s)
                    .context("parse assets/bulletin-polkadot-identities.json")?;
                for e in list {
                    let display = if e.sub.trim().is_empty() {
                        e.name.clone()
                    } else {
                        format!("{}/{}", e.name, e.sub)
                    };
                    m.bulletin.insert(e.address, display);
                }
                eprintln!(
                    "Loaded {} entries from assets/bulletin-polkadot-identities.json",
                    m.bulletin.len()
                );
            }
            Err(e) => {
                eprintln!("WARN: cannot read assets/bulletin-polkadot-identities.json: {e}");
            }
        }

        Ok(m)
    }

    fn lookup(&self, chain: &ChainCfg, owner_raw: [u8; 32]) -> Option<String> {
        // Convert chain-specific AccountId32 (raw) to generic 42-prefix Substrate SS58
        let generic_fmt = Ss58AddressFormat::custom(42); // 42 = generic Substrate prefix
        let generic_ss58 =
            sr25519::Public::from_raw(owner_raw).to_ss58check_with_version(generic_fmt);

        let (which, map) = if chain.name == "Polkadot Bulletin" {
            ("Bulletin", &self.bulletin)
        } else if chain.ss58 == 0 {
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

/// Save collator data to CSV in the layout the TypeScript payout script reads:
///   ../SystemCollatorCSVFiles/{YYYY-MM}/{polkadot|kusama}/{chain_name}.csv
/// Bulletin goes in the `polkadot` folder as `polkadot_bulletin.csv`; the TS script
/// recognises it by "bulletin" in the file name and applies the $500 rate.
///
/// Columns: address,identity,blocks,pct_total,pct_top,collator_reward_usd,skip_reason
fn save_to_csv(
    rows: &[Row],
    chain: &ChainCfg,
    year: i32,
    month: u8,
    max_count: usize,
    no_reward_set: &HashSet<&str>,
) -> Result<()> {
    // Relay chain folder (Bulletin is a Polkadot chain and is paid from the Polkadot bounty)
    let relay_chain = if chain.ss58 == 0 { "polkadot" } else { "kusama" };

    let folder_name = format!("{:04}-{:02}", year, month);
    let output_dir = PathBuf::from("..")
        .join("SystemCollatorCSVFiles")
        .join(&folder_name)
        .join(relay_chain);

    std::fs::create_dir_all(&output_dir)?;

    // Filename: sanitized chain name, e.g. polkadot_asset_hub.csv, polkadot_bulletin.csv
    let chain_name_sanitized = chain.name.replace(' ', "_").to_lowercase();
    let csv_path = output_dir.join(format!("{}.csv", chain_name_sanitized));

    let cap = reward_cap_usd(chain);

    let mut csv = String::new();
    csv.push_str("address,identity,blocks,pct_total,pct_top,collator_reward_usd,skip_reason\n");

    for row in rows {
        if row.author_ss58 == "UNKNOWN" {
            continue;
        }

        let pct_top = if max_count > 0 {
            (row.blocks as f64) * 100.0 / (max_count as f64)
        } else { 0.0 };

        // Informational only - the TS script computes the actual payout
        let collator_reward_usd = cap * (pct_top / 100.0);

        let identity = if row.identity.is_empty() {
            String::new()
        } else {
            format!("\"{}\"", row.identity.replace('"', "\"\""))
        };

        let skip_reason = if no_reward_set.contains(row.author_ss58.as_str()) {
            "no_reward_list"
        } else {
            ""
        };

        csv.push_str(&format!(
            "{},{},{},{:.4},{:.4},{:.2},{}\n",
            row.author_ss58,
            identity,
            row.blocks,
            row.pct_total,
            pct_top,
            collator_reward_usd,
            skip_reason
        ));
    }

    std::fs::write(&csv_path, csv)?;
    println!("✅ CSV saved: {}", csv_path.display());

    Ok(())
}

// ---------------- RPC pool with failover ----------------

/// One connected endpoint: a subxt client (typed storage) + a raw JSON-RPC client.
#[derive(Clone)]
struct Endpoint {
    url: &'static str,
    api: OnlineClient<PolkadotConfig>,
    rpc: Arc<jsonrpsee::ws_client::WsClient>,
}

/// All endpoints for one chain. Calls go to the preferred endpoint; if a call fails or
/// times out it is retried on the next one, and that one becomes preferred.
struct RpcPool {
    chain_name: &'static str,
    eps: Vec<Endpoint>,
    preferred: AtomicUsize,
}

impl RpcPool {
    /// Connect to every endpoint that answers. Endpoints on a different chain
    /// (genesis hash mismatch) are dropped. At least one must connect.
    async fn connect(chain: &ChainCfg) -> Result<Self> {
        let mut eps = Vec::new();
        let mut genesis: Option<H256> = None;

        for &url in chain.rpcs {
            let attempt = async {
                let api = OnlineClient::<PolkadotConfig>::from_insecure_url(url)
                    .await
                    .with_context(|| format!("connect subxt to {url}"))?;
                let rpc = Arc::new(
                    WsClientBuilder::default()
                        .build(url)
                        .await
                        .with_context(|| format!("connect rpc ws to {url}"))?,
                );
                let g = block_hash_by_number(&rpc, 0).await?;
                Ok::<_, anyhow::Error>((Endpoint { url, api, rpc }, g))
            };

            match tokio::time::timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS), attempt).await {
                Ok(Ok((ep, g))) => {
                    match genesis {
                        None => genesis = Some(g),
                        Some(expected) if expected != g => {
                            eprintln!("WARN: {url} is on a different chain (genesis {g:?}, expected {expected:?}) - not used");
                            continue;
                        }
                        _ => {}
                    }
                    println!("   ✓ connected: {url}");
                    eps.push(ep);
                }
                Ok(Err(e)) => eprintln!("WARN: cannot connect to {url}: {e:#}"),
                Err(_) => eprintln!("WARN: timeout connecting to {url}"),
            }
        }

        if eps.is_empty() {
            bail!("No RPC endpoint reachable for {} (tried: {})", chain.name, chain.rpcs.join(", "));
        }
        Ok(RpcPool { chain_name: chain.name, eps, preferred: AtomicUsize::new(0) })
    }

    fn urls(&self) -> String {
        self.eps.iter().map(|e| e.url).collect::<Vec<_>>().join(", ")
    }

    /// Run `f` on the preferred endpoint; on error/timeout retry on the others.
    async fn run<T, F, Fut>(&self, what: &str, f: F) -> Result<T>
    where
        F: Fn(Endpoint) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let n = self.eps.len();
        let start = self.preferred.load(AtomicOrdering::Relaxed) % n;
        let mut last_err: Option<anyhow::Error> = None;

        for i in 0..n {
            let idx = (start + i) % n;
            let ep = self.eps[idx].clone();
            let url = ep.url;
            match tokio::time::timeout(Duration::from_secs(ATTEMPT_TIMEOUT_SECS), f(ep)).await {
                Ok(Ok(v)) => {
                    if idx != start
                        && self.preferred
                        .compare_exchange(start, idx, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed)
                        .is_ok()
                    {
                        eprintln!("  [{}] switched to {} after failure on {}", self.chain_name, url, self.eps[start].url);
                    }
                    return Ok(v);
                }
                Ok(Err(e)) => last_err = Some(e.context(format!("{what} on {url}"))),
                Err(_) => last_err = Some(anyhow!("{what} on {url}: timeout after {ATTEMPT_TIMEOUT_SECS}s")),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("{what}: no endpoints")))
    }
}

/// Everything read for one block. Built in full before any stats are updated, so a
/// failed attempt that is retried on another endpoint can never be counted twice.
struct BlockInfo {
    owner: Option<[u8; 32]>, // None = author could not be resolved
    invulnerables: HashSet<[u8; 32]>,
}

async fn read_block(ep: &Endpoint, n: u32, chain: ChainCfg) -> Result<BlockInfo> {
    let h = block_hash_by_number(&ep.rpc, n).await?;

    // aura session key (slot % authorities) and the invulnerable set at THIS block
    let (session_key_opt, invulnerables) = tokio::try_join!(
        derive_session_key_typed(&ep.api, h, chain),
        fetch_invulnerables_typed(&ep.api, h, chain),
    )?;

    // owner via Session::KeyOwner((KeyTypeId("aura"), key_bytes))
    let owner = match session_key_opt {
        Some(k) => session_key_owner_account_typed(&ep.api, h, chain, k).await?,
        None => None,
    };

    Ok(BlockInfo { owner, invulnerables })
}

async fn timestamp_at(pool: &RpcPool, n: u32, chain: ChainCfg) -> Result<u64> {
    pool.run(&format!("timestamp #{n}"), |ep| async move {
        let h = block_hash_by_number(&ep.rpc, n).await?;
        Ok::<_, anyhow::Error>(block_timestamp_typed(&ep.api, h, chain).await?.unwrap_or(0))
    })
        .await
}

// ---------------- main ----------------
#[tokio::main]
async fn main() -> Result<()> {
    let chain = prompt_chain()?;
    let Inputs { year, month } = prompt_inputs()?;

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
        "==> Chain: {}  |  RPCs: {}\n==> Window: [{} .. {})  |  Reward cap: ${:.2}",
        chain.name, chain.rpcs.join(", "), start_dt.to_rfc3339(), end_dt.to_rfc3339(), reward_cap_usd(&chain)
    );

    // connections (all endpoints for this chain; failover between them)
    println!("==> Connecting…");
    let pool = Arc::new(RpcPool::connect(&chain).await?);
    if pool.eps.len() < chain.rpcs.len() {
        eprintln!("WARN: running with {} of {} endpoints (no failover if it drops)", pool.eps.len(), chain.rpcs.len());
    }

    // latest
    let (latest_num, latest_hash) = pool
        .run("latest block", |ep| async move {
            let b = ep.api.blocks().at_latest().await?;
            Ok::<_, anyhow::Error>((b.number(), b.hash()))
        })
        .await?;
    let latest_ts = pool
        .run("latest timestamp", |ep| async move {
            Ok::<_, anyhow::Error>(block_timestamp_typed(&ep.api, latest_hash, chain).await?.unwrap_or(0))
        })
        .await?;
    println!("==> Latest: #{} ts={}", latest_num, fmt_ts(latest_ts));
    if latest_ts < start_ms {
        bail!("Latest {} is before window start {}.", fmt_ts(latest_ts), fmt_ts(start_ms));
    }

    // bounds via binary search
    println!("==> Locating first block ≥ {} (binary search)…", fmt_ts(start_ms));
    let first_num = bin_search_first_ge(&pool, 0, latest_num, start_ms, chain).await?;
    println!("   -> first in window: #{}", first_num);

    println!("==> Locating last block < {} (binary search)…", fmt_ts(end_ms));
    let ub = bin_search_first_ge(&pool, first_num, latest_num, end_ms, chain).await?;
    let last_num = ub.saturating_sub(1);
    println!("   -> last in window:  #{}", last_num);
    if last_num < first_num { bail!("Empty window: last({last_num}) < first({first_num})."); }

    // Invulnerables can change during the month (added/removed at any block), so the set is
    // read at EVERY block. A block counts only if its author was invulnerable at that block,
    // and every account that was invulnerable at any point is collected here with the first
    // and last block it was seen, so the report/CSV shows all of them for the whole month.
    let inv_seen: Arc<Mutex<HashMap<[u8; 32], (u32, u32)>>> = Arc::new(Mutex::new(HashMap::new()));
    println!("==> Invulnerables are read per block (set may change during the window)");

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
    let skipped_non_invulnerable = Arc::new(Mutex::new(0usize));

    // run
    futures::stream::iter(
        chunk_specs
            .into_iter()
            .map(|(c_start, c_end, chunk_pb)| {
                let pool = pool.clone();
                let stats = stats.clone();
                let overall_pb = overall_pb.clone();
                let pb_for_tasks = chunk_pb.clone();
                let unknowns = unknowns.clone();
                let block_errors = block_errors.clone();
                let inv_seen = inv_seen.clone();
                let skipped_non_invulnerable = skipped_non_invulnerable.clone();
                let chain = chain;

                async move {
                    let numbers: Vec<u32> = (c_start..=c_end).collect();

                    futures::stream::iter(numbers.into_iter().map(move |n| {
                        let pool = pool.clone();
                        let stats = stats.clone();
                        let chunk_pb = pb_for_tasks.clone();
                        let overall_pb = overall_pb.clone();
                        let unknowns = unknowns.clone();
                        let block_errors = block_errors.clone();
                        let inv_seen = inv_seen.clone();
                        let skipped_non_invulnerable = skipped_non_invulnerable.clone();
                        let chain = chain;

                        async move {
                            // All reads for this block, with failover between endpoints.
                            // Stats are only touched after a fully successful read.
                            match pool
                                .run(&format!("block #{n}"), |ep| async move { read_block(&ep, n, chain).await })
                                .await
                            {
                                Ok(info) => {
                                    // record every invulnerable seen, with first/last block
                                    {
                                        let mut seen = inv_seen.lock().await;
                                        for raw in info.invulnerables.iter() {
                                            let e = seen.entry(*raw).or_insert((n, n));
                                            if n < e.0 { e.0 = n; }
                                            if n > e.1 { e.1 = n; }
                                        }
                                    }

                                    match info.owner {
                                        // only count blocks authored by a collator that was
                                        // invulnerable at this block
                                        Some(owner_raw) if info.invulnerables.contains(&owner_raw) => {
                                            let mut sm = stats.lock().await;
                                            *sm.entry(owner_raw).or_insert(0) += 1;
                                        }
                                        Some(_) => {
                                            let mut sk = skipped_non_invulnerable.lock().await;
                                            *sk += 1;
                                        }
                                        None => {
                                            let mut u = unknowns.lock().await;
                                            *u += 1;
                                            let mut sm = stats.lock().await;
                                            *sm.entry([0u8; 32]).or_insert(0) += 1;
                                        }
                                    }
                                }
                                Err(e) => {
                                    let mut be = block_errors.lock().await;
                                    *be += 1;
                                    eprintln!("  [block #{n} error - all endpoints failed] {e:#}");
                                }
                            }

                            chunk_pb.inc(1);
                            overall_pb.inc(1);
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
    let skipped_non_invulnerable = Arc::try_unwrap(skipped_non_invulnerable).unwrap().into_inner();
    let total_scanned: usize = stats.values().copied().sum();

    // Build NO_REWARD set for quick membership test
    let no_reward_set: HashSet<&'static str> = NO_REWARD_COLLATORS.iter().copied().collect();

    let inv_seen = Arc::try_unwrap(inv_seen).unwrap().into_inner();
    if inv_seen.is_empty() {
        bail!("CollatorSelection::Invulnerables was empty/unset on {} for the whole window.", chain.name);
    }

    // Every invulnerable from the month gets a row, even with 0 blocks
    let mut stats = stats;
    for raw in inv_seen.keys() {
        stats.entry(*raw).or_insert(0);
    }

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

    // Top producer among real collators (UNKNOWN must not set the 100% mark)
    let max_count = rows
        .iter()
        .filter(|r| r.author_ss58 != "UNKNOWN")
        .map(|r| r.blocks)
        .max()
        .unwrap_or(0);
    let cap = reward_cap_usd(&chain);

    println!("\n================ SUMMARY (full scan) ================");
    println!("Chain:      {}", chain.name);
    println!("Chain RPCs: {}", pool.urls());
    println!("Window:     [{} .. {})", start_dt.to_rfc3339(), end_dt.to_rfc3339());
    println!("Blocks counted (invulnerables + unresolved): {}", total_scanned);
    println!("Blocks skipped (non-invulnerable authors):   {}", skipped_non_invulnerable);
    println!(
        "{:<6}  {:<48}  {:<28}  {:>8}  {:>7}  {:>7}  {:>9}",
        "Rank", "Author (Owner SS58)", "Identity", "Blocks", "%", "%Top", "Payout $"
    );
    println!("{}", "-".repeat(140));

    for (i, row) in rows.iter().enumerate() {
        let pct_top = if max_count > 0 {
            (row.blocks as f64) * 100.0 / (max_count as f64)
        } else { 0.0 };
        let payout = if row.author_ss58 == "UNKNOWN" { 0.0 } else { cap * (pct_top / 100.0) };
        let id_display = if row.identity.is_empty() { "-".to_string() } else { row.identity.clone() };

        println!(
            "{:<6}  {:<48}  {:<28}  {:>8}  {:>7.2}  {:>7.2}  {:>9.2}",
            i + 1,
            row.author_ss58,
            id_display,
            row.blocks,
            row.pct_total,
            pct_top,
            payout
        );
    }
    println!("{}", "-".repeat(140));
    println!("Note: '%' is share of counted (invulnerable) blocks in window; '%Top' is relative to the top producer.");
    println!("Reward cap for '%Top' payout: ${:.2} (paid in USDT on Polkadot, converted to KSM by the payout script on Kusama)", cap);

    // All invulnerables seen during the window, with the block range they were seen in
    let mut inv_list: Vec<(&[u8; 32], &(u32, u32))> = inv_seen.iter().collect();
    inv_list.sort_by_key(|(_, (first, _))| *first);
    println!("\nInvulnerables during the window: {}", inv_list.len());
    for (raw, (first, last)) in inv_list {
        let ss58 = ss58_from_raw32_with_prefix(*raw, chain.ss58);
        let id = identity_maps.lookup(&chain, *raw).unwrap_or_else(|| "-".to_string());
        let note = if *first == first_num && *last == last_num { "whole window" } else { "changed during window" };
        println!("   - {:<48}  {:<28}  #{} .. #{}  ({})", ss58, id, first, last, note);
    }

    // diagnostics
    let unknowns = Arc::try_unwrap(unknowns).unwrap().into_inner();
    let block_errors = Arc::try_unwrap(block_errors).unwrap().into_inner();
    eprintln!(
        "Diagnostics: unknown-authors={}, block-errors={}, non-invulnerable-blocks-skipped={}",
        unknowns, block_errors, skipped_non_invulnerable
    );

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
            &no_reward_set,
        )?;
    }

    println!("\n==> Done.");
    Ok(())
}

// ---------------- interactive ----------------
// Only the chain, year and month are needed. No EMA / staking rate: payouts are in USD
// (USDT on Polkadot); the TS payout script asks for the KSM price when paying Kusama.
struct Inputs {
    year: i32,
    month: u8,
}

fn prompt_chain() -> Result<ChainCfg> {
    loop {
        println!("Select chain:");
        println!("  1) Polkadot  Asset Hub");
        println!("  2) Polkadot  Bridge Hub");
        println!("  3) Polkadot  Coretime");
        println!("  4) Polkadot  Collectives");
        println!("  5) Polkadot  People");
        println!("  6) Polkadot  Bulletin");
        println!("  7) Kusama    Asset Hub");
        println!("  8) Kusama    Bridge Hub");
        println!("  9) Kusama    Coretime");
        println!(" 10) Kusama    People");
        println!(" 11) Kusama    Encointer");
        print!("Enter selection (1-11): ");
        io::stdout().flush().ok();
        let mut s = String::new();
        io::stdin().read_line(&mut s)?;
        match s.trim() {
            "1"  => return Ok(CHAINS[0]),
            "2"  => return Ok(CHAINS[1]),
            "3"  => return Ok(CHAINS[2]),
            "4"  => return Ok(CHAINS[3]),
            "5"  => return Ok(CHAINS[4]),
            "6"  => return Ok(CHAINS[5]),
            "7"  => return Ok(CHAINS[6]),
            "8"  => return Ok(CHAINS[7]),
            "9"  => return Ok(CHAINS[8]),
            "10" => return Ok(CHAINS[9]),
            "11" => return Ok(CHAINS[10]),
            _ => eprintln!("  -> Please enter 1..11."),
        }
    }
}

fn prompt_inputs() -> Result<Inputs> {
    // Same order as the TypeScript payout script: year, then month
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
    Ok(Inputs { year, month })
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
        "Polkadot Bulletin"    => api.storage().at(at).fetch(&bulletin_polkadot::storage().timestamp().now()).await?,
        "Kusama Asset Hub"     => api.storage().at(at).fetch(&ahp_kusama::storage().timestamp().now()).await?,
        "Kusama Bridge Hub"    => api.storage().at(at).fetch(&bridgehub_kusama::storage().timestamp().now()).await?,
        "Kusama Coretime"      => api.storage().at(at).fetch(&coretime_kusama::storage().timestamp().now()).await?,
        "Kusama People"        => api.storage().at(at).fetch(&people_kusama::storage().timestamp().now()).await?,
        "Kusama Encointer"     => api.storage().at(at).fetch(&encointer_kusama::storage().timestamp().now()).await?,
        _ => None,
    })
}

async fn bin_search_first_ge(
    pool: &RpcPool,
    mut lo: u32,
    mut hi: u32,
    target_ms: u64,
    chain: ChainCfg,
) -> Result<u32> {
    let lo_ts = timestamp_at(pool, lo, chain).await?;
    let hi_ts = timestamp_at(pool, hi, chain).await?;

    if hi_ts < target_ms {
        bail!("bin_search_first_ge: hi(#{} ts={}) < target {}", hi, fmt_ts(hi_ts), fmt_ts(target_ms));
    }
    if lo_ts >= target_ms { return Ok(lo); }

    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_ts = timestamp_at(pool, mid, chain).await?;
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
        "Polkadot Bulletin" => {
            let slot: Option<bulletin_polkadot::runtime_types::sp_consensus_slots::Slot> =
                api.storage().at(at).fetch(&bulletin_polkadot::storage().aura().current_slot()).await?;
            let auths: Option<
                bulletin_polkadot::runtime_types::bounded_collections::bounded_vec::BoundedVec<
                    bulletin_polkadot::runtime_types::sp_consensus_aura::sr25519::app_sr25519::Public
                >
            > = api.storage().at(at).fetch(&bulletin_polkadot::storage().aura().authorities()).await?;
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

/// Fetch CollatorSelection::Invulnerables at `at` as a set of raw AccountId32s.
/// The value is SCALE-encoded and re-decoded as Vec<[u8;32]> so this works regardless of
/// whether a chain's metadata types it as Vec or BoundedVec.
async fn fetch_invulnerables_typed(
    api: &OnlineClient<PolkadotConfig>,
    at: H256,
    chain: ChainCfg,
) -> Result<HashSet<[u8; 32]>> {
    // Mirrors how `authorities()` is unwrapped elsewhere in this file (`let v = bv.0;`):
    // pull the inner Vec<T> straight out of the generated BoundedVec rather than
    // re-encoding/decoding the whole collection (which trips over duplicate
    // parity-scale-codec versions between this crate and subxt's bundled copy).
    macro_rules! fetch_inv {
        ($m:ident) => {{
            let addr = $m::storage().collator_selection().invulnerables();
            let v = api.storage().at(at).fetch(&addr).await?;
            v.map(|bv| bv.0.into_iter().map(account_to_raw32).collect::<Vec<[u8; 32]>>())
        }};
    }

    let accounts: Option<Vec<[u8; 32]>> = match chain.name {
        // Polkadot
        "Polkadot Asset Hub" => fetch_inv!(ahp_polkadot),
        "Polkadot Bridge Hub" => fetch_inv!(bridgehub_polkadot),
        "Polkadot Coretime" => fetch_inv!(coretime_polkadot),
        "Polkadot Collectives" => fetch_inv!(collectives_polkadot),
        "Polkadot People" => fetch_inv!(people_polkadot),
        "Polkadot Bulletin" => fetch_inv!(bulletin_polkadot),
        // Kusama
        "Kusama Asset Hub" => fetch_inv!(ahp_kusama),
        "Kusama Bridge Hub" => fetch_inv!(bridgehub_kusama),
        "Kusama Coretime" => fetch_inv!(coretime_kusama),
        "Kusama People" => fetch_inv!(people_kusama),
        "Kusama Encointer" => fetch_inv!(encointer_kusama),
        _ => None,
    };

    Ok(accounts.unwrap_or_default().into_iter().collect())
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
        "Polkadot Bulletin" => {
            let kt = bulletin_polkadot::runtime_types::sp_core::crypto::KeyTypeId(aura);
            let call = bulletin_polkadot::storage().session().key_owner((kt, session_key_raw32.to_vec()));
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
fn account_to_raw32<T: subxt::ext::codec::Encode>(acc: T) -> [u8; 32] {
    let bytes = acc.encode();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes[..32]);
    out
}