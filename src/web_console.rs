use crate::config::WebConsoleConfig;
use crate::server::{BrokerState, SharedBroker};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use std::io;
use std::sync::Arc;

#[derive(Clone)]
struct ConsoleState {
    broker: SharedBroker,
    admin_token: Option<Arc<str>>,
}

#[derive(Serialize)]
struct QueueSummary {
    name: String,
    depth: usize,
}

#[derive(Serialize)]
struct PurgeResult {
    removed: usize,
}

pub(crate) fn start(broker: SharedBroker, config: WebConsoleConfig) -> io::Result<()> {
    let address = config.validate()?;
    let listener = std::net::TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    let state = ConsoleState {
        broker,
        admin_token: config.admin_token.map(Arc::from),
    };
    std::thread::Builder::new()
        .name("activemq-web-console".to_owned())
        .spawn(move || {
            if let Err(error) = run_console(listener, state) {
                tracing::error!(%error, "web console stopped");
            }
        })?;
    Ok(())
}

fn run_console(listener: std::net::TcpListener, state: ConsoleState) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        tracing::info!(address = %listener.local_addr()?, "web console ready");
        let app = Router::new()
            .route("/", get(index))
            .route("/api/queues", get(list_queues))
            .route("/api/queues/{name}/purge", post(purge_queue))
            .with_state(state);
        axum::serve(listener, app).await.map_err(io::Error::other)
    })
}

async fn index() -> Html<&'static str> {
    Html(
        r##"<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>ActiveMQ Classic Rust Console</title>
<style>
body{font:16px system-ui,sans-serif;max-width:900px;margin:3rem auto;padding:0 1rem;color:#172033}
h1{font-size:1.6rem}button{cursor:pointer}table{border-collapse:collapse;width:100%}
th,td{text-align:left;padding:.65rem;border-bottom:1px solid #ccd2dc}
#status{min-height:1.5em;color:#9b1c1c}
</style></head>
<body><h1>ActiveMQ Classic Rust Console</h1><p id="status"></p>
<table><thead><tr><th>Queue</th><th>Messages</th><th>Actions</th></tr></thead>
<tbody id="queues"></tbody></table>
<script>
const token=prompt("Admin token (leave blank if not configured)")||"";
const headers=token?{"Authorization":"Bearer "+token}:{};
async function refresh(){
  const response=await fetch("/api/queues",{headers});
  if(!response.ok)throw new Error(response.status===401?"Unauthorized":"Queue request failed");
  const queues=await response.json(), body=document.querySelector("#queues");
  body.replaceChildren();
  for(const queue of queues){
    const row=document.createElement("tr"), name=document.createElement("td");
    const depth=document.createElement("td"), actions=document.createElement("td");
    name.textContent=queue.name; depth.textContent=queue.depth;
    const button=document.createElement("button"); button.textContent="Purge";
    button.onclick=async()=>{if(!confirm("Purge queue "+queue.name+"?"))return;
      const result=await fetch("/api/queues/"+encodeURIComponent(queue.name)+"/purge",{method:"POST",headers});
      if(!result.ok){document.querySelector("#status").textContent="Purge failed";return}
      document.querySelector("#status").textContent="Queue purged"; await refresh();};
    actions.append(button); row.append(name,depth,actions); body.append(row);
  }
}
refresh().catch(error=>document.querySelector("#status").textContent=error.message);
</script></body></html>"##,
    )
}

async fn list_queues(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
) -> Result<Json<Vec<QueueSummary>>, StatusCode> {
    authorize(&state, &headers)?;
    let broker = state
        .broker
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(queue_summaries(&broker)))
}

async fn purge_queue(
    State(state): State<ConsoleState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<PurgeResult>, StatusCode> {
    authorize(&state, &headers)?;
    let mut broker = state
        .broker
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let in_memory = broker.queues.remove(&name).map_or(0, |queue| queue.len());
    let removed = broker
        .store
        .purge_queue(&name)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(PurgeResult {
        removed: in_memory.max(removed),
    }))
}

fn queue_summaries(broker: &BrokerState) -> Vec<QueueSummary> {
    let mut queues = broker
        .queues
        .iter()
        .map(|(name, queue)| QueueSummary {
            name: name.clone(),
            depth: queue.len(),
        })
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| left.name.cmp(&right.name));
    queues
}

fn authorize(state: &ConsoleState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let Some(token) = &state.admin_token else {
        return Ok(());
    };
    let valid = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| constant_time_eq(provided.as_bytes(), token.as_bytes()));
    if valid {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_requires_exact_bytes() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"Secret"));
        assert!(!constant_time_eq(b"secret", b"secret-long"));
    }

    #[test]
    fn queue_summaries_are_sorted() {
        let store_path =
            std::env::temp_dir().join(format!("activemq-console-test-{}.sled", std::process::id()));
        let store = crate::sled_store::SledStore::open(&store_path).unwrap();
        let broker = BrokerState {
            queues: [
                ("zeta".to_owned(), Default::default()),
                ("alpha".to_owned(), Default::default()),
            ]
            .into(),
            store,
        };
        let summaries = queue_summaries(&broker);
        assert_eq!(summaries[0].name, "alpha");
        assert_eq!(summaries[1].name, "zeta");
        drop(broker);
        std::fs::remove_dir_all(store_path).unwrap();
    }
}
