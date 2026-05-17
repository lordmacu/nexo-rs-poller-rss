//! Subprocess entrypoint for the RSS poller plugin.
//!
//! Daemon spawns this binary; we boot the broker (Phase 92
//! `NEXO_BROKER_KIND` + `NEXO_BROKER_URL` stamping), subscribe to
//! the `plugin.poller.rss.tick` topic, dispatch every incoming
//! request to `serve_one_tick`, and reply on the message's own
//! `reply_to`.

use std::sync::Arc;

use nexo_broker::{AnyBroker, BrokerHandle};
use nexo_microapp_sdk::poller::{serve_one_tick, PollerHandler};
use nexo_poller_rss::RssHandler;
use serde_json::Value;

const MANIFEST: &str = include_str!("../nexo-plugin.toml");

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--print-manifest") {
        println!("{MANIFEST}");
        return Ok(());
    }

    let broker_url = std::env::var("NEXO_BROKER_URL").ok();
    let broker = match broker_url.as_deref() {
        Some(url) if !url.is_empty() => boot_broker(url).await?,
        _ => AnyBroker::local(),
    };
    tracing::info!(
        broker_url = broker_url.as_deref().unwrap_or("<local>"),
        "nexo-poller-rss: broker ready",
    );

    let handler: Arc<dyn PollerHandler> = Arc::new(RssHandler::new());
    let topic = "plugin.poller.rss.tick";
    let mut sub = broker.subscribe(topic).await?;
    tracing::info!(topic, "tick subscriber up");

    while let Some(event) = sub.next().await {
        let plugin_id = "rss";
        let broker_clone = broker.clone();
        let handler_clone = Arc::clone(&handler);

        // Unwrap the `Message` envelope serialized inside event.payload
        // (broker.request semantics).
        let msg: nexo_broker::Message = match serde_json::from_value(event.payload.clone()) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "tick envelope parse failed; dropping");
                continue;
            }
        };
        let reply_to = msg.reply_to.clone();
        let request_payload: Value = msg.payload;

        tokio::spawn(async move {
            if let Err(e) = serve_one_tick(
                plugin_id,
                broker_clone,
                handler_clone,
                request_payload,
                reply_to.as_deref(),
            )
            .await
            {
                tracing::warn!(error = %e, "serve_one_tick failed");
            }
        });
    }

    Ok(())
}

async fn boot_broker(broker_url: &str) -> anyhow::Result<AnyBroker> {
    let broker_inner = nexo_config::types::broker::BrokerInner {
        kind: if broker_url.starts_with("nats://") {
            nexo_config::types::broker::BrokerKind::Nats
        } else {
            nexo_config::types::broker::BrokerKind::Local
        },
        url: broker_url.to_string(),
        auth: nexo_config::types::broker::BrokerAuthConfig::default(),
        persistence: nexo_config::types::broker::BrokerPersistenceConfig::default(),
        limits: nexo_config::types::broker::BrokerLimitsConfig::default(),
        fallback: nexo_config::types::broker::BrokerFallbackConfig::default(),
    };
    AnyBroker::from_config(&broker_inner)
        .await
        .map_err(|e| anyhow::anyhow!("broker connect failed: {e}"))
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,nexo_poller_rss=debug"));
    fmt().with_env_filter(filter).with_target(false).init();
}
