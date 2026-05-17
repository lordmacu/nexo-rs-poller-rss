//! RSS / Atom feed poller — provider-agnostic, no credentials.
//!
//! Cursor encodes the highest-seen `<guid>` (or `<id>` for Atom)
//! plus the last `ETag` header. Server returns `304 Not Modified`
//! → tick reports zero items and keeps the cursor unchanged.
//!
//! Extracted from the in-tree `crates/poller/src/builtins/rss.rs`
//! during the Phase 96 Laravel-style poller refactor. The legacy
//! `OutboundDelivery` + channel enum coupling is gone; this crate
//! resolves the channel's account_id via
//! `PollerHost::credentials_get` and publishes directly to the
//! `plugin.outbound.<channel>.<account_id>` topic.

use std::collections::HashSet;

use async_trait::async_trait;
use reqwest::header::{ETAG, IF_NONE_MATCH};
use serde::{Deserialize, Serialize};
use serde_json::json;

use nexo_microapp_sdk::poller::{PollerHandler, TickRequest};
use nexo_poller::{PollerError, PollerHost, TickAck, TickMetrics};

/// Per-job config shape — operator-facing YAML in `pollers.yaml`.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RssJobConfig {
    pub feed_url: String,
    #[serde(default = "default_max")]
    pub max_per_tick: usize,
    /// Mustache-light template. Fields: `{title}`, `{link}`, `{summary}`.
    #[serde(default = "default_template")]
    pub message_template: String,
    pub deliver: DeliverCfg,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DeliverCfg {
    pub channel: String,
    #[serde(alias = "recipient")]
    pub to: String,
}

fn default_max() -> usize {
    5
}
fn default_template() -> String {
    "{title}\n{link}".to_string()
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct CursorState {
    seen_ids: Vec<String>,
    etag: Option<String>,
}

const SEEN_CAP: usize = 200;

pub struct RssHandler {
    http: reqwest::Client,
}

impl RssHandler {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest builder"),
        }
    }
}

impl Default for RssHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PollerHandler for RssHandler {
    async fn tick(
        &self,
        req: TickRequest,
        host: std::sync::Arc<dyn PollerHost>,
    ) -> Result<TickAck, PollerError> {
        let cfg: RssJobConfig =
            serde_json::from_value(req.config.clone()).map_err(|e| PollerError::Config {
                job: req.job_id.clone(),
                reason: e.to_string(),
            })?;
        let cursor_bytes = req.cursor_bytes()?;
        let mut state: CursorState = cursor_bytes
            .as_deref()
            .and_then(|b| serde_json::from_slice(b).ok())
            .unwrap_or_default();

        let mut req_builder = self.http.get(&cfg.feed_url);
        if let Some(tag) = state.etag.as_deref() {
            req_builder = req_builder.header(IF_NONE_MATCH, tag);
        }
        let resp = req_builder
            .send()
            .await
            .map_err(|e| PollerError::Transient(anyhow::Error::from(e)))?;

        if resp.status().as_u16() == 304 {
            return Ok(TickAck {
                next_cursor: None,
                next_interval_hint: None,
                metrics: Some(TickMetrics::default()),
            });
        }
        if !resp.status().is_success() {
            let status = resp.status();
            if status.is_client_error() {
                return Err(PollerError::Permanent(anyhow::anyhow!(
                    "HTTP {status} — feed config likely wrong"
                )));
            }
            return Err(PollerError::Transient(anyhow::anyhow!("HTTP {status}")));
        }

        let new_etag = resp
            .headers()
            .get(ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let body = resp
            .text()
            .await
            .map_err(|e| PollerError::Transient(anyhow::Error::from(e)))?;

        let items = parse_feed(&body);
        let known: HashSet<String> = state.seen_ids.iter().cloned().collect();

        // Resolve target topic via reverse-RPC.
        let cred = host
            .credentials_get(cfg.deliver.channel.clone())
            .await
            .map_err(|e| PollerError::Permanent(anyhow::anyhow!("credentials_get: {e}")))?;
        let account_id = cred
            .get("account_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                PollerError::Permanent(anyhow::anyhow!(
                    "credentials_get('{}') returned no `account_id`",
                    cfg.deliver.channel
                ))
            })?
            .to_string();
        let topic = format!("plugin.outbound.{}.{}", cfg.deliver.channel, account_id);

        let mut new_ids = Vec::new();
        let mut items_seen = 0u32;
        let mut items_dispatched = 0u32;
        for item in items.iter().take(cfg.max_per_tick) {
            items_seen += 1;
            if known.contains(&item.id) {
                continue;
            }
            let text = render_template(&cfg.message_template, item);
            let payload = json!({ "to": cfg.deliver.to, "text": text });
            let payload_bytes = serde_json::to_vec(&payload)
                .map_err(|e| PollerError::Transient(anyhow::Error::from(e)))?;
            host.broker_publish(topic.clone(), payload_bytes)
                .await
                .map_err(|e| PollerError::Transient(anyhow::anyhow!("broker_publish: {e}")))?;
            items_dispatched += 1;
            new_ids.push(item.id.clone());
        }

        state.seen_ids.extend(new_ids);
        if state.seen_ids.len() > SEEN_CAP {
            let drop = state.seen_ids.len() - SEEN_CAP;
            state.seen_ids.drain(0..drop);
        }
        state.etag = new_etag;
        let cursor = serde_json::to_vec(&state).ok();
        Ok(TickAck {
            next_cursor: cursor,
            next_interval_hint: None,
            metrics: Some(TickMetrics {
                items_seen,
                items_dispatched,
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedItem {
    pub id: String,
    pub title: String,
    pub link: String,
    pub summary: String,
}

pub fn parse_feed(body: &str) -> Vec<FeedItem> {
    // Minimal RSS 2.0 + Atom parser. Production-grade parsers are
    // overkill for a poller subprocess that walks feeds once per
    // interval and only needs the dedup id + display fields.
    let trimmed = body.trim_start_matches('\u{FEFF}');
    let mut out = Vec::new();
    let mut tokens = trimmed.split("<item").skip(1);
    while let Some(chunk) = tokens.next() {
        let item_block = chunk.split("</item>").next().unwrap_or("");
        let id = extract_tag(item_block, "guid")
            .or_else(|| extract_tag(item_block, "link"))
            .unwrap_or_default();
        let title = extract_tag(item_block, "title").unwrap_or_default();
        let link = extract_tag(item_block, "link").unwrap_or_default();
        let summary = extract_tag(item_block, "description").unwrap_or_default();
        if !id.is_empty() {
            out.push(FeedItem {
                id,
                title,
                link,
                summary,
            });
        }
    }
    if out.is_empty() {
        // Try Atom: <entry>...<id>...</id><title>...</title><link href="..."/></entry>
        let mut atom_tokens = trimmed.split("<entry").skip(1);
        while let Some(chunk) = atom_tokens.next() {
            let entry_block = chunk.split("</entry>").next().unwrap_or("");
            let id = extract_tag(entry_block, "id").unwrap_or_default();
            let title = extract_tag(entry_block, "title").unwrap_or_default();
            let link = extract_atom_link(entry_block).unwrap_or_default();
            let summary = extract_tag(entry_block, "summary").unwrap_or_default();
            if !id.is_empty() {
                out.push(FeedItem {
                    id,
                    title,
                    link,
                    summary,
                });
            }
        }
    }
    out
}

fn extract_tag(block: &str, tag: &str) -> Option<String> {
    let open_marker = format!("<{tag}");
    let close_marker = format!("</{tag}>");
    let open_pos = block.find(&open_marker)?;
    // Skip past either "<tag>" or "<tag attr=...>" — we only need
    // the position of the first '>' AFTER our open marker.
    let after_marker = &block[open_pos + open_marker.len()..];
    let gt_offset = after_marker.find('>')?;
    let content_start = open_pos + open_marker.len() + gt_offset + 1;
    let rest = &block[content_start..];
    let close_pos = rest.find(&close_marker)?;
    Some(strip_cdata(rest[..close_pos].trim()).to_string())
}

fn extract_atom_link(block: &str) -> Option<String> {
    let after = block.split("<link").nth(1)?;
    let attrs = after.split('>').next()?;
    let href_pos = attrs.find("href=")?;
    let rest = &attrs[href_pos + 5..];
    let quote = rest.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = 1;
    let value_end = rest[value_start..].find(quote)? + value_start;
    Some(rest[value_start..value_end].to_string())
}

fn strip_cdata(s: &str) -> &str {
    s.trim_start_matches("<![CDATA[").trim_end_matches("]]>")
}

fn render_template(template: &str, item: &FeedItem) -> String {
    template
        .replace("{title}", &item.title)
        .replace("{link}", &item.link)
        .replace("{summary}", &item.summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rss_2_0_extracts_items() {
        let body = r#"
            <rss version="2.0">
              <channel>
                <item>
                  <guid>id1</guid>
                  <title>First post</title>
                  <link>https://example.com/1</link>
                  <description>Body one</description>
                </item>
                <item>
                  <guid>id2</guid>
                  <title>Second post</title>
                  <link>https://example.com/2</link>
                  <description>Body two</description>
                </item>
              </channel>
            </rss>
        "#;
        let items = parse_feed(body);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].id, "id1");
        assert_eq!(items[1].title, "Second post");
    }

    #[test]
    fn parse_atom_extracts_entries() {
        let body = r#"
            <feed xmlns="http://www.w3.org/2005/Atom">
              <entry>
                <id>https://example.com/a</id>
                <title>Atom A</title>
                <link href="https://example.com/a" />
                <summary>summary a</summary>
              </entry>
              <entry>
                <id>https://example.com/b</id>
                <title>Atom B</title>
                <link href="https://example.com/b" />
                <summary>summary b</summary>
              </entry>
            </feed>
        "#;
        let items = parse_feed(body);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].link, "https://example.com/a");
        assert_eq!(items[1].id, "https://example.com/b");
    }

    #[test]
    fn render_template_substitutes_fields() {
        let item = FeedItem {
            id: "x".into(),
            title: "Hi".into(),
            link: "https://x".into(),
            summary: "s".into(),
        };
        assert_eq!(
            render_template("{title}: {link}", &item),
            "Hi: https://x"
        );
    }

    #[test]
    fn cursor_state_round_trips() {
        let s = CursorState {
            seen_ids: vec!["a".into(), "b".into()],
            etag: Some("W/\"xyz\"".into()),
        };
        let bytes = serde_json::to_vec(&s).unwrap();
        let back: CursorState = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.seen_ids, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(back.etag.as_deref(), Some("W/\"xyz\""));
    }

    #[test]
    fn config_accepts_recipient_alias() {
        let cfg: RssJobConfig = serde_json::from_value(json!({
            "feed_url": "https://example.com/feed.xml",
            "message_template": "x",
            "deliver": { "channel": "telegram", "recipient": "-100" },
        }))
        .unwrap();
        assert_eq!(cfg.deliver.to, "-100");
    }
}
