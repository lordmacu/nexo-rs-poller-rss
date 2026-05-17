# nexo-poller-rss

> RSS / Atom feed poller plugin for [Nexo](https://github.com/lordmacu/nexo-rs) agents (out-of-tree subprocess).

Extracted from the in-tree `nexo-poller::builtins::rss` during the
Phase 96 Laravel-style poller refactor. The runtime in
`crates/poller/` (daemon-side) is provider-agnostic; this plugin
owns the RSS / Atom parsing + outbound routing.

## Install

```bash
cargo install nexo-poller-rss
```

The daemon auto-discovers the binary via its `[plugin.entrypoint]`
manifest section + spawns one subprocess at boot. The
`[plugin.poller]` section declares the `rss` kind so any
`pollers.yaml` job with `kind: rss` routes through this plugin.

## Operator YAML

```yaml
# pollers.yaml fragment
jobs:
  - id: hn_top
    kind: rss
    agent: cody
    schedule: { every: 15m }
    config:
      feed_url: "https://hnrss.org/frontpage"
      max_per_tick: 5
      message_template: "{title}\n{link}"
      deliver:
        channel: whatsapp
        to: "+573001234567"
```

## What it does

- Fetches the feed via HTTPS with `ETag` / `If-None-Match`
  conditional GETs — `304 Not Modified` reports zero items and
  leaves the cursor unchanged.
- Dedups items via `<guid>` (RSS) or `<id>` (Atom). Bounded ring
  of last 200 ids per job persisted as the cursor.
- Resolves the outbound channel's `account_id` via reverse-RPC
  (`PollerHost::credentials_get`) and publishes the message to
  the `plugin.outbound.<channel>.<account_id>` topic — daemon
  picks up via the standard channel plugin fanout.

## Wire shape

Daemon publishes `plugin.poller.rss.tick` with a `Message`
envelope (`reply_to` set); subprocess replies with `{ next_cursor,
metrics }` on the reply inbox. Error envelopes classify into
`PollerError::Transient` / `::Permanent` / `::Config` via JSON-RPC
error codes `-32001` / `-32002` / `-32602`.

## License

MIT OR Apache-2.0.
