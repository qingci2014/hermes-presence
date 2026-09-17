# Hermes Presence 0.1.3 — experimental, bridge-required

Source-only publication of a conversation-continuity experiment for Hermes Agent.
**Not a stock Hermes plugin installation. Not approved for the Hermes plugin catalog.**

## Compatibility

Presence requires a separate, custom **Gateway Service API v1** host bridge. That
bridge is not an official Hermes API. The original distribution targets host commit
`345cd2b057a452236de401d3534b8502a7465e8d` with per-file hash checks.
Upstream commit `64ea66b03d44ead9ffea48161132e5deca5d255a` does not expose the
`gateway_service` hook or the required gateway extension/delivery receipt modules.
Ordinary plugin installation cannot enable this implementation on that host.

This repository deliberately does **not** ship the original host-file replacement
installer, host snapshots, personal migration archives, or local environment files.
There is no supported installation command for unmodified upstream Hermes here.
Do not overwrite newer Hermes source files with an older bridge. Developers with
an independently reviewed compatible bridge may use this source; everyone else
should treat this release as code for inspection, not a working integration.

## What the implementation does

- Stores source-backed topics, preferences, personal records and unfinished work
  in a separate local SQLite database under the plugin state directory.
- Registers `/temporal`, the `temporal_commitment` tool, continuity/time prompt
  sections, and pre-LLM/pre-tool/post-tool hooks. The custom `gateway_service`
  factory is registered only when the host exposes it.
- Gates the tool on an attached gateway; on an unsupported host it reports
  `gateway_bridge_v1_required` / `waiting_for_gateway_adapter`.
- Includes background review, attempt budgets, cancellation and stale-input
  fencing. Background review uses the host-configured model via `ctx.llm` and
  can incur model charges. Intended transport is authorized Feishu private chat,
  one gateway process per profile. Groups and other platforms are not supported.
- Blocks writes to native user memory in a bound conversation and directs those
  writes to source-backed Presence records. This is an intentional behavior change.
- A fresh compatible installation defaults to enabled/live operation; `/temporal
  off` disables it persistently. There is no self-updater.

## Privacy and safety

Runtime records can contain sensitive quoted text. Do not commit plugin data,
SQLite files, chat exports, configuration or migration archives. The code uses
host-provided model and transport clients; no credentials are distributed here.
Review code and bridge permissions before use. This is experimental software,
not a security boundary against malicious in-process plugins.

## Tests and evidence boundaries

```sh
python -m unittest discover -s tests -v
```

The included synthetic tests verify unsupported-host behavior, registration
contracts and isolated storage only. They do not prove proactive contact was
sent or received, model decision quality, delivery reliability, or long-term
stability. No live conversations or personal database are used by these tests.
See `docs/COMPATIBILITY.md` for missing host contracts and admission blockers.

## License

MIT; the included upstream copyright notice is preserved. This publication
retains the 0.1.3 runtime source without pretending it is a newer implementation.
