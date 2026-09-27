# Hermes Presence — experimental, bridge-required

**Agent 现在会主动给你发信息，问问之前你无意中提过的事、一些进行到一半未完结的任务。**

An agent can reach out on its own about something you mentioned in passing or a task you left half-finished—without requiring a formal reminder request. Presence stores source-backed context, revisits it later, and may choose to contact you when it has a worthwhile reason. It can also remain silent; you can pause or cancel a follow-up. This is the intended behavior, not a guarantee of judgment or delivery in every case.

**Experimental source only. Not a stock Hermes plugin installation or an approved catalog entry.** The public `main` branch now contains the local 0.1.4 plugin source (timezone and topic/care changes). The `v0.1.3-experimental.1` prerelease tag remains the original 0.1.3 snapshot; it has not been rewritten.

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
should treat this repository as code for inspection, not a working integration.

## What's new in the 0.1.4 source on `main`

- Resolves a valid IANA timezone for conversation context: explicit plugin setting, effective Hermes setting, then the host timezone (`TZ`, Linux zoneinfo links or `/etc/timezone`, and optional `tzlocal`). Ambiguous abbreviations are not guessed; an unknown zone is reported as `Etc/UTC`. This reads settings only and does not change the system clock.
- A source-backed `topic_update` now requires a `care_decision` (`create` or `skip`) and a reason. When creating care, it stores the topic and follow-up draft in one transaction; failed writes roll back together, and repeated calls for the same development do not create another draft. A draft still needs a verified response-delivery receipt before it becomes active.
- Adds literal topic keyword lookup. The normal conversation model still judges whether an event is worth revisiting and when; these changes do not guarantee its judgment.

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

The included 13 isolated tests verify unsupported-host behavior, registration
contracts, timezone selection and atomic topic/care storage. They do not prove proactive contact was
sent or received, model decision quality, delivery reliability, or long-term
stability. No live conversations or personal database are used by these tests.
See `docs/COMPATIBILITY.md` for missing host contracts and admission blockers.

## License

MIT; the included upstream copyright notice is preserved. The original 0.1.3 prerelease tag remains unchanged; `main` publishes the current local 0.1.4 plugin source without its host bridge, runtime data or private configuration.
