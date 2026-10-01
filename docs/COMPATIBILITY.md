# Compatibility and admission status

This is an **experimental, bridge-required, source-only prerelease**, not an
installable integration for stock Hermes Agent and not a catalog-approved plugin.
The separate host bridge and its replacement-file installer are not included.

## Checked upstream

NousResearch/hermes-agent commit `64ea66b03d44ead9ffea48161132e5deca5d255a`:

- `hermes_cli/plugins.py` does not list `gateway_service` in `VALID_HOOKS`.
- The upstream tree lacks `gateway/extension_services.py` and
  `gateway/delivery_receipt.py`, which belong to the custom bridge design.
- Presence's factory is conditional: without the hook it reports
  `gateway_bridge_v1_required`, keeps the tool unavailable and starts no scheduler.
  Registration still installs prompt sections and ordinary hooks and creates local
  plugin state; “unsupported” does not mean no registration side effects.
- The original bridge distribution targets a different host commit,
  `345cd2b057a452236de401d3534b8502a7465e8d`. Do not apply its host snapshots to
  newer Hermes versions.

## Tests actually run

For the 0.1.5 source publication, `python3 -B -m unittest discover -s tests -v`:
**16 tests passed**. The added cases verify that undated ongoing topics can survive
a session reset without scheduled contact or personal-memory writes, then gain a
care draft from fresh evidence. Delivery confirmation is still required to arm it.
These scripted calls do not measure real-model selection quality.

### Historical initial publication checks

`python3 -B -m unittest discover -s tests -v`: **4 tests passed**. These are
synthetic isolated registration/storage/parse checks, not live delivery tests.

The official `tools/plugin_guard.py` and `tools/skills_guard.py` downloaded from
the checked upstream commit were run unchanged against this source publication:
**plugin-guard-v4, verdict safe, zero findings**. This is a pattern scan, not a
security certification or an upstream review.

A full call to the downloaded `validate_plugin_dir()` could not complete because
the partial offline source snapshot lacks `hermes_cli.plugin_python_deps`.
The available official static checks were run separately. They found a genuine
manifest admission failure: `config_schema.temporal.type: object` is not among
that validator's accepted types (it accepts `dict`, `map`, or `mapping`).
The runtime source and manifest are intentionally preserved as distributed;
this publication does not silently relabel itself catalog-compatible.
No full capability-probe pass, stock-host installation pass, or catalog CI pass
is claimed. Capability declarations also require review before a future submission.

## Minimal upstream interface proposal (not submitted)

An upstream discussion should establish a small, versioned gateway-service
contract before adapting this plugin. It should not submit a bundle of old host
file replacements or Presence-specific business state into the core.

1. **Lifecycle:** a plugin-owned, version-negotiated async service factory;
   awaited start/stop and a clean-shutdown checkpoint notification. Fail closed
   for unknown versions. Profile isolation and a single-owner lease are required.
2. **Authorized ingress:** invoke the service after authentication/authorization
   and before dispatch, including busy/queued input. Supply verified profile,
   private-conversation identity, message ID, session identity and reset/resume
   transitions. Await persistence so newer input can revoke old review/send work.
3. **Turn binding:** a scoped turn context visible to plugin tools/hooks, with
   explicit child-agent exclusion and cancellation propagation. Do not require
   plugins to discover global gateway instances.
4. **Delivery completion:** a structured receipt covering the whole assistant
   handoff, including streaming and media. Distinguish sent/failed/unknown and
   partial delivery; generation completion alone must never arm a follow-up.
5. **Outbound authorization and fencing:** host-owned route verification, current
   session/busy checks, bounded send, idempotency token and explicit uncertain
   outcome. Do not retry an uncertain send automatically. Revalidate before send.
6. **Session lifecycle:** reset/resume/delete and shutdown/restart reconciliation
   that cannot resurrect revoked delivery authority or lose isolation between users.

Presence currently also depends on private Feishu adapter and gateway attributes.
A hook name alone is insufficient: those dependencies need supported adapters or
an explicit compatibility contract, plus tests for auth refusal, groups, stale
input, concurrent ingress, ambiguous sends, restart and partial delivery.

No catalog PR is opened for this release. The next milestone is interface review
and an isolated upstream-host integration suite, not an installation promise.
