# Publication boundary

Only the directory-plugin entrypoint, manifest, MIT LICENSE and Python modules
from the 0.1.3 distribution are included, plus synthetic tests and publication
documentation. The existing copyright notice is retained verbatim.

Excluded: host bridge snapshots, installer/management scripts, original
extraction metadata, environment and credential files, databases, migration
archives, chat transcripts, logs, bytecode and all unrelated historical project
content. Transfer/migration *code* is included, but no transferred user records.

The source allowlist was hash-compared with the original package. A literal and
path/credential-pattern review found no concrete personal identifiers or embedded
credentials in the allowlisted source; flagged Chinese strings were generic UI
messages. The network helper builds a URL from a host-provided Feishu SDK config,
not a bundled destination or key. Runtime records can still contain sensitive
information, so this publication review is not a full runtime security audit.

The official upstream pattern scan returned `safe` with zero findings. Synthetic
contract tests passed (4 tests). Full admission validation did not complete and
the available static validator rejected one manifest type; see COMPATIBILITY.md.
