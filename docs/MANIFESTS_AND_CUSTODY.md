
# Manifests and Custody

## Purpose

This document records the current downloader behavior and the operator/audit custody
contract for Databento provider receipts, local checksum ledgers, ingest/session
manifests, condition files, and canonical DBNs. It does not claim that every policy
below is enforced by code. The core rule is simple: **artifact role comes from
provenance and content, not from the filename `manifest.json`.**

This is a downstream operator guide, not a member of the release or its
receipt-bound repository-document set, and it has no authority over release
evidence. Publication and citation of its snapshot claims are gated on release
`dbc-20260802-v1` having a terminal `PASS` receipt. That receipt includes the
exact 44,341-path protected data/artifact pre/post gate derived from the
original 44,417 regular non-Markdown observations by excluding all 76
exact-basename `.DS_Store` rows across the 12 Databento-audit top-level
namespaces named in the receipt. The gate does not cover unrelated top-level
populations such as `CRYPTOHFTDATA` or `IBKR`. A copy without that closed
receipt may state the publication condition, not claim it was satisfied.
The terminal protocol additionally requires explicit external-writer
quiescence. Its compare-before-replace, complete-tree reopens, and protected
metadata fences detect ordinary drift; they are not an OS-mandatory lock.

The 2026-08-02 static reference inventory contains 6,145 canonical compressed DBNs and
745,248,556,848 bytes. The profile/catalog view has 27 dataset/schema/scope profile
groups, each schema-homogeneous, over 36 physical parents. The provenance table has
26 aggregation groups because its
`OPRA_NVDA_0DTE_CBBO_AND_DEFINITION` row combines the separate
`opra_nvda_0dte_cbbo1m` and `opra_nvda_0dte_definition` profile groups. It is a custody
baseline, not a live provider state.

## Canonical identity contract

The release-stable object identity is
`(release_id, relative_path, compressed_sha256)`. For release
`dbc-20260802-v1`, `storage_root_id` is
`apfs-0981c0ff-6bb8-4e7a-86db-5702f9344301:HFT-data`. The observed absolute
path is a locator only: it may change after a remount or migration and is not an
identity field. Never use basename alone.

Every custody record must retain:

- release ID, storage-root namespace, relative path, compressed SHA-256, and
  the observed absolute-path locator;
- logical group as an aggregation label, never as a substitute object key;
- local `st_size` and a freshly computed local digest when asserting current-byte
  integrity;
- every provider-declared `(size, algorithm, digest)` object, not only the preferred
  declaration;
- every provider receipt path and origin;
- every local hash declaration and source path;
- ingest membership, checksum membership, and size-only membership separately;
- conflict code and provenance tier;
- dataset, schema, symbol scope, requested interval, and split customization from the
  provider receipt when available.

Release-scoped relative-path and digest identity prevents known collisions:

- VXX/VXZ add-on basenames collide with 86-symbol EQUS regime basenames;
- ten `XNAS_ITCH_MULTI_10STOCK_MBO` symbol directories contain a split-symbol job;
- an unsplit ten-stock receipt has XNAS basenames that must not be attached to the
  NVDA-only MBO directory;
- the 168-byte archived EQUS NVDA daily sample shares a basename with a distinct
  349,657-byte canonical all-symbol object;
- two incompatible OPRA provider objects share the 2025-11-13 CMBP-1 basename.

## Manifest roles

| Artifact role | What it can prove | What it cannot prove |
|---|---|---|
| Databento-native manifest | Provider declared object name, expected size/hash, receipt scope | Current local byte identity unless rehashed; live availability; realized records |
| `condition.json` / condition calendar | Provider job condition or requested availability context | Realized file count, realized symbols, record count, successful decode |
| Session/ingest manifest | Local downloader membership and recorded transport metadata | Provider-native lineage unless linked to a provider receipt |
| `SHA256SUMS` | A retained local digest declaration | Databento origin; query scope; current-byte integrity unless recomputed |
| Analysis/custom manifest | Local analytical metadata according to its own schema | Provider custody merely because it is named `manifest.json` |
| Symlink | Navigation relation at a filesystem snapshot | Payload ownership, independent bytes, durable lineage after move |

### Filename-specific hazards

- `ARCX_PILLAR_NVDA_MBO/manifest.json` and
  `XNAS_BASIC_NVDA_CMBP_1/manifest.json` are ingest v1.2 records; provider receipts are
  elsewhere.
- The GLBX BBO, OHLCV-1s, and statistics `manifest.json` files are provider-native.
- The current GLBX OHLCV-1s rerun configuration targets the existing provider-native
  `manifest.json` as output. This is an overwrite hazard. Create a new output path and
  new receipt filename before any rerun.
- OPRA NVDA 0DTE's custom manifest is local analysis metadata, not a provider-native
  batch receipt.
- OPRA index CBBO and statistics each retain byte-identical root and `_provenance`
  session manifests. Duplicate records do not imply duplicate downloads.

## Per-file provenance field contract

The frozen `provenance_per_file.tsv` contains 6,145 rows and 22 columns. Field meanings
and exact populated-row counts are:

| Field or field family | Populated rows | Contract |
|---|---:|---|
| `absolute_path`, `relative_path`, `logical_group`, `local_size_bytes` | 6,145 | Source provenance-table fields. In the accepted joined payload they are prefixed as `prov_absolute_path`, `prov_relative_path`, `prov_logical_group`, and `prov_local_size_bytes`. Stable identity instead uses literal `_relative_path` plus `_compressed_sha256` and the table-wide release ID; the validator requires the provenance locator duplicates to agree. `prov_logical_group` has 26 values and is not literal `_group_id`, the 27-group profile key. |
| `provider_native_declared` | 6,145 | Explicit boolean: 4,569 true, 1,576 false |
| `provider_hash_algorithm`, `provider_declared_objects`, `provider_receipt_paths`, `provider_receipt_origins` | 4,569 | Retain provider evidence even when declarations conflict |
| `provider_expected_size_bytes`, `provider_expected_sha256`, `provider_expected_size_matches_local` | 4,568 | Populate only when provider declarations reduce to one unambiguous object |
| `local_sha_declared` | 6,145 | Explicit boolean: 4,515 true, 1,630 false |
| `local_declared_hashes`, `local_sha_source_paths` | 4,515 | Retained local declarations; may contain more than one value in a conflict |
| `ingest_manifest_membership` | 3,637 | One or more session/ingest manifests list the object |
| `ingest_checksum_membership` | 3,631 | Ingest membership includes a checksum |
| `ingest_membership_without_checksum` | 6 | Preserve size-only downloader evidence as a gap |
| `any_declared_hash` | 6,145 | Pre-audit retained-declaration boolean: 6,070 true, 75 false; it intentionally excludes the audit's new whole-scope pre/post ledgers |
| `conflict_code`, `provenance_tier` | 6,145 | Machine-actionable fail-closed classification |
| `note` | 3,134 | Human explanation; never the primary machine key |

There are 38 unique provider-receipt source paths, 34 unique local-hash source paths,
20 unique ingest-membership paths, and three unique size-only ingest-gap manifest
paths in the per-file table.

## Tier assignment and promotion rules

| Tier | Predicate | Snapshot count |
|---|---|---:|
| A | One unambiguous provider object and agreeing retained local hash declaration | 3,013 |
| B | One unambiguous provider object; no separate local hash declaration | 1,555 |
| C | Local hash declaration; no matched provider-native receipt | 1,501 |
| D | Incompatible provider or local declarations; exact-object rule required | 1 |
| E | No matched retained provider or local hash declaration before this audit | 75 |

Rules:

1. A checksum-less ingest membership is a gap annotation; it does not demote otherwise
   independent provider/local evidence to Tier E.
2. Provider expected-size equality is necessary but not sufficient for current-byte
   identity. Recompute the digest before claiming a current match.
3. Tier C proves retained local integrity metadata, not Databento lineage.
4. Tier B proves retained provider intent, not a second local custody hash.
5. Tier D always wins over basename, size-only, or recency selection. A conflict-
   specific exact-path, audit-hash, and matching-receipt rule may permit consumption
   without removing the Tier-D classification.
6. Tier E means no retained declaration predating this audit. The new audit hash
   observation does not establish provider lineage or silently promote the object.

## OPRA conflict record

The OPRA NVDA CMBP-1 group contains 19 files: 15 Tier A, three Tier C, and one Tier D.
The Tier-D path is the 33,547,368,092-byte 2025-11-13 object. Retained declarations
contain both a 71,207-byte provider object with SHA-256
`0039f4733cd7a968ad55ba8973c25a1b173fb82b5da9f468a764a18c4e803fa7`
and a 33,547,368,092-byte provider object with SHA-256
`20bc80be51e224cac47908e3ade83947d7078fa93ddf110ab6583cb8025fb80e`.

The current full-path object's audit pre-write SHA-256 is the latter value, exactly
matching the older full-object provider receipt `OPRA-20260305-FP53NRH898` and retained
local declaration. All eight DBNs in that older receipt match the audit ledger. The
newer `OPRA-20260611-3XLNN3TT55` receipt has nine DBNs: eight match their canonical
objects, while its 71,207-byte 2025-11-13 object is a distinct superseded sliver. The
newer receipt is not evidence for the current 33,547,368,092-byte object.

Required handling:

- preserve both provider jobs and both local declarations;
- never collapse `provider_declared_objects` to one value;
- block basename-based selection and manifest-recency selection;
- prohibit using the newer sliver receipt to validate the current full object;
- permit the current full object only when a loader pins its exact full path, the
  audit-observed full-object SHA-256, and the matching older provider receipt;
- otherwise exclude it; retain Tier D and the historical conflict in every case.

## Ingest checksum gaps

Exactly six files are ingest members without an ingest-recorded checksum:

| Relative path | Bytes | Independent evidence tier |
|---|---:|---|
| `EQUS_MINI/REGIME_UNIVERSE/bbo1s_2025-01-02_to_2026-01-30/equs-mini-20250102.bbo-1s.dbn.zst` | 31,652,797 | A |
| `EQUS_MINI/REGIME_UNIVERSE/ohlcv1s_2025-01-02_to_2026-01-30/equs-mini-20250102.ohlcv-1s.dbn.zst` | 3,577,290 | A |
| `GLBX_MDP3/ES_NQ/mbp10_2025-11-04_to_2025-11-25/glbx-mdp3-20251105.mbp-10.dbn.zst` | 1,069,969,658 | B |
| `GLBX_MDP3/ES_NQ/mbp10_2025-11-04_to_2025-11-25/glbx-mdp3-20251106.mbp-10.dbn.zst` | 1,421,411,627 | B |
| `GLBX_MDP3/ES_NQ/mbp10_2025-11-04_to_2025-11-25/glbx-mdp3-20251107.mbp-10.dbn.zst` | 1,478,831,109 | B |
| `GLBX_MDP3/ES_NQ/mbp10_2025-11-04_to_2025-11-25/glbx-mdp3-20251109.mbp-10.dbn.zst` | 16,659,835 | B |
| **Total** | **4,022,102,316** |  |

Repair must append a new verified checksum receipt or explicitly superseding manifest.
Do not edit a historical manifest in place.

## Group-specific acquisition hazards

1. **Base/addendum mixing:** EQUS MINI OHLCV has a 270-file provider-backed base and
   110-file local-only addendum. EQUS Summary all-symbol daily has a 482-file
   provider-backed base and 26-file local-only addendum. Three XNAS regime auxiliary
   groups each have a 126-file local-only addendum. Addenda that omit schema cannot
   inherit schema merely from directory naming.
2. **Incorrect configuration label:** the 26-file EQUS Summary addendum says
   `REGIME_UNIVERSE_86` despite residing in all-symbol OHLCV. Preserve the mismatch as
   a limitation; do not normalize it away.
3. **Split-symbol custody:** the 1,340-file multi-stock XNAS job is organized into ten
   symbol directories. Preserve the central mapping manifest and never rejoin by
   basename.
4. **Scope collision:** VXX/VXZ add-on files and 86-symbol regime files have colliding
   basenames. Receipt query scope plus full path is required.
5. **Unmatched later NVDA MBO:** 69 XNAS NVDA MBO files totaling 16,504,710,965 bytes
   have neither a matched provider receipt nor local SHA declaration. Do not attach the
   unsplit ten-stock receipt merely because filenames match.
6. **Schema-join populations:** OPRA definition files are documented as join-key
   populations; absence from an ingest session manifest does not mean they are
   interchangeable with quote/trade payloads.
7. **Provider-manifest overwrite:** the GLBX OHLCV-1s output path collision must be
   corrected before any acquisition rerun. Historical provider receipts are immutable.

## Condition-file semantics

Treat `condition.json` as provider job context, not as a realized holdings manifest.
For the EQUS MINI regime query, the condition/receipt scope names 86 symbols while the
canonical population contains 270 date files. Neither number proves 86 realized
symbols inside every file. The safe evidence chain is:

```text
condition/query scope
    -> provider declared objects
    -> exact local full paths
    -> fresh local hash match
    -> DBN metadata requested/partial/not-found symbols
    -> decoded realized records
```

Stop at the strongest stage actually measured. Never promote a condition calendar into
record, symbol, or partition statistics.

## Enforcement boundary

### `IMPLEMENTED_DOWNLOADER_RAIL`

Current `databento-ingest` source implements these behaviors:

- new payload bytes stream into `<filename>.downloading`; the running SHA-256
  and final size are compared with the selected Databento job object before
  `Path.rename()` promotes the temp name;
- resumable partial temps are rehashed before an HTTP Range request; malformed
  size/hash outcomes delete the known-bad temp and fail that attempt;
- a final file that already exists with the expected size is skipped without a
  hash recomputation; the explicit `verify` command is the separate full-digest
  path;
- data-file promotion has no file or directory `fsync` and there is no
  same-output-directory writer lock, so namespace-atomic rename is not a
  power-loss durability guarantee;
- the local v1.3 session manifest is written with the shared JSON atomic-write
  primitive (`fsync` plus `os.replace`), which is a stronger durability rail
  than the multi-gigabyte data-file promotion.

### `SESSION_MANIFEST_SEMANTICS`

The local v1.3 `manifest.json` is not a Databento-native receipt. When a mixed
run downloads some objects and skips matching-size final files, its `files`
array contains both successful new downloads and size-only skips, while its
`checksums` map contains only files newly downloaded and verified in that run.
Failed objects are excluded from `files` and listed in
`metadata.failed_files`. If every provider object already exists at the
expected size, the current function returns before writing a new session
manifest. The manifest is therefore a session/result record, not a complete
current-directory checksum ledger. Its known downstream soft contracts are
top-level `date_range` and `metadata.failed_files`; raw consumers read DBN
files directly.

### `MANUAL_OPERATOR_POLICY`

The following protocol is an operator/audit requirement. It is not claimed as
fully code-enforced.

Before any new acquisition or custody-document update:

1. Freeze the intended dataset, schema, symbol scope, interval, split settings, and
   output directory.
2. Resolve every existing output filename and manifest path read-only.
3. Use a new job-scoped directory or uniquely named receipt. Never overwrite
   provider-native receipts, pinned configs, or historical session manifests.
4. Retain provider manifest, condition, metadata, and symbology ancillary files where
   supplied.
5. Compute a fresh local digest after durable write and compare it to the unambiguous
   provider expected digest.
6. Record full-path membership and checksum membership separately.
7. Fail closed on competing declarations; preserve all candidates.
8. Register replicas and derivatives separately. The 467 decompressed DBNs,
   37,459 export regular files (including 40 exact-basename `.DS_Store`; 37,419
   remain in the terminal protected population), 3,146 live export aliases,
   and five archive DBNs are not new canonical coverage.
9. Validate symlinks without following them for byte accounting. Five archive links are
   currently broken and must not be silently repaired.
10. Publish a static snapshot timestamp. Never describe an on-disk receipt as evidence
    of current entitlement or live provider availability.

### `AUDIT_ONLY_CLASSIFICATION`

The 6,145-path canonical population, 27 profile groups, 26 custody groups,
36-parent map, A-E provenance tiers, replica/archive classifications, and
release identity contract were constructed by the 2026-08-02 read-only audit.
They are descriptive release artifacts. `databento-ingest` does not currently
compute, enforce, or update them during download.

### `UNIMPLEMENTED_RAIL`

The current downloader does not implement a writer lock, `fsync` durability for
data-file promotion, hash-before-skip for matching-size final files, automatic
conflict-preserving provider-receipt selection, the release-stable object key,
provenance-tier assignment, parent-profile registration, or automatic
wiki/catalog updates. It also does not prevent a config from targeting an
existing provider-native receipt filename. These remain explicit operator or
future implementation gates; this documentation must not describe them as
live capabilities.

## Durable release evidence

Use only the immutable Markdown release at
`audits/databento/releases/dbc-20260802-v1/`:

- `CANONICAL_FILE_PROFILE.md` embeds the complete per-file, profile-group, and
  physical-parent TSV payloads;
- `AUXILIARY_EVIDENCE.md` embeds the complete four-column replica path/size
  observation, a seven-column canonical-to-replica decoded-stream identity
  ledger, a separately generated four-column system-checksum ledger, and the
  five-file archive dual-decoder evidence. All 467 decompressed replicas match
  their canonical decoded streams under both identity passes; this establishes
  byte identity for the observed decoded representations, not additional
  market coverage or permission to delete either representation;
- `STATISTICAL_PROFILE_METHODOLOGY.md` defines populations, denominators,
  formulas, sentinels, units, and nonclaims;
- `METHOD_IMPLEMENTATION_SOURCES.md` classifies profiler, aggregator,
  validator, renderer, and dependency sources as revision-pinned, embedded,
  observation-only, or unavailable; an absolute scratch locator is not a pin;
- `EVIDENCE_MANIFEST.md` binds every immutable payload; and
- `VALIDATION_RECEIPT.md` is the terminal independent acceptance and
  installation receipt.

The embedded independent validation reports `PASS` with provenance and
aggregate checks enabled. The profile aggregate has 27
dataset/schema/scope rows; the 6,145-row per-file table yields 26 distinct
custody-group labels because that view combines the two OPRA NVDA 0DTE profile
groups. The terminal receipt also binds
the exact pre/post comparison of all 44,341 protected data/artifact paths in
its 12 named Databento-audit top-level namespaces. The original observation has
44,417 rows; all 76 exact-basename `.DS_Store` rows are excluded from the
terminal gate. The pre ledger is an audit-local observation, not an input to the retained A-E tier
assignment.

## Static/non-live limitation

No claim in this document establishes current Databento API access, entitlements,
remote object retention, catalog completeness, or live capture. A live acquisition
claim requires a new provider response and new custody artifacts. Historical manifests
must remain historical.
