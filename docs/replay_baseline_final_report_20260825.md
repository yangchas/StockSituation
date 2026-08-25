# Replay Baseline Final Report — 2026-08-25

## Baseline

- Baseline commit: `7af7f79c1372b956e8711e297413047434f5cb3c`
- Source branch: `codex/replay-p0-fixes`
- Formal checkout: clean checkout of the frozen replay baseline
- Git worktree: clean
- Production host used for verification: `cobra-ion.exe.xyz`

## Build and source closure

- Compiler: g++ 13.3.0
- protoc: `libprotoc 3.21.12`
- protobuf runtime: `/usr/local/protobuf` linked by the existing `make.sh`
- Generation command: `protoc -I C --cpp_out=C C/schema.proto`
- Generated `schema.pb.cc` SHA-256: `b0a0e75df5859d6d3845ccc137e5a1f274f5952edcd982af65cff9392b34ad2a`
- Generated `schema.pb.h` SHA-256: `732cf9c2f89ca06c5daaf380ec2d371c7e9be965d37c02d11889a59a74ea356a`

## Gates

| Gate | Result | Evidence |
|---|---|---|
| formal_git_checkout | PASS | clean detached checkout at baseline commit |
| tracked_test_closure | PASS | `engine_next/tests/replay_fixture_checks.py`: 6 passed |
| cpp_self_test | PASS | existing `--self-test` passed |
| auction_contract_consistency | PASS | narrow equal valid bid1/ask1 fallback and all negative boundaries passed |
| continuous_q2_replay | PASS | 305,893 rows, 60 frames, source reject 0 |
| q2frame_determinism | PASS | SHA-256 `6faa4b247ecfc7ab3be4b3305f4fa8a973239f96c789565bb89a2b3e2a69ea01` |
| offline_dependency_isolation | PASS | stale/missing replay hot-rank fixture fails before refresh; no external fallback |
| runner_state_isolation | PASS | patched `IntradayDataHub` references restored on exception |
| write_isolation | PASS | replay uses read-only fixture; no production writers |
| python_record_determinism | PASS | two runs, 202 records each, L2 `b40c8bf5c0ecd4b3c67456f2d23a8d8a853f58f150c3b7e586fdc91da3034a47` |

## Replay conclusion

`REPLAY_BASELINE_PASS`.

The reporting source-closure commit is intentionally separate from this frozen
replay baseline. It must not be substituted for the baseline until its clean
checkout and reporting tests are independently verified.

Replay records only reached clock checkpoints. Production scheduled handlers and notification semantics remain `NOT_EVALUATED` by design.

This report does not certify production fact equivalence or production email takeover.
