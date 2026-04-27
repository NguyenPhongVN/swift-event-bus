# EventBus — Release Plan Status

> Updated: 2026-04-27  
> Target release: **v1.0.0**  
> Local status: **implemented and verified**

---

## v1.0.0 Scope

### Runtime features
| Item | Status | Notes |
| --- | --- | --- |
| Weak subscriber auto-cleanup | DONE | `on(_:owner:priority:id:handler:)` and limited variant remove stale handlers when owner deallocates |
| `os.Logger` debug logging | DONE | `EventLogger` now writes through unified logging |
| OSSignposter integration | DONE | `EventBusObservability(signpostsEnabled:)` instruments `publish(_:)` |
| Backward-compatible middleware cleanup alias | DONE | `removeAllMiddlewares()` added alongside existing singular form |

### Release assets
| Item | Status | Notes |
| --- | --- | --- |
| `LICENSE` | DONE | MIT |
| `CHANGELOG.md` | DONE | Includes `1.0.0` and initial `0.1.0` history |
| `.swiftlint.yml` | DONE | Stabilizes CI behavior |
| SwiftUI example app | DONE | Source-only sample under `Examples/SwiftUIExample/` |
| DocC hosting workflow | DONE | `.github/workflows/docc.yml` deploys to GitHub Pages |
| CI workflow | DONE | Build, test, SwiftLint, iOS simulator |

### Verification
| Item | Status | Notes |
| --- | --- | --- |
| `swift build` | DONE | Clean |
| `swift test` | DONE | 56 / 56 passing |
| Stream operator coverage | DONE | filter, map, debounce, throttle |
| Metrics coverage | DONE | published, dropped, active handlers/streams/oneshot |
| Owner cleanup coverage | DONE | weak owner deallocation test added |
| Middleware type-mismatch drop coverage | DONE | verified |

---

## What is in v1.0.0 now

### Core API
- Generic publish, closure subscriptions, limited handlers, priority ordering, and type-wide unsubscribe.
- One-shot async APIs: `next(_:)`, `nextOrSuspend(_:)`, `waitForAny(_:_:)`, `waitForAll(_:_:timeout:)`.
- Streaming APIs: base stream, replay, filter, map, debounce, throttle.
- Combine publisher integration with automatic subject cleanup.
- Diagnostics via `EventBusMetrics`.
- Production observability via `EventBusObservability`.

### SwiftUI
- `@EventListener`
- `EventListenerStorage`
- `@EventPublisher`
- `.eventBus(_:)`
- `.onEvent`
- `.onEventLifeCycle`
- `.onChangeOfEmitEvent`
- `.emitEvent`

### Middleware
- Sync middleware
- Async middleware
- Per-instance removal
- Remove-all cleanup
- Debug logging with `EventLogger`

---

## Remaining release actions outside source code

These are not code gaps anymore; they are repo/distribution steps:

1. Create the final release commit.
2. Create and push git tag `v1.0.0`.
3. Publish GitHub release notes from `CHANGELOG.md`.
4. Enable GitHub Pages so DocC workflow can deploy.
5. Replace placeholder GitHub badge/URLs in docs if the canonical repo path changes.

---

## Verification snapshot

```text
swift build   -> PASS
swift test    -> PASS (56 tests)
warnings      -> 0
errors        -> 0
swift mode    -> 6
```
