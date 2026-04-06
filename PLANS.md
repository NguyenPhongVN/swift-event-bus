# EventBus - Implementation Status

> Updated: 2026-04-27  
> Current status: **v0.1+ implemented locally, tests passing**

---

## What exists now

### Core API
| Feature | API | Status |
| --- | --- | --- |
| Publish generic | `publish<T: Event>(_:)` | DONE |
| Closure subscribe | `on / off` | DONE |
| One-shot async throws | `next(_:) async throws` | DONE |
| Legacy suspend variant | `nextOrSuspend(_:)` | DONE |
| Infinite stream | `stream(_:)` | DONE |
| Combine publisher | `subscribe(_:)` | DONE |
| Reset all state | `reset()` | DONE |

### Middleware and lifecycle
| Feature | API | Status |
| --- | --- | --- |
| Sync middleware | `use/remove(EventMiddleware)` | DONE |
| Async middleware | `use/remove(AsyncEventMiddleware)` | DONE |
| Remove all middlewares | `removeAllMiddlewares()` | DONE |
| Drop / mutate event chain | sync + async | DONE |

### Handler control
| Feature | API | Status |
| --- | --- | --- |
| Remove all handlers by type | `unsubscribeAll(for:)` | DONE |
| Auto-unsubscribe after N calls | `on(limit:)` | DONE |
| Handler priority | `on(priority:)` | DONE |

### Stream operators and waiting
| Feature | API | Status |
| --- | --- | --- |
| Filter stream | `stream(_:filter:)` | DONE |
| Map stream | `stream(_:map:)` | DONE |
| Debounce stream | `stream(_:debounce:)` | DONE |
| Throttle stream | `stream(_:throttle:latest:)` | DONE |
| Wait first of two types | `waitForAny` | DONE |
| Wait both types | `waitForAll` | DONE |
| Replay last N events | `stream(_:replay:)` | DONE |

### SwiftUI
| Feature | API | Status |
| --- | --- | --- |
| Listen latest event | `@EventListener` | DONE |
| Publish from view | `@EventPublisher` | DONE |
| Environment injection | `EnvironmentValues.eventBus`, `.eventBus(_:)` | DONE |
| View task listener | `onEvent` | DONE |
| View lifecycle listener | `onEventLifeCycle` | DONE |
| Emit on value change | `onChangeOfEmitEvent` | DONE |
| Button helper | `action {}` | DONE |

### Observability and docs
| Feature | API / File | Status |
| --- | --- | --- |
| Debug logger | `EventLogger` | DONE |
| Metrics | `metrics` | DONE |
| README | `README.md` | DONE |
| DocC | `Sources/EventBus/EventBus.docc` | DONE |
| CI | `.github/workflows/ci.yml` | DONE |

---

## What was fixed from the old roadmap

### Fixed
- `next()` cancellation no longer leaks continuation. The new main API is `async throws`.
- Type-level cleanup now exists for handlers and middleware.
- Missing regression tests were added for cancellation, middleware removal, custom bus action, limits, priorities, replay, waits, and metrics.
- Combine subjects are removed when subscriber count returns to zero.
- SwiftUI can receive a bus through environment and use it from property wrappers and view modifiers.

### Implemented beyond MVP
- Async middleware chain.
- Replay support.
- Built-in metrics.
- Stream operators.
- Wait helpers for two event types.
- DocC and CI scaffolding.

---

## Remaining gaps

### Not implemented in repo workflow
- No git tag or package release has been created yet.
- No SPDX license file has been added yet.
- CI workflow was added, but it has not been validated in GitHub Actions from this local session.

### Technical debt
- `swift test` passes, but the package still emits a few strict-concurrency warnings in SwiftUI and stream helper internals. Behavior is correct under current tests, but these warnings should be cleaned up before a public release.

---

## Verification snapshot

- `swift test`: PASS
- Test count: 47
- Failures: 0

---

## Recommended next step

1. Remove remaining compiler warnings.
2. Add a license.
3. Run CI in GitHub and cut `v0.1.0`.
