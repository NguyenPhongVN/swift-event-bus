# EventBus — Development Plans

> Updated: 2026-04-27  
> Build: **clean (0 warnings, 0 errors, Swift 6 strict concurrency)**  
> Tests: **47 / 47 passing**

---

## Đã có (Done)

### Core

| Feature | API | Ghi chú |
|---|---|---|
| Publish generic | `publish<T: Event>(_:)` | ✅ Qua middleware chain trước khi dispatch |
| Closure subscribe | `on(_:priority:id:handler:)` | ✅ Trả về `UUID` để `off()` |
| Closure limited | `on(_:limit:…)` | ✅ Tự `off()` sau N lần |
| Priority dispatch | `on(priority:)` | ✅ high → normal → low, cùng priority theo thứ tự đăng ký |
| Remove one handler | `off(_:id:)` | ✅ |
| Remove all by type | `unsubscribeAll(for:)` | ✅ |
| One-shot async (throws) | `next(_:) async throws` | ✅ Throws `CancellationError` nếu task bị cancel |
| One-shot async (no-throw) | `nextOrSuspend(_:)` | ✅ Block mãi đến khi có event hoặc `reset()` |
| Race hai types | `waitForAny(_:_:)` | ✅ Trả tuple, đúng một field non-nil |
| Collect cả hai types | `waitForAll(_:_:timeout:)` | ✅ Throws `EventBusTimeoutError` nếu hết giờ |
| Infinite stream | `stream(_:)` | ✅ |
| Replay stream | `stream(_:replay:)` | ✅ Buffer mặc định 100 event/type |
| Filter stream | `stream(_:filter:)` | ✅ |
| Map stream | `stream(_:map:)` | ✅ `AsyncStream<U>` |
| Debounce stream | `stream(_:debounce:)` | ✅ Chỉ emit sau khi "im lặng" đủ interval |
| Throttle stream | `stream(_:throttle:latest:)` | ✅ `latest: true/false` |
| Combine publisher | `subscribe(_:)` → `AnyPublisher<T, Never>` | ✅ Subject tự xoá khi subscriber = 0 |
| Reset toàn bộ | `reset()` | ✅ Đóng stream, cancel oneshot, xoá handlers + replay |
| Button action helper | `action(_:)` | ✅ `nonisolated`, trả `@Sendable () -> Void` |

### Middleware

| Feature | API | Ghi chú |
|---|---|---|
| Sync middleware | `use(_:)` / `remove(_:)` — `EventMiddleware` | ✅ |
| Async middleware | `use(_:)` / `remove(_:)` — `AsyncEventMiddleware` | ✅ Suspend publish đến khi `process` xong |
| Xoá tất cả | `removeAllMiddleware()` | ✅ |
| Drop event | Trả `nil` trong `process(_:)` | ✅ Tính vào `metrics.totalDroppedByMiddleware` |
| Mutate event | Trả event mới trong `process(_:)` | ✅ |
| Debug logger | `EventLogger` | ✅ No-op trên Release build |

### SwiftUI

| Feature | API | Ghi chú |
|---|---|---|
| Observe latest event | `@EventListener(T.self)` | ✅ `@Observable` + `@dynamicMemberLookup` |
| Lịch sử events | `EventListenerStorage.history` | ✅ |
| Publish từ view | `@EventPublisher` | ✅ `callAsFunction` syntax |
| Environment injection | `.eventBus(_:)` / `@Entry var eventBus` | ✅ |
| Subscribe theo visibility | `.onEvent(_:bus:perform:)` | ✅ Dùng `.task`, cancel khi `onDisappear` |
| Subscribe theo lifecycle | `.onEventLifeCycle(_:bus:perform:)` | ✅ `@StateObject` + `deinit`, sống qua `fullScreenCover` |
| Emit khi value thay đổi | `.onChangeOfEmitEvent(of:bus:event:)` | ✅ Wrap `onChange` |
| Fire-and-forget | `.emitEvent(of:bus:)` | ✅ |

### Observability & Infra

| Feature | File / API | Ghi chú |
|---|---|---|
| Metrics | `EventBus.metrics` → `EventBusMetrics` | ✅ published, dropped, streams, handlers, oneshot |
| DocC | `Sources/EventBus/EventBus.docc/` | ✅ 5 articles: EventBus, GettingStarted, MiddlewareGuide, AsyncGuide, SwiftUIIntegration |
| README | `README.md` | ✅ |
| CI | `.github/workflows/ci.yml` | ✅ Build + test + iOS simulator + SwiftLint |
| Swift 6 | `swiftLanguageModes: [.v6]` | ✅ Strict concurrency, 0 warning |

---

## Chưa có (Missing)

### Cần thiết trước khi release

| Item | Mức độ | Ghi chú |
|---|---|---|
| **LICENSE file** | 🔴 Bắt buộc | Chưa có file `LICENSE`. SPM package public cần SPDX license (MIT hoặc Apache-2.0). |
| **Git tag v0.1.0** | 🔴 Bắt buộc | Chưa có tag nào. SPM resolve theo tag. |
| **CHANGELOG.md** | 🟡 Nên có | Theo dõi thay đổi giữa các phiên bản. |
| **SwiftLint config** | 🟡 Nên có | CI chạy SwiftLint nhưng không có `.swiftlint.yml`; dùng default rules, dễ false-positive. |

### Thiếu test coverage

| Test case | Ghi chú |
|---|---|
| `stream(filter:)` | Chưa có test riêng |
| `stream(map:)` | Chưa có test riêng |
| `stream(debounce:)` | Chưa có test riêng |
| `stream(throttle:latest:)` | Chưa có test riêng |
| `nextOrSuspend(_:)` | Chưa có test riêng |
| `metrics.activeStreams` / `metrics.activeHandlers` | Chỉ test `totalPublished` + `totalDropped` |
| `metrics.activeOneshotHandlers` | Chưa test |
| `emitEvent(of:bus:)` | SwiftUI modifier, cần test tách biệt |
| Middleware thay đổi type | Middleware trả event type khác bị drop; chưa có test verify warning path |

### Tính năng còn thiếu

| Feature | Ghi chú |
|---|---|
| `waitForAny` / `waitForAll` cho 3+ types | Hiện tại bị giới hạn ở 2 types; variadic generics của Swift 6 có thể giải quyết nhưng API phức tạp hơn |
| `stream(merge:)` | Merge hai event stream khác type thành `AsyncStream<any Event>` |
| `stream(zip:)` | Pair event từ hai types theo thứ tự thành `AsyncStream<(A, B)>` |
| `stream(scan:)` | Accumulate state qua từng event: `scan(initial:) { acc, event in … }` |
| Combine subscribers trong `metrics` | `metrics.activeStreams` không tính Combine subscriber — chỉ tính `AsyncStream` |
| `replayBufferLimit` per type | Hiện tại dùng 1 limit chung cho tất cả types; không thể set limit riêng cho từng event type |
| Weak subscriber auto-cleanup | Không có cơ chế tự động `off()` khi object subscriber bị deallocate; caller phải tự quản lý |
| Test helper / `EventRecorder` | Không có mock/spy bus; test hiện tại dùng bus thật — khó isolate unit test SwiftUI views |
| Example app | Không có example project minh hoạ real-world usage |

---

## Cần tối ưu (Optimization)

| Vấn đề | Mức độ | Chi tiết |
|---|---|---|
| **`dispatchHandlers` sort mỗi lần publish** | 🟡 Trung bình | Hiện tại sort `handlers[key]` mỗi khi publish. Nếu handler ít thay đổi nhưng event nhiều, nên pre-sort khi `on()` / `off()` và cache sorted array. |
| **`drain()` trong tests dùng 100ms sleep** | 🟡 Trung bình | Fragile trên CI chậm. Một số test có thể dùng `XCTestExpectation` hoặc `confirmation()` (Swift Testing) thay vì sleep. |
| **`stream(debounce:)` tạo 1 Task / event** | 🟢 Thấp | Burst event → nhiều Task bị cancel ngay. Đúng về correctness, nhưng với event cực cao tần (>1000/s) nên dùng actor-based scheduler thay vì Task chain. |
| **Một `Task` detached trong `cancelOneshotHandler`** | 🟢 Thấp | `onCancel` gọi `Task { await self.cancelOneshotHandler(...) }` — có thể race với publish đến cùng actor. Hiện tại đúng vì actor serialize, nhưng tạo thêm 1 Task hop không cần thiết. |
| **`oneshotHandlers` bị cancel bởi `reset()` không throw** | 🟢 Thấp | `reset()` gọi `entry.cancel()` nhưng `nextOrSuspend` dùng `withCheckedContinuation` (non-throwing) — continuation bị huỷ nhưng không bao giờ resume, có thể leak theo một số Swift runtime version. |

---

## Roadmap phát triển (Next Steps)

### v0.1.0 — Release cơ bản (ngay bây giờ)
- [ ] Thêm `LICENSE` (MIT)
- [ ] Thêm `.swiftlint.yml` với rules phù hợp Apple style
- [ ] Bổ sung tests cho `stream(filter/map/debounce/throttle)`, `nextOrSuspend`, metrics
- [ ] Tạo `CHANGELOG.md`
- [ ] Cut git tag `v0.1.0`

### v0.2.0 — Test & DX
- [ ] `EventRecorder` — mock bus ghi lại events cho unit test SwiftUI views
- [ ] `stream(scan:initial:)` — accumulate state
- [ ] `metrics` tính cả Combine subscriber
- [ ] Pre-sort handlers khi đăng ký (thay vì mỗi lần publish)
- [ ] `replayBufferLimit` per event type

### v0.3.0 — Multi-type operators
- [ ] `stream(merge: A.self, B.self)` → `AsyncStream<any Event>`
- [ ] `stream(zip: A.self, B.self)` → `AsyncStream<(A, B)>`
- [ ] `waitForAny` / `waitForAll` cho 3 types (nếu variadic generics ổn định)

### v1.0.0 — Production-ready
- [ ] Weak subscriber auto-cleanup (`on(owner: self) { [weak self] in … }`)
- [ ] `os.Logger` thay `print` trong `EventLogger`
- [ ] Instruments / OSSignpost integration để profile publish latency
- [ ] Example SwiftUI app
- [ ] DocC hosted trên GitHub Pages

---

## Snapshot hiện tại

```
swift build   → ✅ 0 warnings, 0 errors
swift test    → ✅ 47 / 47 passed
Swift version → 6.3 (strict concurrency)
Platforms     → iOS 17+, macOS 15+, tvOS 17+, watchOS 10+, visionOS 1+
```
