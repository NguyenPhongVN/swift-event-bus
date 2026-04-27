# EventBus

A lightweight, type-safe event bus for Swift — with full support for async/await, Combine, SwiftUI, weak owner cleanup, and production observability.

[![CI](https://github.com/your-org/swift-event-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/swift-event-bus/actions)
![Swift 6](https://img.shields.io/badge/Swift-6-orange)
![Platforms](https://img.shields.io/badge/platforms-iOS%2017%20·%20macOS%2015%20·%20tvOS%2017%20·%20watchOS%2010%20·%20visionOS%201-blue)

---

## Overview

`EventBus` decouples components through a publish-subscribe pattern. One part of your app publishes an event; any number of other parts react — with no direct dependency between them.

```swift
// 1. Define an event
struct UserLoggedIn: Event {
    let username: String
}

// 2. Subscribe anywhere
for await event in await EventBus.shared.stream(UserLoggedIn.self) {
    print("Welcome,", event.username)
}

// 3. Publish anywhere
await EventBus.shared.publish(UserLoggedIn(username: "An"))
```

Choose the subscription style that fits each use site:

| Style | API | Best for |
|---|---|---|
| Closure | `on` / `off` | ViewModels, imperative code |
| Async/await | `next(_:)`, `stream(_:)` | Swift Concurrency tasks |
| Combine | `subscribe(_:)` | Existing Combine pipelines |
| SwiftUI | `@EventListener`, `@EventPublisher`, `.onEvent` | Views and property wrappers |

Release assets included:

- `CHANGELOG.md`
- `.swiftlint.yml`
- `.github/workflows/docc.yml`
- `Examples/SwiftUIExample/`

---

## Installation

Add the package in `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/NguyenPhongVN/swift-event-bus", from: "1.0.0")
],
targets: [
    .target(
        name: "MyApp",
        dependencies: [.product(name: "EventBus", package: "swift-event-bus")]
    )
]
```

---

## Quick Start

### 1 — Define Events

Any `struct`, `enum`, or `class` conforming to `Event` can be published. Because `Event` inherits from `Sendable`, all stored properties must also be `Sendable`.

```swift
import EventBus

struct CartUpdated: Event {
    let itemCount: Int
    let totalPrice: Decimal
}

struct OrderPlaced: Event {
    let orderId: String
}
```

> Prefer `struct`. Value types are `Sendable` by default when all their properties are `Sendable`.

### 2 — Create a Bus

```swift
// App-wide singleton
let bus = EventBus.shared

// Feature-scoped instance (pass via dependency injection)
let checkoutBus = EventBus()

// Custom replay buffer (default: 100 events per type)
let bus = EventBus(replayBufferLimit: 50)

// Enable OSSignposter intervals for Instruments
let observedBus = EventBus(
    observability: .init(
        subsystem: "com.example.checkout",
        category: "Runtime",
        signpostsEnabled: true
    )
)
```

### 3 — Publish

```swift
// From an async context
await bus.publish(CartUpdated(itemCount: 3, totalPrice: 49.99))

// From a synchronous context
Task { await bus.publish(OrderPlaced(orderId: "ORD-001")) }
```

---

## Subscription Styles

### Closure

```swift
let id = await bus.on(CartUpdated.self) { event in
    print("Cart:", event.itemCount, "items")
}

// Remove later
await bus.off(CartUpdated.self, id: id)

// Remove all handlers for a type
await bus.unsubscribeAll(for: CartUpdated.self)
```

**Auto-unsubscribe after N deliveries:**

```swift
await bus.on(OrderPlaced.self, limit: 1) { event in
    showConfirmation(for: event.orderId)   // fires once, then removed automatically
}
```

**Priority ordering** (high → normal → low, registration order within same priority):

```swift
await bus.on(OrderPlaced.self, priority: .high)   { _ in validateInventory() }
await bus.on(OrderPlaced.self, priority: .normal) { _ in sendConfirmationEmail() }
await bus.on(OrderPlaced.self, priority: .low)    { _ in updateAnalytics() }
```

**Weak owner auto-cleanup**:

```swift
@MainActor
final class CheckoutViewModel {
    var completedOrders: [String] = []
}

let viewModel = CheckoutViewModel()

await bus.on(OrderPlaced.self, owner: viewModel) { owner, event in
    owner.completedOrders.append(event.orderId)
}
```

### Async/Await — One-Shot

`next(_:)` suspends until the next matching event arrives, then returns. Throws `CancellationError` if the task is cancelled first.

```swift
let event = try await bus.next(OrderPlaced.self)
print("Order confirmed:", event.orderId)
```

Use the non-throwing variant only when you deliberately want to suspend until an event arrives:

```swift
let event = await bus.nextOrSuspend(OrderPlaced.self)
```

**Race two event types** — returns as soon as the first one arrives:

```swift
let (login, guest) = try await bus.waitForAny(UserLoggedIn.self, GuestSessionStarted.self)
```

**Collect both event types** — waits until one of each has arrived:

```swift
let (profile, settings) = try await bus.waitForAll(
    ProfileLoaded.self,
    SettingsLoaded.self,
    timeout: .seconds(5)   // throws EventBusTimeoutError if deadline passes
)
```

### Async/Await — Stream

`stream(_:)` returns an `AsyncStream<T>` that yields every matching event until the task is cancelled or `reset()` is called.

```swift
let task = Task {
    for await event in await bus.stream(CartUpdated.self) {
        updateBadge(count: event.itemCount)
    }
}

// Unsubscribe
task.cancel()
```

**Stream operators:**

```swift
// Replay last N events to new subscribers
await bus.stream(CartUpdated.self, replay: 5)

// Filter — only yield events matching a predicate
await bus.stream(OrderPlaced.self, filter: { $0.orderId.hasPrefix("ORD") })

// Map — transform each event into another type
await bus.stream(UserLoggedIn.self, map: \.username)   // → AsyncStream<String>

// Debounce — emit only after the given silence interval
await bus.stream(SearchQueryChanged.self, debounce: .milliseconds(300))

// Throttle — at most one event per interval window
await bus.stream(LocationUpdated.self, throttle: .seconds(1), latest: true)
```

### Combine

```swift
import Combine

var cancellables = Set<AnyCancellable>()

await bus.subscribe(UserLoggedIn.self)
    .map(\.username)
    .sink { print("Login:", $0) }
    .store(in: &cancellables)
```

The publisher completes when `reset()` is called. The underlying subject is removed automatically when all subscribers cancel.

---

## Middleware

Middleware intercepts every published event before it reaches subscribers. Return `nil` to drop the event, return the original to pass it through, or return a mutated copy to transform it. Middlewares execute in registration order.

```swift
// Synchronous middleware
final class LoggingMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? {
        print("[Event]", type(of: event))
        return event
    }
}

// Async middleware — can await remote calls
final class AuthMiddleware: AsyncEventMiddleware {
    func process(_ event: any Event) async -> (any Event)? {
        guard event is PurchaseEvent else { return event }
        let authorized = await authService.validate()
        return authorized ? event : nil   // drop if unauthorized
    }
}
```

```swift
await bus.use(LoggingMiddleware())
await bus.use(AuthMiddleware(authService: authService))

// Remove specific middleware
await bus.remove(loggingMiddleware)

// Remove all
await bus.removeAllMiddleware()
```

**Built-in debug logger** (no-op in Release builds):

```swift
await bus.use(EventLogger())
```

`EventLogger` writes through `os.Logger`, so event traces show up in the unified logging system instead of raw `print`.

---

## SwiftUI

### Environment Injection

Scope a bus to a view subtree so all descendants use it automatically:

```swift
CheckoutView()
    .eventBus(checkoutBus)
```

### @EventListener

Observes the latest event and triggers a view re-render on each new arrival. Supports `@dynamicMemberLookup` so you can access event properties directly:

```swift
struct ProfileHeader: View {
    @EventListener(UserLoggedIn.self) var login

    var body: some View {
        // login.username is shorthand for login.value?.username
        Text(login.username ?? "Guest")
    }
}
```

`EventListenerStorage` also exposes the full event history:

```swift
login.value      // T? — most recent event
login.history    // [T] — all received events
login.count      // Int — history.count
login.reset()    // clears value and history without stopping the stream
```

### @EventPublisher

Publishes events from a view. Reads the bus from the environment automatically:

```swift
struct CheckoutButton: View {
    @EventPublisher private var publish

    var body: some View {
        Button("Place Order") {
            publish(OrderPlaced(orderId: "ORD-001"))
        }
    }
}
```

Or use `action(_:)` directly in a `Button`:

```swift
Button("Login", action: bus.action { UserLoggedIn(username: "An") })
```

### View Modifiers

```swift
// Tied to view visibility — pauses when view is covered by fullScreenCover or navigation push
.onEvent(CartUpdated.self) { event in
    updateBadge(count: event.itemCount)
}

// Tied to view lifetime — keeps receiving events even when the view is temporarily hidden
.onEventLifeCycle(OrderPlaced.self) { event in
    showNotification(for: event)
}

// Publish an event whenever a value changes
TextField("Search", text: $query)
    .onChangeOfEmitEvent(of: query) { SearchQueryChanged(query: $0) }
```

| Modifier | Stream lifetime |
|---|---|
| `.onEvent` | View visible (`.task` lifecycle) |
| `.onEventLifeCycle` | View in hierarchy (`deinit`) |

---

## Diagnostics

```swift
let metrics = await bus.metrics

metrics.totalPublished           // total events through publish(_:)
metrics.totalDroppedByMiddleware // events dropped by middleware
metrics.activeStreams             // live AsyncStream subscriptions
metrics.activeHandlers           // registered closure handlers
metrics.activeOneshotHandlers    // pending next(_:) waiters
```

**Observability**:

```swift
let bus = EventBus(
    observability: .init(
        subsystem: "com.example.payments",
        category: "EventBus",
        signpostsEnabled: true
    )
)
```

When signposts are enabled, each `publish(_:)` emits an `OSSignposter` interval that can be inspected in Instruments.

**Reset** — clears all subscribers, closes all streams, and empties replay buffers:

```swift
await bus.reset()
// Active for-await loops exit cleanly.
// Pending next(_:) calls throw CancellationError.
```

---

## Platform Support

| Platform | Minimum version |
|---|---|
| iOS | 17.0 |
| macOS | 15.0 |
| tvOS | 17.0 |
| watchOS | 10.0 |
| visionOS | 1.0 |

Requires **Swift 6** with strict concurrency enabled.

---

## Example App

See `Examples/SwiftUIExample/` for a minimal SwiftUI sample showing environment injection, `@EventListener`, `@EventPublisher`, and `.onEventLifeCycle`.

---

## License

MIT — see [LICENSE](LICENSE).
