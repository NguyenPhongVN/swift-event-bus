# Getting Started with EventBus

Add decoupled event communication to your Swift app in minutes.

## Overview

`EventBus` uses a publish-subscribe model: one part of your app publishes an event, and any other part can subscribe to it — without either side knowing about the other. This article walks through the three steps to get going:

1. Define your events.
2. Publish events.
3. Subscribe to events.

## Step 1 — Define Events

An event is any `struct`, `enum`, or `class` that conforms to ``Event``. Because `Event` inherits from `Sendable`, all stored properties must also be `Sendable`.

```swift
struct UserLoggedIn: Event {
    let username: String
}

struct UserLoggedOut: Event {
    let userId: Int
}

struct CartUpdated: Event {
    let itemCount: Int
    let totalPrice: Decimal
}
```

> Tip: Prefer `struct` for events. Value types are `Sendable` by default when all their properties are `Sendable`.

## Step 2 — Publish Events

Call ``EventBus/publish(_:)`` from anywhere in your app. It's an `async` method, so call it from an async context or wrap it in a `Task`:

```swift
// From an async context
await EventBus.shared.publish(UserLoggedIn(username: "An"))

// From a synchronous context
Task {
    await EventBus.shared.publish(CartUpdated(itemCount: 3, totalPrice: 49.99))
}
```

### Shared vs. Custom Bus

``EventBus/shared`` is a singleton suitable for app-wide events. For feature-scoped events, create a dedicated instance and pass it through dependency injection:

```swift
let checkoutBus = EventBus()

// Publish to the scoped bus
await checkoutBus.publish(CartUpdated(itemCount: 1, totalPrice: 9.99))
```

## Step 3 — Subscribe to Events

Choose the subscription style that fits your architecture:

### Closure

Register a handler with ``EventBus/on(_:priority:id:handler:)`` and remove it later with the returned `UUID`:

```swift
let id = await EventBus.shared.on(UserLoggedIn.self) { event in
    print("Logged in:", event.username)
}

// Later, when you no longer need the subscription:
await EventBus.shared.off(UserLoggedIn.self, id: id)
```

### Async/Await — One-Shot

``EventBus/next(_:)`` suspends until the next matching event arrives, then returns:

```swift
let event = try await EventBus.shared.next(UserLoggedIn.self)
print("First login:", event.username)
```

### Async/Await — Stream

``EventBus/stream(_:)`` returns an `AsyncStream` that yields every matching event until cancelled:

```swift
for await event in await EventBus.shared.stream(CartUpdated.self) {
    updateCartBadge(count: event.itemCount)
}
```

Place this inside a `Task` to run it concurrently. Cancel the task to stop receiving events:

```swift
let task = Task {
    for await event in await EventBus.shared.stream(UserLoggedIn.self) {
        print("Login:", event.username)
    }
}

// Stop streaming:
task.cancel()
```

### Combine

``EventBus/subscribe(_:)`` returns an `AnyPublisher` you can use in existing Combine pipelines:

```swift
EventBus.shared.subscribe(UserLoggedIn.self)
    .map(\.username)
    .sink { print("Login:", $0) }
    .store(in: &cancellables)
```

## Removing All Handlers

To remove every handler for a specific event type without affecting others, use ``EventBus/unsubscribeAll(for:)``:

```swift
await EventBus.shared.unsubscribeAll(for: CartUpdated.self)
```

To clear the entire bus — all handlers, streams, and Combine subjects — call ``EventBus/reset()``:

```swift
await EventBus.shared.reset()
```

> Note: After `reset()`, all active `for await` loops exit cleanly and pending `next()` calls throw `CancellationError`.

## Next Steps

- Learn how to intercept and transform events with <doc:MiddlewareGuide>.
- Explore async stream operators in <doc:AsyncGuide>.
- Connect EventBus to your SwiftUI views in <doc:SwiftUIIntegration>.
