# ``EventBus``

A lightweight, type-safe event bus for Swift apps, with full support for async/await, Combine, SwiftUI, weak owner cleanup, and production observability.

## Overview

`EventBus` decouples components through a publish-subscribe pattern. One part of your app publishes an event; any number of other parts react to it — with no direct dependency between them.

```swift
// Define an event
struct UserLoggedIn: Event {
    let username: String
}

// Publish from anywhere
await EventBus.shared.publish(UserLoggedIn(username: "An"))

// React anywhere
for await event in await EventBus.shared.stream(UserLoggedIn.self) {
    print("Welcome,", event.username)
}
```

### Subscription Styles

Choose the style that fits each use site:

| Style | API | Best for |
| --- | --- | --- |
| Closure | ``EventBus/on(_:priority:id:handler:)`` | Imperative code, ViewModels |
| Async/await | ``EventBus/next(_:)``, ``EventBus/stream(_:)`` | Swift Concurrency Tasks |
| Combine | ``EventBus/subscribe(_:)`` | Existing Combine pipelines |
| SwiftUI | ``EventListener``, ``EventPublisher`` | Views and property wrappers |

### Thread Safety

`EventBus` is a Swift actor. Every method is safe to call from any concurrent context. All subscriber callbacks are dispatched in the order they arrive at the actor.

### Scoping

Use ``EventBus/shared`` for app-wide events. Create a dedicated instance to scope events to a feature or subsystem:

```swift
let checkoutBus = EventBus()
```

## Topics

### Essentials

- <doc:GettingStarted>
- <doc:MiddlewareGuide>
- <doc:AsyncGuide>
- <doc:SwiftUIIntegration>

### Defining Events

- ``Event``

### Creating a Bus

- ``EventBus/shared``
- ``EventBus/init(replayBufferLimit:)``
- ``EventBus/init(replayBufferLimit:observability:)``

### Publishing

- ``EventBus/publish(_:)``
- ``EventBus/action(_:)``

### Closure Subscriptions

- ``EventBus/on(_:priority:id:handler:)``
- ``EventBus/on(_:limit:priority:id:handler:)``
- ``EventBus/on(_:owner:priority:id:handler:)``
- ``EventBus/on(_:owner:limit:priority:id:handler:)``
- ``EventBus/off(_:id:)``
- ``EventBus/unsubscribeAll(for:)``
- ``EventPriority``

### Async/Await

- ``EventBus/next(_:)``
- ``EventBus/nextOrSuspend(_:)``
- ``EventBus/waitForAny(_:_:)``
- ``EventBus/waitForAll(_:_:timeout:)``

### Streaming

- ``EventBus/stream(_:)``
- ``EventBus/stream(_:replay:)``
- ``EventBus/stream(_:filter:)``
- ``EventBus/stream(_:map:)``
- ``EventBus/stream(_:debounce:)``
- ``EventBus/stream(_:throttle:latest:)``

### Combine

- ``EventBus/subscribe(_:)``

### Middleware

- ``EventMiddleware``
- ``AsyncEventMiddleware``
- ``EventLogger``
- ``EventBus/use(_:)-4v1oc``
- ``EventBus/use(_:)-5m7lh``
- ``EventBus/remove(_:)-6xkqp``
- ``EventBus/remove(_:)-2b8ej``
- ``EventBus/removeAllMiddleware()``
- ``EventBus/removeAllMiddlewares()``

### SwiftUI

- ``EventListener``
- ``EventListenerStorage``
- ``EventPublisher``

### Diagnostics

- ``EventBus/metrics``
- ``EventBusMetrics``
- ``EventBusObservability``
- ``EventBus/reset()``

### Errors

- ``EventBusTimeoutError``
