# Middleware

Intercept, transform, validate, or drop events before they reach subscribers.

## Overview

Middleware sits between ``EventBus/publish(_:)`` and every subscriber. Each middleware in the chain receives the event, then either passes it along (possibly mutated) or drops it by returning `nil`.

```
publish(event)
  → Middleware A → Middleware B → Middleware C
    → Closure handlers
    → AsyncStream continuations
    → Combine publishers
```

Middlewares run in registration order. If any middleware returns `nil`, the chain stops immediately and no subscriber is notified.

## Synchronous Middleware

Conform to ``EventMiddleware`` and implement `process(_:)`:

```swift
// Pass every event through unchanged
final class LoggingMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? {
        print("[Event]", type(of: event))
        return event   // pass through
    }
}

// Drop events of a specific type
final class GuestFilterMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? {
        event is PurchaseEvent ? nil : event
    }
}

// Mutate a specific event type, pass everything else through unchanged
final class UsernameNormalizerMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? {
        guard let e = event as? UserLoggedIn else { return event }
        return UserLoggedIn(username: e.username.lowercased())
    }
}
```

Register with ``EventBus/use(_:)-4v1oc``:

```swift
await bus.use(LoggingMiddleware())
await bus.use(UsernameNormalizerMiddleware())
```

## Async Middleware

Conform to ``AsyncEventMiddleware`` when your middleware needs to `await` — for example, to validate a token with a remote server or read from a database:

```swift
final class AuthValidationMiddleware: AsyncEventMiddleware {
    let authService: AuthService

    func process(_ event: any Event) async -> (any Event)? {
        guard event is PurchaseEvent else { return event }
        let isAuthorized = await authService.validate()
        return isAuthorized ? event : nil   // drop if not authorized
    }
}
```

Register with ``EventBus/use(_:)-5m7lh``:

```swift
await bus.use(AuthValidationMiddleware(authService: authService))
```

> Important: Async middleware suspends the publish call until `process(_:)` returns. Use it only when truly necessary to avoid blocking high-frequency publish paths.

## Middleware Ordering

The chain executes in registration order. The following example adds a prefix, then converts to uppercase:

```swift
final class PrefixMiddleware: EventMiddleware {
    let prefix: String
    func process(_ event: any Event) -> (any Event)? {
        guard let e = event as? MessageEvent else { return event }
        return MessageEvent(text: prefix + e.text)
    }
}

final class UppercaseMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? {
        guard let e = event as? MessageEvent else { return event }
        return MessageEvent(text: e.text.uppercased())
    }
}

await bus.use(PrefixMiddleware(prefix: ">>> "))
await bus.use(UppercaseMiddleware())

await bus.publish(MessageEvent(text: "hello"))
// Subscriber receives: MessageEvent(text: ">>> HELLO")
```

## Built-In Middleware

``EventLogger`` is a ready-made debug middleware that writes every event through `os.Logger`. It is a no-op in release builds:

```swift
await bus.use(EventLogger())
```

## Removing Middleware

Remove a specific middleware with ``EventBus/remove(_:)-6xkqp`` (or ``EventBus/remove(_:)-2b8ej`` for async):

```swift
let logger = EventLogger()
await bus.use(logger)

// Later:
await bus.remove(logger)
```

Remove all middleware at once with ``EventBus/removeAllMiddleware()``:

```swift
await bus.removeAllMiddleware()
```

> Note: Removing middleware only affects future publish calls. Events already in flight are not affected.
