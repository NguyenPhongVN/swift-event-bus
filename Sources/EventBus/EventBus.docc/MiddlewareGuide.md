# Middleware Guide

Use middleware to transform or drop events before they reach subscribers.

Synchronous middleware:

```swift
final class PrefixMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? {
        event
    }
}
```

Async middleware:

```swift
final class RemoteValidationMiddleware: AsyncEventMiddleware {
    func process(_ event: any Event) async -> (any Event)? {
        event
    }
}
```

Registration:

```swift
await bus.use(PrefixMiddleware())
await bus.use(RemoteValidationMiddleware())
```

Removal:

```swift
await bus.removeAllMiddlewares()
```
