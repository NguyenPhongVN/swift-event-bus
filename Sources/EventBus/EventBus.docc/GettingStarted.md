# Getting Started

Create a bus:

```swift
let bus = EventBus()
```

Define an event:

```swift
struct LoginEvent: Event {
    let username: String
}
```

Publish:

```swift
await bus.publish(LoginEvent(username: "An"))
```

Receive with the style that fits your feature:

```swift
let id = await bus.on(LoginEvent.self) { event in
    print(event.username)
}
```

```swift
let login = try await bus.next(LoginEvent.self)
```

```swift
for await event in await bus.stream(LoginEvent.self) {
    print(event.username)
}
```
