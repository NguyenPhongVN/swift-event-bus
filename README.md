# EventBus

Type-safe event bus for Swift with one shared surface across:

- closure subscriptions
- async/await one-shot waits
- `AsyncStream`
- Combine
- SwiftUI property wrappers and view helpers

## Installation

```swift
dependencies: [
    .package(url: "https://github.com/your-org/swift-event-bus.git", from: "0.1.0")
]
```

```swift
.target(
    name: "MyApp",
    dependencies: [
        .product(name: "EventBus", package: "swift-event-bus")
    ]
)
```

## Define Events

```swift
import EventBus

struct LoginEvent: Event {
    let username: String
}

struct LogoutEvent: Event {
    let userID: Int
}

struct CounterEvent: Event {
    let value: Int
}
```

## Create a Bus

Use the shared singleton:

```swift
let bus = EventBus.shared
```

Or scope a bus to one feature:

```swift
let bus = EventBus()
```

You can also configure replay buffer capacity:

```swift
let bus = EventBus(replayBufferLimit: 200)
```

## Publish Events

```swift
await bus.publish(LoginEvent(username: "an"))
await bus.publish(CounterEvent(value: 1))
```

## Closure API

Basic subscribe/unsubscribe:

```swift
let id = await bus.on(LoginEvent.self) { event in
    print("login:", event.username)
}

await bus.publish(LoginEvent(username: "an"))
await bus.off(LoginEvent.self, id: id)
```

Remove every closure handler for one event type:

```swift
await bus.unsubscribeAll(for: LoginEvent.self)
```

Auto-unsubscribe after `N` deliveries:

```swift
await bus.on(CounterEvent.self, limit: 3) { event in
    print("only first 3 events:", event.value)
}
```

Priority ordering:

```swift
await bus.on(LoginEvent.self, priority: .high) { _ in
    print("runs first")
}

await bus.on(LoginEvent.self, priority: .normal) { _ in
    print("runs after high")
}

await bus.on(LoginEvent.self, priority: .low) { _ in
    print("runs last")
}
```

Stable registration id:

```swift
let fixedID = UUID()
await bus.on(LoginEvent.self, id: fixedID) { _ in
    print("custom id")
}
```

## One-Shot Async API

Wait for the next matching event:

```swift
let login = try await bus.next(LoginEvent.self)
print(login.username)
```

If the waiting task is cancelled, `next(_:)` throws `CancellationError`.

Legacy suspend-forever behavior:

```swift
let login = await bus.nextOrSuspend(LoginEvent.self)
```

Wait until one of two event types arrives first:

```swift
let result = try await bus.waitForAny(LoginEvent.self, LogoutEvent.self)

if let login = result.0 {
    print("login:", login.username)
}

if let logout = result.1 {
    print("logout:", logout.userID)
}
```

Wait until both event types have arrived:

```swift
let (login, logout) = try await bus.waitForAll(
    LoginEvent.self,
    LogoutEvent.self,
    timeout: .seconds(5)
)
```

If the timeout expires, the API throws `EventBusTimeoutError`.

## AsyncStream API

Basic stream:

```swift
let stream = await bus.stream(CounterEvent.self)

for await event in stream {
    print(event.value)
}
```

Replay the last `N` events to new subscribers:

```swift
let replayed = await bus.stream(CounterEvent.self, replay: 5)
```

Filter:

```swift
let positives = await bus.stream(CounterEvent.self, filter: { event in
    event.value > 0
})
```

```swift
for await event in positives {
    print(event.value)
}
```

Map:

```swift
let usernames = await bus.stream(LoginEvent.self, map: { event in
    event.username.uppercased()
})

for await username in usernames {
    print(username)
}
```

Debounce:

```swift
let debounced = await bus.stream(CounterEvent.self, debounce: .milliseconds(300))
```

Throttle:

```swift
let throttled = await bus.stream(
    CounterEvent.self,
    throttle: .seconds(1),
    latest: true
)
```

## Combine API

```swift
import Combine

var cancellables = Set<AnyCancellable>()

await bus.subscribe(LoginEvent.self)
    .sink { event in
        print(event.username)
    }
    .store(in: &cancellables)
```

## Middleware

Synchronous middleware:

```swift
final class PrefixMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? {
        guard let login = event as? LoginEvent else { return event }
        return LoginEvent(username: "prefix-" + login.username)
    }
}
```

Async middleware:

```swift
final class AuthMiddleware: AsyncEventMiddleware {
    func process(_ event: any Event) async -> (any Event)? {
        // validate, enrich, or drop
        return event
    }
}
```

Register:

```swift
let sync = PrefixMiddleware()
let async = AuthMiddleware()

await bus.use(sync)
await bus.use(async)
```

Remove one middleware:

```swift
await bus.remove(sync)
await bus.remove(async)
```

Remove all middlewares:

```swift
await bus.removeAllMiddlewares()
```

Debug logger:

```swift
await bus.use(EventLogger())
```

`EventLogger` only prints in `DEBUG`.

## Metrics

```swift
let metrics = await bus.metrics

print(metrics.totalPublished)
print(metrics.totalDroppedByMiddleware)
print(metrics.activeStreams)
print(metrics.activeHandlers)
print(metrics.activeOneshotHandlers)
```

## Reset

Reset clears:

- Combine subjects
- closure handlers
- one-shot waiters
- active streams
- replay buffers

```swift
await bus.reset()
```

## EventBus Action Helper

Useful for SwiftUI button actions:

```swift
let action = bus.action { LoginEvent(username: "an") }
action()
```

Or directly in a button:

```swift
Button("Login", action: bus.action { LoginEvent(username: "an") })
```

## SwiftUI Property Wrappers

Inject a bus into the view tree:

```swift
RootView()
    .eventBus(bus)
```

Listen to the latest event:

```swift
struct ContentView: View {
    @EventListener(LoginEvent.self) private var login

    var body: some View {
        Text(login.username ?? "guest")
    }
}
```

Use an explicit bus instead of environment:

```swift
@EventListener(LoginEvent.self, bus: customBus) private var login
```

`EventListenerStorage` also exposes:

```swift
login.value
login.history
login.count
login.reset()
```

Publish from SwiftUI:

```swift
struct LoginButton: View {
    @EventPublisher private var publish

    var body: some View {
        Button("Login") {
            publish(LoginEvent(username: "an"))
        }
    }
}
```

Use explicit bus:

```swift
@EventPublisher(bus: customBus) private var publish
```

Call through projected value:

```swift
$publish(LoginEvent(username: "an"))
```

## SwiftUI View Helpers

Listen while the view's `.task` is active:

```swift
Text("Counter")
    .onEvent(CounterEvent.self, bus: bus) { event in
        print(event.value)
    }
```

Listen across the view lifecycle, not just visible state:

```swift
Text("Counter")
    .onEventLifeCycle(CounterEvent.self, bus: bus) { event in
        print(event.value)
    }
```

Publish when a value changes:

```swift
TextField("Username", text: $username)
    .onChangeOfEmitEvent(of: username, bus: bus) { value in
        LoginEvent(username: value)
    }
```

Publish immediately:

```swift
someView.emitEvent(of: LoginEvent(username: "an"), bus: bus)
```

## Platform Support

| Platform | Minimum |
| --- | --- |
| iOS | 17.0 |
| macOS | 15.0 |
| tvOS | 17.0 |
| watchOS | 10.0 |
| visionOS | 1.0 |

## License

The repository does not include a license file yet.
