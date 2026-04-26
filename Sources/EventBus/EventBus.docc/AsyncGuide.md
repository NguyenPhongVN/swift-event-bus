# Async/Await and Streaming

Receive events using Swift Concurrency — one-shot waits, multi-event races, and infinite streams with built-in operators.

## Overview

`EventBus` provides two async subscription families:

- **One-shot** — suspend until the next matching event, then continue.
- **Streaming** — iterate a sequence of events with `for await`.

Both respect Swift Structured Concurrency: cancelling the parent task ends the subscription cleanly.

## One-Shot: next(_:)

``EventBus/next(_:)`` suspends the current task until the next event of the requested type arrives, then returns that event. If the task is cancelled first, it throws `CancellationError`.

```swift
// Wait for the user to log in before proceeding
let event = try await EventBus.shared.next(UserLoggedIn.self)
print("Welcome,", event.username)
```

Use `try await` inside a `Task` or any async function:

```swift
Task {
    do {
        let order = try await bus.next(OrderPlaced.self)
        showConfirmation(for: order)
    } catch {
        // Task was cancelled before an order arrived
    }
}
```

### next(_:) vs nextOrSuspend(_:)

``EventBus/nextOrSuspend(_:)`` is a non-throwing variant that ignores cancellation — the caller stays suspended until a matching event arrives or ``EventBus/reset()`` is called. Use it only when you are certain the surrounding context will always receive the event:

```swift
// Never throws — caller blocks indefinitely
let event = await bus.nextOrSuspend(UserLoggedIn.self)
```

Prefer ``EventBus/next(_:)`` in structured-concurrency contexts because it honours cooperative cancellation.

## Racing Two Event Types: waitForAny(_:_:)

``EventBus/waitForAny(_:_:)`` suspends until the first event of either type arrives, then returns. Exactly one of the two returned optionals is non-`nil`.

```swift
let (login, guest) = try await bus.waitForAny(UserLoggedIn.self, GuestSessionStarted.self)

if let login {
    showDashboard(for: login.username)
} else if let guest {
    showGuestBanner()
}
```

This is useful for branching on whichever user action comes first.

## Collecting Two Event Types: waitForAll(_:_:timeout:)

``EventBus/waitForAll(_:_:timeout:)`` suspends until one event of each type has arrived — in any order — then returns both.

```swift
let (profile, settings) = try await bus.waitForAll(
    ProfileLoaded.self,
    SettingsLoaded.self
)
renderHome(profile: profile, settings: settings)
```

Pass a `timeout` to throw ``EventBusTimeoutError`` if both events do not arrive in time:

```swift
do {
    let (profile, settings) = try await bus.waitForAll(
        ProfileLoaded.self,
        SettingsLoaded.self,
        timeout: .seconds(5)
    )
    renderHome(profile: profile, settings: settings)
} catch is EventBusTimeoutError {
    showErrorBanner("Data took too long to load.")
}
```

## Infinite Streams

``EventBus/stream(_:)`` returns an `AsyncStream<T>` that yields every event of the given type until the task is cancelled or ``EventBus/reset()`` is called.

```swift
let task = Task {
    for await event in await EventBus.shared.stream(CartUpdated.self) {
        updateBadge(count: event.itemCount)
    }
}

// Stop receiving events:
task.cancel()
```

> Note: Always run `for await` inside a `Task` so it doesn't block the calling context. Cancel the task to unsubscribe.

### Replay Past Events

``EventBus/stream(_:replay:)`` prefixes the stream with up to `last` events from the internal replay buffer before yielding new ones. This is useful for late subscribers that need to catch up on recent state.

```swift
// Receive the 5 most recent CartUpdated events, then continue streaming
for await event in await bus.stream(CartUpdated.self, replay: 5) {
    render(event)
}
```

The replay buffer holds up to `replayBufferLimit` events per type (default: 100), set at ``EventBus/init(replayBufferLimit:)``.

### Filtering

``EventBus/stream(_:filter:)`` drops events that do not satisfy a predicate:

```swift
for await event in await bus.stream(MessageReceived.self, filter: { !$0.isRead }) {
    showNotification(for: event)
}
```

### Mapping

``EventBus/stream(_:map:)`` transforms each event before yielding it. The result is `AsyncStream<U>` where `U` is the return type of the transform:

```swift
for await username in await bus.stream(UserLoggedIn.self, map: \.username) {
    print("Login:", username)
}
```

## Rate Limiting

### Debounce

``EventBus/stream(_:debounce:)`` suppresses rapid-fire events. An event is forwarded only after the given interval elapses with no new events of the same type.

```swift
// Only react after the user stops typing for 300 ms
for await event in await bus.stream(SearchQueryChanged.self, debounce: .milliseconds(300)) {
    performSearch(query: event.query)
}
```

This is well-suited for text fields, scroll position updates, and any other high-frequency input.

### Throttle

``EventBus/stream(_:throttle:latest:)`` limits delivery to at most one event per interval window.

```swift
// Refresh at most once per second
for await event in await bus.stream(LocationUpdated.self, throttle: .seconds(1)) {
    updateMap(location: event.coordinate)
}
```

When `latest` is `true` (the default), the most recent event seen during a suppressed window is delivered at the window boundary. When `false`, suppressed events are silently discarded.

```swift
// Discard intermediate events — only the first in each window is delivered
for await event in await bus.stream(FrameRendered.self, throttle: .milliseconds(16), latest: false) {
    processFrame(event)
}
```

## Combining Operators

Stream operators compose because each returns a new `AsyncStream`:

```swift
// Unread messages from a specific channel, debounced, mapped to display strings
let stream = await bus
    .stream(MessageReceived.self, filter: { $0.channel == "support" && !$0.isRead })

for await message in stream {
    // custom processing
}
```

For multi-step transformations, apply operators sequentially inside a `Task`.
