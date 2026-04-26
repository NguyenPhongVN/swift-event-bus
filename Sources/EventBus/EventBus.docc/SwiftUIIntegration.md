# SwiftUI Integration

React to events directly in SwiftUI views using property wrappers and view modifiers.

## Overview

`EventBus` ships with four SwiftUI-specific APIs:

| API | Kind | Use |
| --- | --- | --- |
| ``EventListener`` | Property wrapper | Observe the latest event and re-render automatically |
| ``EventPublisher`` | Property wrapper | Publish events from a view action |
| ``View/onEvent(_:bus:perform:)`` | View modifier | Run a closure on each event while the view is visible |
| ``View/onEventLifeCycle(_:bus:perform:)`` | View modifier | Run a closure on each event for the view's full lifetime |

## Environment Injection

By default, all SwiftUI APIs use ``EventBus/shared``. To scope events to a view subtree, inject a custom bus with the `.eventBus(_:)` modifier:

```swift
let checkoutBus = EventBus()

CheckoutView()
    .eventBus(checkoutBus)
```

Every ``EventListener``, ``EventPublisher``, and `.onEvent` modifier inside `CheckoutView` and its descendants will automatically use `checkoutBus` instead of the shared instance.

## @EventListener

``EventListener`` is a property wrapper that subscribes to events and exposes the latest one for data binding. SwiftUI automatically re-renders the view whenever a new event arrives.

```swift
struct ProfileHeader: View {
    @EventListener(UserLoggedIn.self) var login

    var body: some View {
        Text(login.username ?? "Guest")
    }
}
```

`login.username` works because ``EventListener`` adopts `@dynamicMemberLookup` — it transparently forwards key-path access to the most recently received event. This is equivalent to `login.value?.username`.

### EventListenerStorage

``EventListener``'s `wrappedValue` is an ``EventListenerStorage`` object that exposes additional properties:

| Property | Type | Description |
| --- | --- | --- |
| `value` | `T?` | Most recently received event |
| `history` | `[T]` | All events received since creation |
| `count` | `Int` | Number of events in `history` |

Use `history` to display a feed or log:

```swift
struct EventFeed: View {
    @EventListener(MessageReceived.self) var messages

    var body: some View {
        List(messages.history, id: \.id) { msg in
            Text(msg.text)
        }
    }
}
```

Call `reset()` on the storage to clear `value` and `history` without interrupting the live stream:

```swift
Button("Clear") { messages.reset() }
```

### Explicit Bus

Pass a `bus` argument to bind to a specific instance instead of the environment:

```swift
@EventListener(CheckoutEvent.self, bus: checkoutBus) var checkout
```

## @EventPublisher

``EventPublisher`` is a property wrapper that exposes a publish closure. Call it directly, or call it with `callAsFunction` syntax:

```swift
struct LoginButton: View {
    @EventPublisher private var publish

    var body: some View {
        Button("Login") {
            publish(UserLoggedIn(username: "An"))
        }
    }
}
```

``EventPublisher`` reads the bus from the environment automatically. To use a specific bus:

```swift
@EventPublisher(bus: checkoutBus) private var publish
```

### action(_:) Alternative

For SwiftUI `Button` and other action closures, ``EventBus/action(_:)`` produces a `() -> Void` directly:

```swift
Button("Checkout", action: bus.action { CheckoutStarted() })
```

## View Modifiers

### onEvent

``View/onEvent(_:bus:perform:)`` attaches a closure that runs on the main actor every time a matching event arrives. The stream is backed by SwiftUI's `.task` modifier, so it starts when the view appears and is cancelled when the view disappears.

```swift
ContentView()
    .onEvent(NotificationReceived.self) { event in
        showBanner(event.message)
    }
```

> Important: Because `.onEvent` is tied to view visibility, it pauses when the view is covered by a `fullScreenCover` or a navigation push. Use ``View/onEventLifeCycle(_:bus:perform:)`` if you need to receive events while the view is temporarily hidden.

### onEventLifeCycle

``View/onEventLifeCycle(_:bus:perform:)`` keeps the subscription alive for the entire time the view remains in the view hierarchy — not just while it is visible. The stream is cancelled only when the view is deallocated.

```swift
ContentView()
    .onEventLifeCycle(OrderStatusChanged.self) { event in
        updateOrderBadge(event.status)
    }
```

This is the right choice when:
- The view is temporarily covered by a `fullScreenCover` or navigation push.
- You must not miss events that arrive while the view is off-screen.

**Choosing between the two modifiers:**

| Scenario | Modifier |
| --- | --- |
| Events only matter while the view is visible | ``View/onEvent(_:bus:perform:)`` |
| Events must arrive even when view is covered | ``View/onEventLifeCycle(_:bus:perform:)`` |

### onChangeOfEmitEvent

``View/onChangeOfEmitEvent(of:bus:event:)`` publishes an event whenever a value changes. It is a thin wrapper around SwiftUI's `onChange`:

```swift
TextField("Search", text: $query)
    .onChangeOfEmitEvent(of: query, event: { SearchQueryChanged(query: $0) })
```

### emitEvent

``View/emitEvent(of:bus:)`` fires a single event immediately — useful in `onAppear` or similar one-shot hooks:

```swift
.onAppear {
    bus.emitEvent(of: ScreenViewed(name: "Home"))
}
```
