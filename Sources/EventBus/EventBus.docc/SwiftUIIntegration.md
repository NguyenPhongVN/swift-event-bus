# SwiftUI Integration

Attach a bus to the view tree:

```swift
ContentView()
    .eventBus(myBus)
```

Observe the latest event:

```swift
struct ContentView: View {
    @EventListener(LoginEvent.self) private var login

    var body: some View {
        Text(login.username ?? "Guest")
    }
}
```

Publish from the view:

```swift
struct LoginButton: View {
    @EventPublisher private var publish

    var body: some View {
        Button("Login") {
            publish(LoginEvent(username: "An"))
        }
    }
}
```
