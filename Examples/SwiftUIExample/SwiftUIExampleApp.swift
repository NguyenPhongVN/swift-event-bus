import SwiftUI
import EventBus

@main
struct SwiftUIExampleApp: App {
    private let bus = EventBus(
        observability: .init(
            subsystem: "com.example.eventbus-demo",
            category: "App",
            signpostsEnabled: true
        )
    )

    var body: some Scene {
        WindowGroup {
            ContentView()
                .eventBus(bus)
        }
    }
}
