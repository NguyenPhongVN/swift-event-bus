import SwiftUI

// MARK: - @EventPublisher

/// Publishes events from a SwiftUI view, using the injected ``EventBus`` by default.
///
/// Picks up the ``EventBus`` from the SwiftUI environment automatically, or bind to a
/// specific instance via `EventPublisher(bus:)`.
///
///     @EventPublisher var publish
///     Button("Login") { publish(UserLoggedIn(username: "An")) }
///     Button("Login") { $publish(UserLoggedIn(username: "An")) }   // callAsFunction
@propertyWrapper
public struct EventPublisher: DynamicProperty {

    @Environment(\.eventBus) private var environmentBus
    private let explicitBus: EventBus?

    /// Creates a publisher.
    ///
    /// - Parameter bus: A specific bus to publish to. When `nil`, uses the environment bus.
    public init(bus: EventBus? = nil) {
        self.explicitBus = bus
    }

    /// A closure that publishes any `Event` to the bus.
    public var wrappedValue: (any Event) -> Void {
        let bus = explicitBus ?? environmentBus
        return { event in Task { await bus.publish(event) } }
    }

    /// The property wrapper itself, enabling `$publish(event)` syntax via `callAsFunction`.
    public var projectedValue: EventPublisher { self }

    /// Publishes `event` to the bus. Equivalent to calling `wrappedValue(event)`.
    public func callAsFunction<T: Event>(_ event: T) {
        let bus = explicitBus ?? environmentBus
        Task { await bus.publish(event) }
    }
}
