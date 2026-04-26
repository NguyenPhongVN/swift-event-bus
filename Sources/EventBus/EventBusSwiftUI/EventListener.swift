import SwiftUI
// MARK: - @EventListener

/// Observes the latest event of type `T` inside a SwiftUI view.
///
/// Supports `@dynamicMemberLookup` so you can write `login.username` instead of
/// `login.value?.username`. Picks up the ``EventBus`` from the SwiftUI environment
/// automatically, or bind to a specific instance with `EventListener(_:bus:)`.
///
///     @EventListener(LoginEvent.self) var login
///     if let name = login.username { Text(name) }
@dynamicMemberLookup
@propertyWrapper
public struct EventListener<T: Event>: DynamicProperty {

    @Environment(\.eventBus) private var environmentBus
    @State private var storage: EventListenerStorage<T>

    private let type:        T.Type
    private let explicitBus: EventBus?

    /// Creates a listener for events of `type`.
    ///
    /// - Parameters:
    ///   - type: The event type to observe.
    ///   - bus: A specific bus to use. When `nil`, the bus is read from the SwiftUI environment.
    public init(_ type: T.Type, bus: EventBus? = nil) {
        self.type        = type
        self.explicitBus = bus
        _storage = State(wrappedValue: EventListenerStorage<T>())
    }

    public mutating func update() {
        let bus     = explicitBus ?? environmentBus
        let storage = self.storage
        let type    = self.type
        Task { @MainActor in storage.bind(type: type, bus: bus) }
    }

    /// The storage object exposing the latest event and full history.
    @MainActor public var wrappedValue: EventListenerStorage<T> { storage }

    /// Accesses a property of the most recent event directly, or `nil` if no event has arrived.
    @MainActor
    public subscript<V>(dynamicMember keyPath: KeyPath<T, V>) -> V? {
        storage.value?[keyPath: keyPath]
    }
}
