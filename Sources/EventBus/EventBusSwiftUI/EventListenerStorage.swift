import Observation

/// Backing storage for ``EventListener``.
///
/// Access `value` and `history` for data binding. SwiftUI automatically re-renders
/// the view whenever a new event arrives.
@Observable
@MainActor
public final class EventListenerStorage<T: Event> {

    /// The most recently received event, or `nil` if none has arrived yet.
    public private(set) var value:   T?
    /// All events received since this storage was created or last ``reset()``.
    public private(set) var history: [T] = []
    /// Number of events in `history`.
    public var count: Int { history.count }

    @ObservationIgnored private var task:         Task<Void, Never>?
    @ObservationIgnored private var busIdentity:  ObjectIdentifier?
    @ObservationIgnored private var typeIdentity: ObjectIdentifier?

    public nonisolated init() {}

    func bind(type: T.Type, bus: EventBus) {
        let newBus  = ObjectIdentifier(bus)
        let newType = ObjectIdentifier(type)
        guard busIdentity != newBus || typeIdentity != newType else { return }
        task?.cancel()
        busIdentity  = newBus
        typeIdentity = newType
        task = Task { [weak self] in
            for await event in await bus.stream(type) {
                guard let self else { break }
                self.value = event
                self.history.append(event)
            }
        }
    }

    /// Clears `value` and `history` without affecting the live stream.
    public func reset() {
        value   = nil
        history = []
    }

    deinit { task?.cancel() }
}
