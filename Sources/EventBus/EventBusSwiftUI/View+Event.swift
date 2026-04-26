import SwiftUI

// MARK: - View Extensions

public extension View {

    /// Listens for events of type `T` and calls `perform` on the main actor.
    ///
    /// Uses SwiftUI's `.task` lifecycle — the stream is cancelled when the view
    /// disappears (e.g. navigation push, `fullScreenCover`). Use
    /// ``onEventLifeCycle(_:bus:perform:)`` if you need events while the view
    /// is covered but still in the hierarchy.
    func onEvent<T: Event>(
        _ type: T.Type,
        bus: EventBus = .shared,
        perform: @escaping @MainActor (T) -> Void
    ) -> some View {
        task {
            for await event in await bus.stream(type) {
                perform(event)
            }
        }
    }

    /// Publishes an event derived from `value` whenever `value` changes.
    func onChangeOfEmitEvent<V: Equatable>(
        of value: V,
        bus: EventBus = .shared,
        event: @escaping @Sendable (V) -> any Event
    ) -> some View {
        onChange(of: value) { _, new in
            Task { await bus.publish(event(new)) }
        }
    }

    /// Immediately publishes `value` to `bus`.
    func emitEvent<V: Event>(of value: V, bus: EventBus = .shared) {
        Task { await bus.publish(value) }
    }

    /// Listens for events tied to the view's full lifecycle — not its visibility.
    ///
    /// Unlike ``onEvent(_:bus:perform:)``, this keeps the stream alive as long as the
    /// view remains in the hierarchy, even when covered by a `fullScreenCover`,
    /// a navigation push, or any other transition that triggers `onDisappear`.
    /// The stream is cancelled only when the view is truly deallocated.
    func onEventLifeCycle<T: Event>(
        _ type: T.Type,
        bus: EventBus = .shared,
        perform: @escaping @Sendable (T) -> Void
    ) -> some View {
        modifier(EventLifecycleModifier(type: type, bus: bus, perform: perform))
    }
}

// MARK: - EventLifecycleModifier

private struct EventLifecycleModifier<T: Event>: ViewModifier {

    @StateObject private var streamTask: EventStreamTask<T>

    init(type: T.Type, bus: EventBus, perform: @escaping @Sendable (T) -> Void) {
        _streamTask = StateObject(wrappedValue: EventStreamTask(type: type, bus: bus, perform: perform))
    }

    func body(content: Content) -> some View {
        content
    }
}

// MARK: - EventStreamTask

/// Owns the background streaming task for ``EventLifecycleModifier``.
///
/// As a `@StateObject`, SwiftUI keeps exactly one instance alive for the view's full
/// hierarchy lifetime — not just while the view is visible. The task is cancelled
/// only in `deinit`, so events are received even when the view is temporarily covered.
@MainActor
private final class EventStreamTask<T: Event>: ObservableObject {

    private var task: Task<Void, Never>?

    init(type: T.Type, bus: EventBus, perform: @escaping @Sendable (T) -> Void) {
        task = Task {
            for await event in await bus.stream(type) {
                perform(event)
            }
        }
    }

    deinit { task?.cancel() }
}
