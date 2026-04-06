import SwiftUI

// MARK: - View Event

public extension View {

    /// Listens for events of type `T` and calls `perform` on the main actor.
    /// The stream is automatically cancelled when the view disappears (e.g. navigation push,
    /// fullScreenCover). Use `onEventLifeCycle` if you need events while the view is covered.
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

    /// Publishes an event derived from `value` whenever it changes.
    func onChangeOfEmitEvent<V: Equatable>(
        of value: V,
        bus: EventBus = .shared,
        event: @escaping @Sendable (V) -> any Event
    ) -> some View {
        onChange(of: value) { _, new in
            Task { await bus.publish(event(new)) }
        }
    }

    /// Immediately publishes `value` to the bus.
    func emitEvent<V: Event>(of value: V, bus: EventBus = .shared) {
        Task { await bus.publish(value) }
    }

    /// Listens for events tied to the view's full lifecycle — not its visibility.
    ///
    /// Unlike `onEvent` (which uses `.task` and cancels on `onDisappear`), this
    /// keeps the stream alive as long as the view remains in the hierarchy, even
    /// when it is covered by a `fullScreenCover`, a navigation push, or any
    /// transition that triggers `onDisappear` without actually removing the view.
    /// The stream is cancelled only when the view is truly deallocated.
    func onEventLifeCycle<T: Event>(
        _ type: T.Type,
        bus: EventBus = .shared,
        perform: @escaping @Sendable (T) -> Void
    ) -> some View {
        modifier(LifeCycleEventModifier(type: type, bus: bus, perform: perform))
    }
}

// MARK: - LifeCycleEventModifier

private struct LifeCycleEventModifier<T: Event>: ViewModifier {

    @StateObject private var holder: EventStreamHolder<T>

    init(type: T.Type, bus: EventBus, perform: @escaping @Sendable (T) -> Void) {
        _holder = StateObject(wrappedValue: EventStreamHolder(type: type, bus: bus, perform: perform))
    }

    func body(content: Content) -> some View {
        content
    }
}

// MARK: - EventStreamHolder

/// Owns the background streaming task. Because it is a `@StateObject`, SwiftUI
/// keeps exactly one instance alive for the lifetime of the view in the hierarchy —
/// not just while the view is visible. The task is cancelled only in `deinit`.
@MainActor
private final class EventStreamHolder<T: Event>: ObservableObject {

    private var task: Task<Void, Never>?

    init(type: T.Type, bus: EventBus, perform: @escaping @Sendable (T) -> Void) {
        let type = type
        let bus  = bus
        task = Task { [weak self] in
            for await event in await bus.stream(type) {
                guard self != nil else { break }
                perform(event)
            }
        }
    }

    deinit { task?.cancel() }
}
