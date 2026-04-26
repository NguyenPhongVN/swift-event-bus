import Foundation

// MARK: - Event

/// Marker protocol for all events dispatched through ``EventBus``.
/// Conform your event types to `Event` to publish and receive them.
public protocol Event: Sendable {}

// MARK: - Middleware

/// Intercepts every published event before it reaches subscribers.
///
/// Return `nil` to drop the event, the original event to pass it through,
/// or a mutated copy to transform it. Middlewares run in registration order.
public protocol EventMiddleware: AnyObject, Sendable {
    func process(_ event: any Event) -> (any Event)?
}

/// Async variant of ``EventMiddleware`` for middlewares that need to `await`.
public protocol AsyncEventMiddleware: AnyObject, Sendable {
    func process(_ event: any Event) async -> (any Event)?
}

/// Debug-only middleware that prints every event to the console.
///
/// Marked `@unchecked Sendable` because the class carries no mutable state;
/// it is effectively immutable after `init()`.
public final class EventLogger: EventMiddleware, @unchecked Sendable {
    public init() {}

    public func process(_ event: any Event) -> (any Event)? {
#if DEBUG
        print("[EventBus] \(type(of: event)) — \(event)")
#endif
        return event
    }
}

// MARK: - Supporting Types

/// Controls dispatch order when multiple handlers are registered for the same event type.
///
/// Higher-priority handlers run before lower-priority ones. Equal-priority handlers
/// run in registration order.
public enum EventPriority: Int, Comparable, Sendable {
    case low    = 0
    case normal = 50
    case high   = 100

    public static func < (lhs: EventPriority, rhs: EventPriority) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

/// A snapshot of ``EventBus`` internal counters, useful for diagnostics and testing.
public struct EventBusMetrics: Sendable {
    /// Total number of events that have passed through ``EventBus/publish(_:)``.
    public let totalPublished: Int
    /// Number of events silently dropped by the middleware chain.
    public let totalDroppedByMiddleware: Int
    /// Number of currently active `AsyncStream` subscriptions.
    public let activeStreams: Int
    /// Number of currently registered closure handlers.
    public let activeHandlers: Int
    /// Number of pending one-shot ``EventBus/next(_:)`` handlers.
    public let activeOneshotHandlers: Int
}

/// Thrown by ``EventBus/waitForAll(_:_:timeout:)`` when the deadline elapses
/// before all expected events arrive.
public struct EventBusTimeoutError: Error, Sendable {
    public init() {}
}

// MARK: - Private Supporting Types

internal struct StreamEntry: Sendable {
    let yield:  @Sendable (Any) -> Void
    let finish: @Sendable () -> Void
}

internal struct HandlerEntry: Sendable {
    let priority:  EventPriority
    let sequence:  Int
    var remaining: Int?
    let handler:   @Sendable (any Event) -> Void
}

internal struct OneshotEntry: Sendable {
    let resume: @Sendable (Any) -> Void
    let cancel: @Sendable () -> Void
}

internal enum MiddlewareEntry {
    case sync(any EventMiddleware)
    case async(any AsyncEventMiddleware)

    func process(_ event: any Event) async -> (any Event)? {
        switch self {
            case .sync(let mw):  return mw.process(event)
            case .async(let mw): return await mw.process(event)
        }
    }

    func matches(_ mw: any EventMiddleware) -> Bool {
        guard case .sync(let stored) = self else { return false }
        return stored === mw
    }

    func matches(_ mw: any AsyncEventMiddleware) -> Bool {
        guard case .async(let stored) = self else { return false }
        return stored === mw
    }
}

/// Mutable integer counter used to invalidate stale debounce/throttle tasks.
///
/// Each instance is created inside a single `Task` and never shared across tasks,
/// so `@unchecked Sendable` is safe here.
internal final class GenerationCounter: @unchecked Sendable {
    var generation = 0
}

internal enum WaitResult<A: Sendable, B: Sendable>: Sendable {
    case first(A), second(B), timeout
}
