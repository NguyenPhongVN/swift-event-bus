import SwiftUI
@preconcurrency import Combine
import OSLog

// MARK: - EventBus

/// Thread-safe, type-safe event bus for publish-subscribe communication.
///
/// Supports four subscription styles:
/// - **Closure**: ``on(_:priority:id:handler:)`` / ``off(_:id:)``
/// - **Async/await**: ``next(_:)``, ``stream(_:)``
/// - **Combine**: ``subscribe(_:)``
/// - **SwiftUI**: `@EventListener`, `@EventPublisher`, `.onEvent`, `.onEventLifeCycle`
///
/// Use ``shared`` for app-wide communication, or create a custom instance to scope
/// events to a subsystem.
public actor EventBus {

    // MARK: Shared Instance

    /// App-wide shared bus. Use a custom instance to scope events to a subsystem.
    public static let shared = EventBus()

    // MARK: Initializer

    /// Creates a new `EventBus`.
    ///
    /// - Parameter replayBufferLimit: Maximum events per type stored for replay.
    ///   Pass `0` to disable replay. Defaults to `100`.
    public init(
        replayBufferLimit: Int = 100,
        observability: EventBusObservability = .default
    ) {
        self.replayBufferLimit = max(0, replayBufferLimit)
        self.logger = Logger(subsystem: observability.subsystem, category: observability.category)
        self.signposter = observability.signpostsEnabled
            ? OSSignposter(logger: logger)
            : nil
    }

    // MARK: Private Storage

    private let replayBufferLimit: Int
    private let logger: Logger
    private let signposter: OSSignposter?
    private var subjects:                [ObjectIdentifier: Any] = [:]
    private var subjectFinishers:        [ObjectIdentifier: @Sendable () -> Void] = [:]
    private var subjectSubscriberCounts: [ObjectIdentifier: Int] = [:]
    private var handlers:                [ObjectIdentifier: [UUID: HandlerEntry]] = [:]
    private var middlewares:             [MiddlewareEntry] = []
    private var streams:                 [ObjectIdentifier: [UUID: StreamEntry]] = [:]
    private var oneshotHandlers:         [ObjectIdentifier: [UUID: OneshotEntry]] = [:]
    private var replayBuffers:           [ObjectIdentifier: [any Event]] = [:]
    private var nextSequence = 0
    private var publishCount = 0
    private var dropCount    = 0

    // MARK: Metrics

    /// Returns a current snapshot of internal counters for diagnostics and testing.
    public var metrics: EventBusMetrics {
        EventBusMetrics(
            totalPublished:           publishCount,
            totalDroppedByMiddleware: dropCount,
            activeStreams:            streams.values.reduce(0)          { $0 + $1.count },
            activeHandlers:           handlers.values.reduce(0)         { $0 + $1.count },
            activeOneshotHandlers:    oneshotHandlers.values.reduce(0)  { $0 + $1.count }
        )
    }

    // MARK: Middleware

    /// Appends a synchronous middleware to the processing chain.
    ///
    /// Middlewares execute in registration order. Return `nil` from `process(_:)` to
    /// drop the event before it reaches any subscriber.
    public func use(_ middleware: any EventMiddleware) {
        middlewares.append(.sync(middleware))
    }

    /// Appends an async middleware to the processing chain.
    public func use(_ middleware: any AsyncEventMiddleware) {
        middlewares.append(.async(middleware))
    }

    /// Removes the first occurrence of `middleware` from the chain.
    public func remove(_ middleware: any EventMiddleware) {
        middlewares.removeAll { $0.matches(middleware) }
    }

    /// Removes the first occurrence of `middleware` from the chain.
    public func remove(_ middleware: any AsyncEventMiddleware) {
        middlewares.removeAll { $0.matches(middleware) }
    }

    /// Removes all registered middlewares.
    public func removeAllMiddleware() {
        middlewares.removeAll()
    }

    /// Backward-compatible alias with the original plural name.
    public func removeAllMiddlewares() {
        removeAllMiddleware()
    }

    // MARK: Publish

    /// Broadcasts `event` to all current subscribers of type `T`.
    ///
    /// The event passes through the middleware chain before delivery. If any middleware
    /// returns `nil`, the event is silently dropped and no subscriber is notified.
    public func publish<T: Event>(_ event: T) async {
        publishCount += 1
        let signpostState = signposter?.beginInterval("Publish")
        defer {
            if let signposter, let signpostState {
                signposter.endInterval("Publish", signpostState)
            }
        }

        var current: (any Event)? = event
        for middleware in middlewares {
            guard let pending = current else { dropCount += 1; return }
            current = await middleware.process(pending)
        }

        guard let resolved = current as? T else {
            if current != nil {
                logger.warning(
                    "Dropped event after middleware changed type from \(String(describing: T.self), privacy: .public)"
                )
            }
            dropCount += 1
            return
        }

        let key = ObjectIdentifier(T.self)
        bufferReplayEvent(resolved, for: key)

        subject(for: T.self).send(resolved)
        dispatchHandlers(resolved, for: key)
        dispatchOneshotHandlers(resolved, for: key)
        streams[key]?.values.forEach { $0.yield(resolved) }
    }

    // MARK: Combine

    /// Returns a Combine publisher that emits every event of type `T`.
    ///
    /// The publisher completes when ``reset()`` is called.
    /// Middleware mutations and drops are reflected in emitted values.
    public func subscribe<T: Event>(_ type: T.Type) -> AnyPublisher<T, Never> {
        let key = ObjectIdentifier(T.self)
        return subject(for: type)
            .handleEvents(
                receiveSubscription: { [weak self] _ in
                    guard let self else { return }
                    Task { await self.incrementSubjectCount(key) }
                },
                receiveCompletion: { [weak self] _ in
                    guard let self else { return }
                    Task { await self.decrementSubjectCount(key) }
                },
                receiveCancel: { [weak self] in
                    guard let self else { return }
                    Task { await self.decrementSubjectCount(key) }
                }
            )
            .eraseToAnyPublisher()
    }

    // MARK: Closure API

    /// Registers a closure handler for events of type `T`.
    ///
    /// - Parameters:
    ///   - type: The event type to observe.
    ///   - priority: Dispatch order relative to other handlers for the same type.
    ///     Higher values run first; equal priorities run in registration order. Defaults to `.normal`.
    ///   - id: Identifier for later removal via ``off(_:id:)``. Defaults to a new `UUID`.
    ///   - handler: Closure invoked on every matching event.
    /// - Returns: The `id` used for registration, for passing to ``off(_:id:)``.
    @discardableResult
    public func on<T: Event>(
        _ type: T.Type,
        priority: EventPriority = .normal,
        id: UUID = UUID(),
        handler: @escaping @Sendable (T) -> Void
    ) -> UUID {
        registerHandler(type, priority: priority, limit: nil, id: id, owner: nil, handler: handler)
    }

    /// Registers a closure handler that fires at most `limit` times, then auto-unregisters.
    ///
    /// - Parameters:
    ///   - type: The event type to observe.
    ///   - limit: Maximum invocations before the handler is automatically removed.
    ///   - priority: Dispatch priority. Defaults to `.normal`.
    ///   - id: Identifier for early removal via ``off(_:id:)``. Defaults to a new `UUID`.
    ///   - handler: Closure invoked on each matching event.
    /// - Returns: The `id` used for registration.
    @discardableResult
    public func on<T: Event>(
        _ type: T.Type,
        limit: Int,
        priority: EventPriority = .normal,
        id: UUID = UUID(),
        handler: @escaping @Sendable (T) -> Void
    ) -> UUID {
        registerHandler(type, priority: priority, limit: max(0, limit), id: id, owner: nil, handler: handler)
    }

    /// Registers a closure handler tied to the lifetime of `owner`.
    ///
    /// Once `owner` is deallocated, the handler is automatically removed the next time
    /// an event of `T` is dispatched.
    @discardableResult
    @preconcurrency
    public func on<T: Event, Owner: AnyObject>(
        _ type: T.Type,
        owner: Owner,
        priority: EventPriority = .normal,
        id: UUID = UUID(),
        handler: @escaping @Sendable (Owner, T) -> Void
    ) -> UUID {
        let weakOwner = WeakOwnerBox(owner)
        return registerHandler(type, priority: priority, limit: nil, id: id, owner: weakOwner) { event in
            guard let owner = weakOwner.value as? Owner else { return }
            handler(owner, event)
        }
    }

    /// Registers a limited handler tied to the lifetime of `owner`.
    @discardableResult
    @preconcurrency
    public func on<T: Event, Owner: AnyObject>(
        _ type: T.Type,
        owner: Owner,
        limit: Int,
        priority: EventPriority = .normal,
        id: UUID = UUID(),
        handler: @escaping @Sendable (Owner, T) -> Void
    ) -> UUID {
        let weakOwner = WeakOwnerBox(owner)
        return registerHandler(type, priority: priority, limit: max(0, limit), id: id, owner: weakOwner) { event in
            guard let owner = weakOwner.value as? Owner else { return }
            handler(owner, event)
        }
    }

    /// Unregisters the handler identified by `id` for events of type `T`.
    public func off<T: Event>(_ type: T.Type, id: UUID) {
        let key = ObjectIdentifier(type)
        handlers[key]?[id] = nil
        if handlers[key]?.isEmpty == true { handlers[key] = nil }
    }

    /// Removes all closure handlers registered for events of type `T`.
    public func unsubscribeAll<T: Event>(for type: T.Type) {
        handlers[ObjectIdentifier(type)] = nil
    }

    // MARK: Async API

    /// Awaits the next event of type `T` and returns it.
    ///
    /// Throws `CancellationError` if the calling task is cancelled before an event arrives.
    /// For a non-throwing variant, use ``nextOrSuspend(_:)``.
    public func next<T: Event>(_ type: T.Type) async throws -> T {
        let id  = UUID()
        let key = ObjectIdentifier(type)
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { cont in
                oneshotHandlers[key, default: [:]][id] = OneshotEntry(
                    resume: { event in if let typed = event as? T { cont.resume(returning: typed) } },
                    cancel: { cont.resume(throwing: CancellationError()) }
                )
            }
        } onCancel: {
            Task { await self.cancelOneshotHandler(key: key, id: id) }
        }
    }

    /// Awaits the next event of type `T`.
    ///
    /// Unlike ``next(_:)``, this variant never throws on cancellation — the caller
    /// remains suspended until a matching event arrives or ``reset()`` is called.
    /// Prefer ``next(_:)`` in structured-concurrency contexts.
    public func nextOrSuspend<T: Event>(_ type: T.Type) async -> T {
        let id  = UUID()
        let key = ObjectIdentifier(type)
        return await withTaskCancellationHandler {
            await withCheckedContinuation { cont in
                oneshotHandlers[key, default: [:]][id] = OneshotEntry(
                    resume: { event in if let typed = event as? T { cont.resume(returning: typed) } },
                    cancel: {}
                )
            }
        } onCancel: {
            Task { await self.removeOneshotHandler(key: key, id: id) }
        }
    }

    /// Returns as soon as the first event of type `A` or `B` arrives.
    ///
    /// Exactly one of the two tuple elements will be non-`nil`.
    /// Throws `CancellationError` if the calling task is cancelled.
    public func waitForAny<A: Event, B: Event>(
        _ a: A.Type,
        _ b: B.Type
    ) async throws -> (A?, B?) {
        try await withThrowingTaskGroup(of: WaitResult<A, B>.self) { group in
            group.addTask { .first(try await self.next(a)) }
            group.addTask { .second(try await self.next(b)) }

            guard let result = try await group.next() else {
                preconditionFailure("Task group produced no results")
            }
            group.cancelAll()

            switch result {
            case .first(let event):  return (event, nil)
            case .second(let event): return (nil, event)
            case .timeout:           preconditionFailure("Timeout task not added in waitForAny")
            }
        }
    }

    /// Awaits one event of each type `A` and `B`, in any order, then returns both.
    ///
    /// - Parameter timeout: If provided, throws ``EventBusTimeoutError`` when the deadline
    ///   elapses before both events have arrived.
    /// - Throws: `CancellationError` on task cancellation; ``EventBusTimeoutError`` on timeout.
    public func waitForAll<A: Event, B: Event>(
        _ a: A.Type,
        _ b: B.Type,
        timeout: Duration? = nil
    ) async throws -> (A, B) {
        try await withThrowingTaskGroup(of: WaitResult<A, B>.self) { group in
            group.addTask { .first(try await self.next(a)) }
            group.addTask { .second(try await self.next(b)) }
            if let timeout {
                group.addTask { try await Task.sleep(for: timeout); return .timeout }
            }

            var first:  A?
            var second: B?

            while let partial = try await group.next() {
                switch partial {
                case .first(let e):  first  = e
                case .second(let e): second = e
                case .timeout:
                    group.cancelAll()
                    throw EventBusTimeoutError()
                }
                if let first, let second {
                    group.cancelAll()
                    return (first, second)
                }
            }
            preconditionFailure("waitForAll finished without collecting both events")
        }
    }

    // MARK: Stream API

    /// Returns an infinite `AsyncStream` that yields every event of type `T`.
    ///
    /// The stream ends when the task owning the `for await` loop is cancelled,
    /// or when ``reset()`` is called.
    public func stream<T: Event>(_ type: T.Type) -> AsyncStream<T> {
        makeStream(type, replay: 0)
    }

    /// Returns an `AsyncStream` that first replays up to `last` past events,
    /// then yields new events indefinitely.
    ///
    /// - Parameter last: Number of historical events to replay to the new subscriber.
    ///   Capped by the `replayBufferLimit` set at initialisation.
    public func stream<T: Event>(_ type: T.Type, replay last: Int) -> AsyncStream<T> {
        makeStream(type, replay: last)
    }

    /// Returns a filtered stream that only yields events matching `predicate`.
    public func stream<T: Event>(
        _ type: T.Type,
        filter predicate: @escaping @Sendable (T) -> Bool
    ) -> AsyncStream<T> {
        AsyncStream { cont in
            let task = Task {
                for await event in self.stream(type) where predicate(event) {
                    cont.yield(event)
                }
                cont.finish()
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    /// Returns a stream that applies `transform` to each event and yields the result.
    public func stream<T: Event, U: Sendable>(
        _ type: T.Type,
        map transform: @escaping @Sendable (T) -> U
    ) -> AsyncStream<U> {
        AsyncStream { cont in
            let task = Task {
                for await event in self.stream(type) {
                    cont.yield(transform(event))
                }
                cont.finish()
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    /// Returns a stream that suppresses rapid-fire events.
    ///
    /// An event is forwarded only after `interval` elapses with no new events of the same type.
    public func stream<T: Event>(
        _ type: T.Type,
        debounce interval: Duration
    ) -> AsyncStream<T> {
        AsyncStream { cont in
            let task = Task {
                let counter = GenerationCounter()
                var pending: Task<Void, Never>?

                for await event in self.stream(type) {
                    pending?.cancel()
                    counter.generation += 1
                    let generation = counter.generation
                    pending = Task { @Sendable in
                        try? await Task.sleep(for: interval)
                        guard !Task.isCancelled, counter.generation == generation else { return }
                        cont.yield(event)
                    }
                }
                pending?.cancel()
                cont.finish()
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    /// Returns a stream that forwards at most one event per `interval` window.
    ///
    /// - Parameter latest: When `true`, the most recent suppressed event is forwarded
    ///   at the window boundary. When `false`, suppressed events are discarded.
    public func stream<T: Event>(
        _ type: T.Type,
        throttle interval: Duration,
        latest: Bool = true
    ) -> AsyncStream<T> {
        AsyncStream { cont in
            let task = Task {
                var nextAllowed = ContinuousClock.now
                let counter     = GenerationCounter()
                var flush:      Task<Void, Never>?

                for await event in self.stream(type) {
                    let now = ContinuousClock.now
                    if now >= nextAllowed {
                        cont.yield(event)
                        nextAllowed = now + interval
                        counter.generation += 1
                        flush?.cancel()
                        flush = nil
                        continue
                    }
                    guard latest else { continue }
                    counter.generation += 1
                    let generation = counter.generation
                    flush?.cancel()
                    let target = nextAllowed
                    flush = Task { @Sendable in
                        let remaining = ContinuousClock.now.duration(to: target)
                        if remaining > .zero { try? await Task.sleep(for: remaining) }
                        guard !Task.isCancelled, counter.generation == generation else { return }
                        cont.yield(event)
                    }
                }
                flush?.cancel()
                cont.finish()
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    // MARK: Reset

    /// Removes all subscribers, closes all active `AsyncStream`s, and clears replay buffers.
    ///
    /// - Active `for await` loops exit cleanly.
    /// - Pending ``next(_:)`` calls throw `CancellationError`.
    /// - New subscriptions can be registered immediately after this call.
    public func reset() {
        subjectFinishers.values.forEach  { $0() }
        streams.values.forEach           { $0.values.forEach { $0.finish() } }
        oneshotHandlers.values.forEach   { $0.values.forEach { $0.cancel() } }

        subjects.removeAll()
        subjectFinishers.removeAll()
        subjectSubscriberCounts.removeAll()
        handlers.removeAll()
        oneshotHandlers.removeAll()
        streams.removeAll()
        replayBuffers.removeAll()
    }

    // MARK: Private Helpers

    private func registerHandler<T: Event>(
        _ type: T.Type,
        priority: EventPriority,
        limit: Int?,
        id: UUID,
        owner: WeakOwnerBox?,
        handler: @escaping @Sendable (T) -> Void
    ) -> UUID {
        let key = ObjectIdentifier(type)
        nextSequence += 1
        handlers[key, default: [:]][id] = HandlerEntry(
            priority:  priority,
            sequence:  nextSequence,
            remaining: limit,
            owner: owner,
            handler:   { event in if let typed = event as? T { handler(typed) } }
        )
        return id
    }

    private func dispatchHandlers<T: Event>(_ event: T, for key: ObjectIdentifier) {
        guard let bucket = handlers[key] else { return }

        let ordered = bucket.sorted {
            $0.value.priority == $1.value.priority
                ? $0.value.sequence < $1.value.sequence
                : $0.value.priority > $1.value.priority
        }

        for (id, entry) in ordered {
            if entry.owner?.value == nil, entry.owner != nil {
                handlers[key]?[id] = nil
                continue
            }
            entry.handler(event)
            guard var updated = handlers[key]?[id], let remaining = updated.remaining else { continue }
            let next = remaining - 1
            if next <= 0 {
                handlers[key]?[id] = nil
            } else {
                updated.remaining = next
                handlers[key]?[id] = updated
            }
        }
        if handlers[key]?.isEmpty == true { handlers[key] = nil }
    }

    private func dispatchOneshotHandlers<T: Event>(_ event: T, for key: ObjectIdentifier) {
        guard let shots = oneshotHandlers[key] else { return }
        oneshotHandlers[key] = nil
        shots.values.forEach { $0.resume(event) }
    }

    private func makeStream<T: Event>(_ type: T.Type, replay last: Int) -> AsyncStream<T> {
        let key  = ObjectIdentifier(type)
        let id   = UUID()
        let past = bufferedEvents(for: key, type: type, count: last)

        return AsyncStream { cont in
            past.forEach { cont.yield($0) }
            let entry = StreamEntry(
                yield:  { event in if let typed = event as? T { cont.yield(typed) } },
                finish: { cont.finish() }
            )
            cont.onTermination = { [weak self] _ in
                guard let self else { return }
                Task { await self.removeStream(key: key, id: id) }
            }
            streams[key, default: [:]][id] = entry
        }
    }

    private func bufferReplayEvent<T: Event>(_ event: T, for key: ObjectIdentifier) {
        guard replayBufferLimit > 0 else { return }
        replayBuffers[key, default: []].append(event)
        let excess = (replayBuffers[key]?.count ?? 0) - replayBufferLimit
        if excess > 0 { replayBuffers[key]?.removeFirst(excess) }
    }

    private func bufferedEvents<T: Event>(for key: ObjectIdentifier, type: T.Type, count: Int) -> [T] {
        guard count > 0 else { return [] }
        return (replayBuffers[key] ?? []).suffix(count).compactMap { $0 as? T }
    }

    private func removeStream(key: ObjectIdentifier, id: UUID) {
        streams[key]?[id] = nil
        if streams[key]?.isEmpty == true { streams[key] = nil }
    }

    private func removeOneshotHandler(key: ObjectIdentifier, id: UUID) {
        oneshotHandlers[key]?[id] = nil
        if oneshotHandlers[key]?.isEmpty == true { oneshotHandlers[key] = nil }
    }

    private func cancelOneshotHandler(key: ObjectIdentifier, id: UUID) {
        guard let entry = oneshotHandlers[key]?[id] else { return }
        oneshotHandlers[key]?[id] = nil
        if oneshotHandlers[key]?.isEmpty == true { oneshotHandlers[key] = nil }
        entry.cancel()
    }

    private func incrementSubjectCount(_ key: ObjectIdentifier) {
        subjectSubscriberCounts[key, default: 0] += 1
    }

    private func decrementSubjectCount(_ key: ObjectIdentifier) {
        guard let count = subjectSubscriberCounts[key] else { return }
        let updated = count - 1
        if updated <= 0 {
            subjectSubscriberCounts[key] = nil
            subjects[key]               = nil
            subjectFinishers[key]       = nil
        } else {
            subjectSubscriberCounts[key] = updated
        }
    }

    private func subject<T: Event>(for type: T.Type) -> PassthroughSubject<T, Never> {
        let key = ObjectIdentifier(type)
        if let stored = subjects[key] as? PassthroughSubject<T, Never> { return stored }
        let subject = PassthroughSubject<T, Never>()
        subjects[key]        = subject
        subjectFinishers[key] = { subject.send(completion: .finished) }
        return subject
    }
}

// MARK: - EventBus Helpers

public extension EventBus {
    /// Returns a `@Sendable () -> Void` closure suitable for use as a SwiftUI button action.
    ///
    ///     Button("Login", action: bus.action { LoginTapped() })
    nonisolated func action<T: Event>(_ make: @escaping @Sendable () -> T) -> @Sendable () -> Void {
        { [self] in Task { await self.publish(make()) } }
    }
}
