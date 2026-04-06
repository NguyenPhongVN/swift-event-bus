import SwiftUI
@preconcurrency import Combine

// MARK: - Protocols

/// Marker protocol for all events dispatched through EventBus.
public protocol Event: Sendable {}

/// Intercepts every published event before it reaches subscribers.
public protocol EventMiddleware: AnyObject, Sendable {
    func process(_ event: any Event) -> (any Event)?
}

/// Async variant of `EventMiddleware`.
public protocol AsyncEventMiddleware: AnyObject, Sendable {
    func process(_ event: any Event) async -> (any Event)?
}

/// Debug-only middleware that prints every event to the console.
public final class EventLogger: EventMiddleware, @unchecked Sendable {
    public init() {}

    public func process(_ event: any Event) -> (any Event)? {
#if DEBUG
        print("[EventBus] \(type(of: event)) \(event)")
#endif
        return event
    }
}

// MARK: - Supporting Types

public enum EventPriority: Int, Comparable, Sendable {
    case low = 0
    case normal = 50
    case high = 100

    public static func < (lhs: EventPriority, rhs: EventPriority) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

public struct EventBusMetrics: Sendable {
    public let totalPublished: Int
    public let totalDroppedByMiddleware: Int
    public let activeStreams: Int
    public let activeHandlers: Int
    public let activeOneshotHandlers: Int

    public init(
        totalPublished: Int,
        totalDroppedByMiddleware: Int,
        activeStreams: Int,
        activeHandlers: Int,
        activeOneshotHandlers: Int
    ) {
        self.totalPublished = totalPublished
        self.totalDroppedByMiddleware = totalDroppedByMiddleware
        self.activeStreams = activeStreams
        self.activeHandlers = activeHandlers
        self.activeOneshotHandlers = activeOneshotHandlers
    }
}

public struct EventBusTimeoutError: Error, Sendable {
    public init() {}
}

private struct StreamEntry: Sendable {
    let yield: @Sendable (Any) -> Void
    let finish: @Sendable () -> Void
}

private struct HandlerEntry: Sendable {
    let priority: EventPriority
    let sequence: Int
    var remaining: Int?
    let handler: @Sendable (any Event) -> Void
}

private struct OneShotEntry: Sendable {
    let resume: @Sendable (Any) -> Void
    let cancel: @Sendable () -> Void
}

private enum WaitForAnyResult<A: Sendable, B: Sendable>: Sendable {
    case first(A)
    case second(B)
}

private enum WaitForAllResult<A: Sendable, B: Sendable>: Sendable {
    case first(A)
    case second(B)
    case timeout
}

private final class DebounceToken: @unchecked Sendable {
    var value = 0
}

private final class ThrottleToken: @unchecked Sendable {
    var value = 0
}

private enum MiddlewareEntry {
    case sync(any EventMiddleware)
    case async(any AsyncEventMiddleware)

    func process(_ event: any Event) async -> (any Event)? {
        switch self {
        case .sync(let middleware):
            return middleware.process(event)
        case .async(let middleware):
            return await middleware.process(event)
        }
    }

    func matches(_ middleware: any EventMiddleware) -> Bool {
        guard case .sync(let stored) = self else { return false }
        return stored === middleware
    }

    func matches(_ middleware: any AsyncEventMiddleware) -> Bool {
        guard case .async(let stored) = self else { return false }
        return stored === middleware
    }
}

/// Thread-safe, type-safe event bus for publish-subscribe communication.
public actor EventBus {
    public static let shared = EventBus()

    public init(replayBufferLimit: Int = 100) {
        self.replayBufferLimit = max(0, replayBufferLimit)
    }

    private let replayBufferLimit: Int

    private var subjects: [ObjectIdentifier: Any] = [:]
    private var subjectFinishers: [ObjectIdentifier: @Sendable () -> Void] = [:]
    private var subjectSubscriberCounts: [ObjectIdentifier: Int] = [:]
    private var handlers: [ObjectIdentifier: [UUID: HandlerEntry]] = [:]
    private var middlewares: [MiddlewareEntry] = []
    private var streams: [ObjectIdentifier: [UUID: StreamEntry]] = [:]
    private var oneshotHandlers: [ObjectIdentifier: [UUID: OneShotEntry]] = [:]
    private var replayBuffers: [ObjectIdentifier: [any Event]] = [:]
    private var nextHandlerSequence = 0
    private var totalPublished = 0
    private var totalDroppedByMiddleware = 0

    // MARK: Metrics

    public var metrics: EventBusMetrics {
        EventBusMetrics(
            totalPublished: totalPublished,
            totalDroppedByMiddleware: totalDroppedByMiddleware,
            activeStreams: streams.values.reduce(0) { $0 + $1.count },
            activeHandlers: handlers.values.reduce(0) { $0 + $1.count },
            activeOneshotHandlers: oneshotHandlers.values.reduce(0) { $0 + $1.count }
        )
    }

    // MARK: Middleware

    public func use(_ middleware: any EventMiddleware) {
        middlewares.append(.sync(middleware))
    }

    public func use(_ middleware: any AsyncEventMiddleware) {
        middlewares.append(.async(middleware))
    }

    public func remove(_ middleware: any EventMiddleware) {
        middlewares.removeAll { $0.matches(middleware) }
    }

    public func remove(_ middleware: any AsyncEventMiddleware) {
        middlewares.removeAll { $0.matches(middleware) }
    }

    public func removeAllMiddlewares() {
        middlewares.removeAll()
    }

    // MARK: Publish

    public func publish<T: Event>(_ event: T) async {
        totalPublished += 1

        var current: (any Event)? = event
        for middleware in middlewares {
            guard let pending = current else {
                totalDroppedByMiddleware += 1
                return
            }
            current = await middleware.process(pending)
        }

        guard let final = current as? T else {
            if current != nil {
                print("[EventBus] Middleware changed event type; dropped \(type(of: event))")
            }
            totalDroppedByMiddleware += 1
            return
        }

        let key = ObjectIdentifier(T.self)
        appendReplayEvent(final, for: key)

        subject(for: T.self).send(final)
        dispatchHandlers(final, for: key)
        dispatchOneShotHandlers(final, for: key)
        streams[key]?.values.forEach { $0.yield(final) }
    }

    // MARK: Combine API

    public func subscribe<T: Event>(_ type: T.Type) -> AnyPublisher<T, Never> {
        let key = ObjectIdentifier(T.self)
        return subject(for: type)
            .handleEvents(
                receiveSubscription: { [weak self] _ in
                    guard let self else { return }
                    Task { await self.incrementSubjectSubscriberCount(for: key) }
                },
                receiveCompletion: { [weak self] _ in
                    guard let self else { return }
                    Task { await self.decrementSubjectSubscriberCount(for: key) }
                },
                receiveCancel: { [weak self] in
                    guard let self else { return }
                    Task { await self.decrementSubjectSubscriberCount(for: key) }
                }
            )
            .eraseToAnyPublisher()
    }

    // MARK: Closure API

    @discardableResult
    public func on<T: Event>(
        _ type: T.Type,
        priority: EventPriority = .normal,
        id: UUID = UUID(),
        handler: @escaping @Sendable (T) -> Void
    ) -> UUID {
        registerHandler(type, priority: priority, limit: nil, id: id, handler: handler)
    }

    @discardableResult
    public func on<T: Event>(
        _ type: T.Type,
        limit: Int,
        priority: EventPriority = .normal,
        id: UUID = UUID(),
        handler: @escaping @Sendable (T) -> Void
    ) -> UUID {
        registerHandler(type, priority: priority, limit: max(0, limit), id: id, handler: handler)
    }

    public func off<T: Event>(_ type: T.Type, id: UUID) {
        let key = ObjectIdentifier(type)
        handlers[key]?[id] = nil
        if handlers[key]?.isEmpty == true {
            handlers[key] = nil
        }
    }

    public func unsubscribeAll<T: Event>(for type: T.Type) {
        handlers[ObjectIdentifier(type)] = nil
    }

    // MARK: Async API

    public func next<T: Event>(_ type: T.Type) async throws -> T {
        let id = UUID()
        let key = ObjectIdentifier(type)
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { cont in
                oneshotHandlers[key, default: [:]][id] = OneShotEntry(
                    resume: { event in
                        if let typed = event as? T {
                            cont.resume(returning: typed)
                        }
                    },
                    cancel: {
                        cont.resume(throwing: CancellationError())
                    }
                )
            }
        } onCancel: {
            Task { await self.cancelOneshotHandler(key: key, id: id) }
        }
    }

    public func nextOrSuspend<T: Event>(_ type: T.Type) async -> T {
        let id = UUID()
        let key = ObjectIdentifier(type)
        return await withTaskCancellationHandler {
            await withCheckedContinuation { cont in
                oneshotHandlers[key, default: [:]][id] = OneShotEntry(
                    resume: { event in
                        if let typed = event as? T {
                            cont.resume(returning: typed)
                        }
                    },
                    cancel: {}
                )
            }
        } onCancel: {
            Task { await self.removeOneshotHandler(key: key, id: id) }
        }
    }

    public func waitForAny<A: Event, B: Event>(
        _ a: A.Type,
        _ b: B.Type
    ) async throws -> (A?, B?) {
        return try await withThrowingTaskGroup(of: WaitForAnyResult<A, B>.self) { group in
            group.addTask { .first(try await self.next(a)) }
            group.addTask { .second(try await self.next(b)) }

            let result = try await group.next()!
            group.cancelAll()

            switch result {
            case .first(let event):
                return (event, nil)
            case .second(let event):
                return (nil, event)
            }
        }
    }

    public func waitForAll<A: Event, B: Event>(
        _ a: A.Type,
        _ b: B.Type,
        timeout: Duration? = nil
    ) async throws -> (A, B) {
        return try await withThrowingTaskGroup(of: WaitForAllResult<A, B>.self) { group in
            group.addTask { .first(try await self.next(a)) }
            group.addTask { .second(try await self.next(b)) }

            if let timeout {
                group.addTask {
                    try await Task.sleep(for: timeout)
                    return .timeout
                }
            }

            var first: A?
            var second: B?

            while let partial = try await group.next() {
                switch partial {
                case .first(let event):
                    first = event
                case .second(let event):
                    second = event
                case .timeout:
                    group.cancelAll()
                    throw EventBusTimeoutError()
                }

                if let first, let second {
                    group.cancelAll()
                    return (first, second)
                }
            }

            fatalError("waitForAll finished without collecting both events")
        }
    }

    public func stream<T: Event>(_ type: T.Type) -> AsyncStream<T> {
        stream(type, replay: 0)
    }

    public func stream<T: Event>(_ type: T.Type, replay last: Int) -> AsyncStream<T> {
        let key = ObjectIdentifier(type)
        let id = UUID()
        let replayEvents = replayEvents(for: key, type: type, last: last)

        return AsyncStream { cont in
            replayEvents.forEach { cont.yield($0) }

            let entry = StreamEntry(
                yield: { event in
                    if let typed = event as? T {
                        cont.yield(typed)
                    }
                },
                finish: {
                    cont.finish()
                }
            )

            cont.onTermination = { [weak self] _ in
                guard let self else { return }
                Task { await self.removeStream(key: key, id: id) }
            }

            streams[key, default: [:]][id] = entry
        }
    }

    public func stream<T: Event>(
        _ type: T.Type,
        filter predicate: @escaping @Sendable (T) -> Bool
    ) -> AsyncStream<T> {
        AsyncStream { cont in
            let task = Task {
                let stream = await self.stream(type)
                for await event in stream {
                    if predicate(event) {
                        cont.yield(event)
                    }
                }
                cont.finish()
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    public func stream<T: Event, U: Sendable>(
        _ type: T.Type,
        map transform: @escaping @Sendable (T) -> U
    ) -> AsyncStream<U> {
        AsyncStream { cont in
            let task = Task {
                let stream = await self.stream(type)
                for await event in stream {
                    cont.yield(transform(event))
                }
                cont.finish()
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    public func stream<T: Event>(
        _ type: T.Type,
        debounce interval: Duration
    ) -> AsyncStream<T> {
        AsyncStream { cont in
            let task = Task {
                let token = DebounceToken()
                var debounceTask: Task<Void, Never>?

                let stream = await self.stream(type)
                for await event in stream {
                    debounceTask?.cancel()
                    token.value += 1
                    let currentToken = token.value
                    debounceTask = Task { @Sendable in
                        try? await Task.sleep(for: interval)
                        guard !Task.isCancelled, token.value == currentToken else { return }
                        cont.yield(event)
                    }
                }

                debounceTask?.cancel()
                cont.finish()
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    public func stream<T: Event>(
        _ type: T.Type,
        throttle interval: Duration,
        latest: Bool = true
    ) -> AsyncStream<T> {
        AsyncStream { cont in
            let task = Task {
                var nextAllowed = ContinuousClock.now
                let token = ThrottleToken()
                var flushTask: Task<Void, Never>?

                let stream = await self.stream(type)
                for await event in stream {
                    let now = ContinuousClock.now
                    if now >= nextAllowed {
                        cont.yield(event)
                        nextAllowed = now + interval
                        token.value += 1
                        flushTask?.cancel()
                        flushTask = nil
                        continue
                    }

                    guard latest else { continue }
                    token.value += 1
                    let currentToken = token.value
                    flushTask?.cancel()
                    let target = nextAllowed
                    flushTask = Task { @Sendable in
                        let sleep = ContinuousClock.now.duration(to: target)
                        if sleep > .zero {
                            try? await Task.sleep(for: sleep)
                        }
                        guard !Task.isCancelled, token.value == currentToken else { return }
                        cont.yield(event)
                    }
                }

                flushTask?.cancel()
                cont.finish()
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    // MARK: Reset

    public func reset() {
        subjectFinishers.values.forEach { $0() }

        for bucket in streams.values {
            bucket.values.forEach { $0.finish() }
        }

        for bucket in oneshotHandlers.values {
            bucket.values.forEach { $0.cancel() }
        }

        subjects.removeAll()
        subjectFinishers.removeAll()
        subjectSubscriberCounts.removeAll()
        handlers.removeAll()
        oneshotHandlers.removeAll()
        streams.removeAll()
        replayBuffers.removeAll()
    }

    // MARK: Private

    private func registerHandler<T: Event>(
        _ type: T.Type,
        priority: EventPriority,
        limit: Int?,
        id: UUID,
        handler: @escaping @Sendable (T) -> Void
    ) -> UUID {
        let key = ObjectIdentifier(type)
        nextHandlerSequence += 1
        handlers[key, default: [:]][id] = HandlerEntry(
            priority: priority,
            sequence: nextHandlerSequence,
            remaining: limit,
            handler: { event in
                if let typed = event as? T {
                    handler(typed)
                }
            }
        )
        return id
    }

    private func dispatchHandlers<T: Event>(_ event: T, for key: ObjectIdentifier) {
        guard let bucket = handlers[key] else { return }
        let ordered = bucket.sorted {
            if $0.value.priority == $1.value.priority {
                return $0.value.sequence < $1.value.sequence
            }
            return $0.value.priority.rawValue > $1.value.priority.rawValue
        }

        for (id, entry) in ordered {
            entry.handler(event)

            guard var current = handlers[key]?[id], let remaining = current.remaining else {
                continue
            }

            let updated = remaining - 1
            if updated <= 0 {
                handlers[key]?[id] = nil
            } else {
                current.remaining = updated
                handlers[key]?[id] = current
            }
        }

        if handlers[key]?.isEmpty == true {
            handlers[key] = nil
        }
    }

    private func dispatchOneShotHandlers<T: Event>(_ event: T, for key: ObjectIdentifier) {
        guard let shots = oneshotHandlers[key] else { return }
        oneshotHandlers[key] = nil
        shots.values.forEach { $0.resume(event) }
    }

    private func appendReplayEvent<T: Event>(_ event: T, for key: ObjectIdentifier) {
        guard replayBufferLimit > 0 else { return }
        replayBuffers[key, default: []].append(event)
        if replayBuffers[key]!.count > replayBufferLimit {
            replayBuffers[key]!.removeFirst(replayBuffers[key]!.count - replayBufferLimit)
        }
    }

    private func replayEvents<T: Event>(
        for key: ObjectIdentifier,
        type: T.Type,
        last: Int
    ) -> [T] {
        guard last > 0 else { return [] }
        let events = replayBuffers[key] ?? []
        return events.suffix(last).compactMap { $0 as? T }
    }

    private func removeStream(key: ObjectIdentifier, id: UUID) {
        streams[key]?[id] = nil
        if streams[key]?.isEmpty == true {
            streams[key] = nil
        }
    }

    private func removeOneshotHandler(key: ObjectIdentifier, id: UUID) {
        oneshotHandlers[key]?[id] = nil
        if oneshotHandlers[key]?.isEmpty == true {
            oneshotHandlers[key] = nil
        }
    }

    private func cancelOneshotHandler(key: ObjectIdentifier, id: UUID) {
        guard let entry = oneshotHandlers[key]?[id] else { return }
        oneshotHandlers[key]?[id] = nil
        if oneshotHandlers[key]?.isEmpty == true {
            oneshotHandlers[key] = nil
        }
        entry.cancel()
    }

    private func incrementSubjectSubscriberCount(for key: ObjectIdentifier) {
        subjectSubscriberCounts[key, default: 0] += 1
    }

    private func decrementSubjectSubscriberCount(for key: ObjectIdentifier) {
        guard let count = subjectSubscriberCounts[key] else { return }
        let updated = count - 1
        if updated <= 0 {
            subjectSubscriberCounts[key] = nil
            subjects[key] = nil
            subjectFinishers[key] = nil
        } else {
            subjectSubscriberCounts[key] = updated
        }
    }

    private func subject<T: Event>(for type: T.Type) -> PassthroughSubject<T, Never> {
        let key = ObjectIdentifier(type)
        if let subject = subjects[key] as? PassthroughSubject<T, Never> {
            return subject
        }

        let subject = PassthroughSubject<T, Never>()
        subjects[key] = subject
        subjectFinishers[key] = {
            subject.send(completion: .finished)
        }
        return subject
    }
}

// MARK: - EventBus Helpers

public extension EventBus {
    nonisolated func action<T: Event>(_ make: @escaping @Sendable () -> T) -> @Sendable () -> Void {
        { [self] in
            Task { await self.publish(make()) }
        }
    }
}

// MARK: - Environment

private struct EventBusEnvironmentKey: EnvironmentKey {
    static let defaultValue: EventBus = .shared
}

public extension EnvironmentValues {
    var eventBus: EventBus {
        get { self[EventBusEnvironmentKey.self] }
        set { self[EventBusEnvironmentKey.self] = newValue }
    }
}

public extension View {
    func eventBus(_ bus: EventBus) -> some View {
        environment(\.eventBus, bus)
    }
}

// MARK: - @EventListener

@dynamicMemberLookup
@propertyWrapper
public struct EventListener<T: Event>: DynamicProperty {
    @Environment(\.eventBus) private var environmentBus
    @StateObject private var storage: EventListenerStorage<T>

    private let type: T.Type
    private let explicitBus: EventBus?

    public init(_ type: T.Type, bus: EventBus? = nil) {
        self.type = type
        self.explicitBus = bus
        _storage = StateObject(wrappedValue: EventListenerStorage<T>())
    }

    public mutating func update() {
        let bus = explicitBus ?? environmentBus
        let storage = self.storage
        let type = self.type
        Task { @MainActor in
            storage.bind(type: type, bus: bus)
        }
    }

    @MainActor
    public var wrappedValue: EventListenerStorage<T> { self.storage }

    @MainActor
    public subscript<V>(dynamicMember keyPath: KeyPath<T, V>) -> V? {
        self.storage.value?[keyPath: keyPath]
    }
}

@MainActor
public final class EventListenerStorage<T: Event>: ObservableObject {
    @Published public private(set) var value: T?
    @Published public private(set) var history: [T] = []
    public var count: Int { history.count }

    @ObservationIgnored private var task: Task<Void, Never>?
    @ObservationIgnored private var busIdentity: ObjectIdentifier?
    @ObservationIgnored private var typeIdentity: ObjectIdentifier?

    nonisolated public init() {}

    func bind(type: T.Type, bus: EventBus) {
        let nextBusIdentity = ObjectIdentifier(bus)
        let nextTypeIdentity = ObjectIdentifier(type)
        guard busIdentity != nextBusIdentity || typeIdentity != nextTypeIdentity else { return }

        task?.cancel()
        busIdentity = nextBusIdentity
        typeIdentity = nextTypeIdentity
        start(type: type, bus: bus)
    }

    private func start(type: T.Type, bus: EventBus) {
        task = Task { [weak self] in
            let stream = await bus.stream(type)
            for await event in stream {
                await self?.apply(event)
            }
        }
    }

    private func apply(_ event: T) {
        value = event
        history.append(event)
    }

    public func reset() {
        value = nil
        history = []
    }

    deinit {
        task?.cancel()
    }
}

// MARK: - @EventPublisher

@propertyWrapper
public struct EventPublisher: DynamicProperty {
    @Environment(\.eventBus) private var environmentBus
    private let explicitBus: EventBus?

    public init(bus: EventBus? = nil) {
        self.explicitBus = bus
    }

    public var wrappedValue: (any Event) -> Void {
        let bus = explicitBus ?? environmentBus
        return { event in
            Task { await bus.publish(event) }
        }
    }

    public var projectedValue: EventPublisher { self }

    public func callAsFunction<T: Event>(_ event: T) {
        let bus = explicitBus ?? environmentBus
        Task { await bus.publish(event) }
    }
}
