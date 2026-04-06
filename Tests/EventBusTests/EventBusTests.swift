import XCTest
@preconcurrency import Combine
@testable import EventBus

// MARK: - Test Events

private struct LoginEvent:   Event { let username: String }
private struct LogoutEvent:  Event { let userId:   Int    }
private struct CounterEvent: Event { let value:    Int    }

// MARK: - Test Middleware

private final class AppendMiddleware: EventMiddleware {
    let suffix: String
    init(_ suffix: String) { self.suffix = suffix }
    func process(_ event: any Event) -> (any Event)? {
        guard let e = event as? LoginEvent else { return event }
        return LoginEvent(username: e.username + suffix)
    }
}

/// Drops every event unconditionally.
private final class DropMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? { nil }
}

/// Drops only LoginEvent; lets all other types through.
private final class DropLoginMiddleware: EventMiddleware {
    func process(_ event: any Event) -> (any Event)? {
        event is LoginEvent ? nil : event
    }
}

private final class AsyncAppendMiddleware: AsyncEventMiddleware {
    let suffix: String
    init(_ suffix: String) { self.suffix = suffix }
    func process(_ event: any Event) async -> (any Event)? {
        guard let e = event as? LoginEvent else { return event }
        return LoginEvent(username: e.username + suffix)
    }
}

// MARK: - Helpers

/// Thread-safe box for capturing mutable state inside `@Sendable` closures.
/// Safe because each test drains before reading.
private final class Ref<T: Sendable>: @unchecked Sendable {
    var value: T
    init(_ value: T) { self.value = value }
}

// MARK: - EventBusTests

final class EventBusTests: XCTestCase {

    /// `nonisolated(unsafe)` allows the synchronous nonisolated setUp/tearDown
    /// to mutate these without actor isolation errors.
    nonisolated(unsafe) private var bus: EventBus!
    nonisolated(unsafe) private var cancellables: Set<AnyCancellable>!

    override func setUp() {
        super.setUp()
        bus = EventBus()
        cancellables = []
    }

    override func tearDown() {
        cancellables = nil
        bus = nil
        super.tearDown()
    }

    /// Sleeps briefly so enqueued actor Tasks / onTermination closures can run.
    private func drain() async {
        try? await Task.sleep(nanoseconds: 100_000_000) // 100 ms
    }

    // MARK: - on / off / publish

    func testOnHandlerReceivesEvent() async {
        let exp = expectation(description: "handler called")
        let received = Ref<LoginEvent?>(nil)

        await bus.on(LoginEvent.self) { event in
            received.value = event
            exp.fulfill()
        }
        await bus.publish(LoginEvent(username: "Alice"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.username, "Alice")
    }

    func testOnReturnsDeterministicIdWhenSupplied() async {
        let fixed = UUID()
        let returned = await bus.on(LoginEvent.self, id: fixed) { _ in }
        XCTAssertEqual(returned, fixed)
    }

    func testOffDeregistersHandler() async {
        let count = Ref<Int>(0)
        let id = await bus.on(LoginEvent.self) { _ in count.value += 1 }

        await bus.publish(LoginEvent(username: "first"))
        await drain()
        XCTAssertEqual(count.value, 1)

        await bus.off(LoginEvent.self, id: id)
        await bus.publish(LoginEvent(username: "second"))
        await drain()
        XCTAssertEqual(count.value, 1, "handler must not fire after off()")
    }

    func testMultipleHandlersAllReceiveEvent() async {
        let exp1 = expectation(description: "handler 1")
        let exp2 = expectation(description: "handler 2")
        await bus.on(LoginEvent.self) { _ in exp1.fulfill() }
        await bus.on(LoginEvent.self) { _ in exp2.fulfill() }
        await bus.publish(LoginEvent(username: "Alice"))
        await fulfillment(of: [exp1, exp2], timeout: 1)
    }

    func testHandlerIgnoresDifferentEventType() async {
        let logoutCalled = Ref<Bool>(false)
        await bus.on(LogoutEvent.self) { _ in logoutCalled.value = true }
        await bus.publish(LoginEvent(username: "Alice"))
        await drain()
        XCTAssertFalse(logoutCalled.value)
    }

    func testHandlerReceivesCorrectPayload() async {
        let exp = expectation(description: "received")
        let received = Ref<CounterEvent?>(nil)
        await bus.on(CounterEvent.self) {
            received.value = $0
            exp.fulfill()
        }
        await bus.publish(CounterEvent(value: 42))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.value, 42)
    }

    // MARK: - Middleware

    func testEventLoggerPassesEventThrough() async {
        await bus.use(EventLogger())
        let exp = expectation(description: "received after logger")
        await bus.on(LoginEvent.self) { _ in exp.fulfill() }
        await bus.publish(LoginEvent(username: "test"))
        await fulfillment(of: [exp], timeout: 1)
    }

    func testMiddlewareCanMutateEvent() async {
        await bus.use(AppendMiddleware("_v2"))
        let exp = expectation(description: "received")
        let received = Ref<LoginEvent?>(nil)
        await bus.on(LoginEvent.self) {
            received.value = $0
            exp.fulfill()
        }
        await bus.publish(LoginEvent(username: "Alice"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.username, "Alice_v2")
    }

    func testMiddlewareCanDropEvent() async {
        await bus.use(DropMiddleware())
        let called = Ref<Bool>(false)
        await bus.on(LoginEvent.self) { _ in called.value = true }
        await bus.publish(LoginEvent(username: "Alice"))
        await drain()
        XCTAssertFalse(called.value)
    }

    func testMiddlewareChainAppliedInOrder() async {
        await bus.use(AppendMiddleware("_A"))
        await bus.use(AppendMiddleware("_B"))
        let exp = expectation(description: "received")
        let received = Ref<LoginEvent?>(nil)
        await bus.on(LoginEvent.self) {
            received.value = $0
            exp.fulfill()
        }
        await bus.publish(LoginEvent(username: "base"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.username, "base_A_B")
    }

    func testAsyncMiddlewareCanMutateEvent() async {
        await bus.use(AsyncAppendMiddleware("_async"))
        let exp = expectation(description: "received")
        let received = Ref<LoginEvent?>(nil)
        await bus.on(LoginEvent.self) {
            received.value = $0
            exp.fulfill()
        }
        await bus.publish(LoginEvent(username: "Alice"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.username, "Alice_async")
    }

    func testDropMiddlewareDoesNotAffectOtherEventTypes() async {
        // DropLoginMiddleware drops LoginEvent but passes CounterEvent through.
        await bus.use(DropLoginMiddleware())

        let loginCalled = Ref<Bool>(false)
        await bus.on(LoginEvent.self) { _ in loginCalled.value = true }

        let exp = expectation(description: "counter received despite login-drop middleware")
        await bus.on(CounterEvent.self) { _ in exp.fulfill() }

        await bus.publish(LoginEvent(username: "dropped"))
        await bus.publish(CounterEvent(value: 1))

        await fulfillment(of: [exp], timeout: 1)
        XCTAssertFalse(loginCalled.value, "LoginEvent must have been dropped by middleware")
    }

    func testRemoveMiddleware() async {
        let middleware = AppendMiddleware("_v2")
        await bus.use(middleware)
        await bus.remove(middleware)

        let exp = expectation(description: "received original event")
        let received = Ref<LoginEvent?>(nil)
        await bus.on(LoginEvent.self) {
            received.value = $0
            exp.fulfill()
        }
        await bus.publish(LoginEvent(username: "Alice"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.username, "Alice")
    }

    func testRemoveAllMiddlewares() async {
        await bus.use(AppendMiddleware("_A"))
        await bus.use(AsyncAppendMiddleware("_B"))
        await bus.removeAllMiddlewares()

        let exp = expectation(description: "received original event")
        let received = Ref<LoginEvent?>(nil)
        await bus.on(LoginEvent.self) {
            received.value = $0
            exp.fulfill()
        }
        await bus.publish(LoginEvent(username: "Alice"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.username, "Alice")
    }

    // MARK: - Combine

    func testCombineSubscriberReceivesEvent() async {
        let exp = expectation(description: "combine received")
        let received = Ref<LoginEvent?>(nil)
        await bus.subscribe(LoginEvent.self)
            .sink {
                received.value = $0
                exp.fulfill()
            }
            .store(in: &cancellables)
        await bus.publish(LoginEvent(username: "Alice"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.username, "Alice")
    }

    func testCombineSubscriberIgnoresDifferentType() async {
        let logoutReceived = Ref<Bool>(false)
        await bus.subscribe(LogoutEvent.self)
            .sink { _ in logoutReceived.value = true }
            .store(in: &cancellables)
        await bus.publish(LoginEvent(username: "Alice"))
        await drain()
        XCTAssertFalse(logoutReceived.value)
    }

    func testMultipleCombineSubscribersAllReceive() async {
        let exp1 = expectation(description: "sub 1")
        let exp2 = expectation(description: "sub 2")
        await bus.subscribe(CounterEvent.self)
            .sink { _ in exp1.fulfill() }
            .store(in: &cancellables)
        await bus.subscribe(CounterEvent.self)
            .sink { _ in exp2.fulfill() }
            .store(in: &cancellables)
        await bus.publish(CounterEvent(value: 7))
        await fulfillment(of: [exp1, exp2], timeout: 1)
    }

    func testCombineMiddlewareMutationIsReflected() async {
        await bus.use(AppendMiddleware("_patched"))
        let exp = expectation(description: "combine got patched event")
        let received = Ref<LoginEvent?>(nil)
        await bus.subscribe(LoginEvent.self)
            .sink {
                received.value = $0
                exp.fulfill()
            }
            .store(in: &cancellables)
        await bus.publish(LoginEvent(username: "raw"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(received.value?.username, "raw_patched")
    }

    // MARK: - next()

    func testNextReturnsFirstMatchingEvent() async {
        let b = bus!
        let task = Task { try await b.next(LoginEvent.self) }
        await drain()

        await bus.publish(LoginEvent(username: "First"))
        let result = try! await task.value
        XCTAssertEqual(result.username, "First")
    }

    func testNextIsOneShot_SecondPublishNotCaptured() async {
        let b = bus!
        let resolved = Ref<Bool>(false)
        let task = Task {
            _ = try await b.next(LoginEvent.self)
            resolved.value = true
        }
        await drain()

        await bus.publish(LoginEvent(username: "First"))
        _ = try? await task.value
        XCTAssertTrue(resolved.value)

        // After next() resolves, a regular on() handler must receive subsequent events.
        let count = Ref<Int>(0)
        let exp = expectation(description: "regular handler receives second event")
        await bus.on(LoginEvent.self) { _ in
            count.value += 1
            exp.fulfill()
        }
        await bus.publish(LoginEvent(username: "Second"))
        await fulfillment(of: [exp], timeout: 1)
        XCTAssertEqual(count.value, 1)
    }

    func testNextDoesNotCrossEventTypes() async {
        let b = bus!
        let resolved = Ref<Bool>(false)
        let task = Task {
            _ = try await b.next(LoginEvent.self)
            resolved.value = true
        }
        await drain()

        await bus.publish(LogoutEvent(userId: 99))  // wrong type
        await drain()
        XCTAssertFalse(resolved.value, "next(LoginEvent) must not resolve on LogoutEvent")

        await bus.publish(LoginEvent(username: "Alice"))  // correct type
        _ = try? await task.value
        XCTAssertTrue(resolved.value)
    }

    func testMultipleConcurrentNextCallsAllReceiveSameEvent() async {
        let b = bus!
        let t1 = Task { try await b.next(LoginEvent.self) }
        let t2 = Task { try await b.next(LoginEvent.self) }
        await drain()

        await bus.publish(LoginEvent(username: "Broadcast"))
        let r1 = try! await t1.value
        let r2 = try! await t2.value
        XCTAssertEqual(r1.username, "Broadcast")
        XCTAssertEqual(r2.username, "Broadcast")
    }

    func testNextThrowsOnCancellation() async {
        let b = bus!
        let task = Task { try await b.next(LoginEvent.self) }
        await drain()

        task.cancel()
        do {
            _ = try await task.value
            XCTFail("expected cancellation")
        } catch is CancellationError {
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    func testNextWithMiddlewareReceivesMutatedEvent() async {
        let b = bus!
        await bus.use(AppendMiddleware("_mw"))
        let task = Task { try await b.next(LoginEvent.self) }
        await drain()

        await bus.publish(LoginEvent(username: "raw"))
        let result = try! await task.value
        XCTAssertEqual(result.username, "raw_mw")
    }

    // MARK: - stream()

    func testStreamReceivesMultipleEventsInOrder() async {
        let b = bus!
        let exp = expectation(description: "received 3 events")
        let values = Ref<[Int]>([])

        let task = Task {
            for await event in await b.stream(CounterEvent.self) {
                values.value.append(event.value)
                if values.value.count == 3 { exp.fulfill() }
            }
        }
        await drain()

        await bus.publish(CounterEvent(value: 1))
        await bus.publish(CounterEvent(value: 2))
        await bus.publish(CounterEvent(value: 3))

        await fulfillment(of: [exp], timeout: 2)
        task.cancel()
        XCTAssertEqual(values.value, [1, 2, 3])
    }

    func testStreamIgnoresDifferentEventType() async {
        let b = bus!
        let received = Ref<[Int]>([])
        let task = Task {
            for await event in await b.stream(CounterEvent.self) {
                received.value.append(event.value)
            }
        }
        await drain()

        await bus.publish(LoginEvent(username: "irrelevant"))
        await bus.publish(CounterEvent(value: 99))
        await drain()

        task.cancel()
        XCTAssertEqual(received.value, [99])
    }

    func testStreamCancelsCleanlyWithoutCrash() async {
        let b = bus!
        let task = Task {
            for await _ in await b.stream(CounterEvent.self) {}
        }
        await drain()
        task.cancel()
        await drain()  // let onTermination Task complete

        // Publish after stream is gone — must not crash
        await bus.publish(CounterEvent(value: 0))
    }

    func testMultipleStreamsForSameTypeAllReceive() async {
        let b = bus!
        let exp1 = expectation(description: "stream 1")
        let exp2 = expectation(description: "stream 2")
        let exp3 = expectation(description: "stream 3")

        let t1 = Task {
            for await e in await b.stream(CounterEvent.self) {
                if e.value == 55 { exp1.fulfill() }
            }
        }
        let t2 = Task {
            for await e in await b.stream(CounterEvent.self) {
                if e.value == 55 { exp2.fulfill() }
            }
        }
        let t3 = Task {
            for await e in await b.stream(CounterEvent.self) {
                if e.value == 55 { exp3.fulfill() }
            }
        }
        await drain()

        await bus.publish(CounterEvent(value: 55))
        await fulfillment(of: [exp1, exp2, exp3], timeout: 2)
        t1.cancel(); t2.cancel(); t3.cancel()
    }

    /// Regression for Bug 4: after one stream terminates, remaining streams
    /// must still receive events (StreamEntry avoids dictionary type-cast corruption).
    func testAfterOneStreamCancelsOtherStreamStillReceives() async {
        let b = bus!
        let expStream2 = expectation(description: "stream2 received event after stream1 cancelled")

        let task1 = Task {
            for await _ in await b.stream(CounterEvent.self) { break }
        }
        let task2 = Task {
            for await event in await b.stream(CounterEvent.self) {
                if event.value == 77 { expStream2.fulfill() }
            }
        }
        await drain()

        task1.cancel()
        await drain()  // wait for onTermination cleanup Task

        await bus.publish(CounterEvent(value: 77))
        await fulfillment(of: [expStream2], timeout: 2)
        task2.cancel()
    }

    func testStreamWithMiddlewareReceivesMutatedEvent() async {
        let b = bus!
        await bus.use(AppendMiddleware("_mw"))
        let exp = expectation(description: "stream got mutated event")
        let received = Ref<String?>(nil)
        let task = Task {
            for await event in await b.stream(LoginEvent.self) {
                received.value = event.username
                exp.fulfill()
            }
        }
        await drain()

        await bus.publish(LoginEvent(username: "raw"))
        await fulfillment(of: [exp], timeout: 1)
        task.cancel()
        XCTAssertEqual(received.value, "raw_mw")
    }

    // MARK: - reset()

    func testResetClearsClosureHandlers() async {
        let count = Ref<Int>(0)
        await bus.on(LoginEvent.self) { _ in count.value += 1 }

        await bus.reset()
        await bus.publish(LoginEvent(username: "post-reset"))
        await drain()

        XCTAssertEqual(count.value, 0, "handler must not fire after reset()")
    }

    func testResetFinishesActiveStreams() async {
        let b = bus!
        let exp = expectation(description: "for-await loop exited")
        let loopExited = Ref<Bool>(false)

        let task = Task {
            for await _ in await b.stream(LoginEvent.self) {}
            loopExited.value = true
            exp.fulfill()
        }
        await drain()

        await bus.reset()
        await fulfillment(of: [exp], timeout: 2)
        XCTAssertTrue(loopExited.value)
        task.cancel()
    }

    func testResetClearsOneshotHandlers() async {
        // reset() clears oneshotHandlers so a pending next() call will never
        // resolve (non-throwing API limitation). The runtime may emit a "leaked
        // continuation" diagnostic for this test — that is expected and acceptable.
        let b = bus!
        let nextResolved = Ref<Bool>(false)
        let task = Task {
            do {
                _ = try await b.next(LoginEvent.self)
                nextResolved.value = true
            } catch {}
        }
        await drain()

        await bus.reset()

        // Post-reset publish must NOT resolve the stale next() handler.
        let count = Ref<Int>(0)
        await bus.on(LoginEvent.self) { _ in count.value += 1 }
        await bus.publish(LoginEvent(username: "after-reset"))
        await drain()

        XCTAssertFalse(nextResolved.value, "next() from before reset must not resolve")
        XCTAssertEqual(count.value, 1, "freshly registered on() handler must still work")
        task.cancel()
    }

    func testAfterResetCanRegisterNewHandlers() async {
        await bus.reset()
        let exp = expectation(description: "new handler works after reset")
        await bus.on(CounterEvent.self) { event in
            if event.value == 1 { exp.fulfill() }
        }
        await bus.publish(CounterEvent(value: 1))
        await fulfillment(of: [exp], timeout: 1)
    }

    func testAfterResetNewStreamReceivesEvents() async {
        let b = bus!
        await bus.reset()

        let exp = expectation(description: "stream after reset receives event")
        let task = Task {
            for await event in await b.stream(CounterEvent.self) {
                if event.value == 5 { exp.fulfill() }
            }
        }
        await drain()

        await bus.publish(CounterEvent(value: 5))
        await fulfillment(of: [exp], timeout: 2)
        task.cancel()
    }

    func testMultipleResetsDoNotCrash() async {
        let b = bus!
        await bus.on(LoginEvent.self) { _ in }
        let task = Task {
            for await _ in await b.stream(LoginEvent.self) {}
        }
        await drain()

        await bus.reset()
        await bus.reset()  // second reset on empty state must not crash
        await drain()
        task.cancel()
    }

    // MARK: - Cross-type isolation

    func testTwoEventTypesDoNotInterfere() async {
        let loginExp  = expectation(description: "login handler")
        let logoutExp = expectation(description: "logout handler")
        let loginCount  = Ref<Int>(0)
        let logoutCount = Ref<Int>(0)

        await bus.on(LoginEvent.self)  { _ in loginCount.value  += 1; loginExp.fulfill()  }
        await bus.on(LogoutEvent.self) { _ in logoutCount.value += 1; logoutExp.fulfill() }

        await bus.publish(LoginEvent(username: "Alice"))
        await bus.publish(LogoutEvent(userId: 1))

        await fulfillment(of: [loginExp, logoutExp], timeout: 1)
        XCTAssertEqual(loginCount.value,  1)
        XCTAssertEqual(logoutCount.value, 1)
    }

    func testResetClearsAllEventTypes() async {
        let loginCount   = Ref<Int>(0)
        let counterCount = Ref<Int>(0)
        await bus.on(LoginEvent.self)   { _ in loginCount.value   += 1 }
        await bus.on(CounterEvent.self) { _ in counterCount.value += 1 }

        await bus.publish(LoginEvent(username: "A"))
        await bus.publish(CounterEvent(value: 1))
        await drain()

        await bus.reset()

        await bus.publish(LoginEvent(username: "B"))
        await bus.publish(CounterEvent(value: 2))
        await drain()
        XCTAssertEqual(loginCount.value,   1, "handlers cleared by reset")
        XCTAssertEqual(counterCount.value, 1, "handlers cleared by reset")
    }

    func testUnsubscribeAllForType() async {
        let count = Ref<Int>(0)
        await bus.on(LoginEvent.self) { _ in count.value += 1 }
        await bus.on(LoginEvent.self) { _ in count.value += 1 }
        await bus.unsubscribeAll(for: LoginEvent.self)
        await bus.publish(LoginEvent(username: "Alice"))
        await drain()
        XCTAssertEqual(count.value, 0)
    }

    func testOnLimitAutomaticallyUnsubscribes() async {
        let count = Ref<Int>(0)
        await bus.on(CounterEvent.self, limit: 2) { _ in count.value += 1 }
        await bus.publish(CounterEvent(value: 1))
        await bus.publish(CounterEvent(value: 2))
        await bus.publish(CounterEvent(value: 3))
        await drain()
        XCTAssertEqual(count.value, 2)
    }

    func testPriorityHandlersRunInOrder() async {
        let order = Ref<[String]>([])
        await bus.on(LoginEvent.self, priority: .low) { _ in order.value.append("low") }
        await bus.on(LoginEvent.self, priority: .high) { _ in order.value.append("high") }
        await bus.on(LoginEvent.self) { _ in order.value.append("normal") }
        await bus.publish(LoginEvent(username: "Alice"))
        await drain()
        XCTAssertEqual(order.value, ["high", "normal", "low"])
    }

    func testWaitForAnyReturnsMatchingBranch() async throws {
        let b = bus!
        let task = Task { try await b.waitForAny(LoginEvent.self, LogoutEvent.self) }
        await drain()
        await bus.publish(LogoutEvent(userId: 5))
        let result = try await task.value
        XCTAssertNil(result.0)
        XCTAssertEqual(result.1?.userId, 5)
    }

    func testWaitForAllCollectsBothEvents() async throws {
        let b = bus!
        let task = Task {
            try await b.waitForAll(LoginEvent.self, LogoutEvent.self, timeout: .seconds(1))
        }
        await drain()
        await bus.publish(LoginEvent(username: "Alice"))
        await bus.publish(LogoutEvent(userId: 9))
        let result = try await task.value
        XCTAssertEqual(result.0.username, "Alice")
        XCTAssertEqual(result.1.userId, 9)
    }

    func testWaitForAllThrowsTimeout() async {
        let b = bus!
        do {
            _ = try await b.waitForAll(
                LoginEvent.self,
                LogoutEvent.self,
                timeout: .milliseconds(50)
            )
            XCTFail("expected timeout")
        } catch is EventBusTimeoutError {
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    func testStreamReplayReturnsLastEvents() async {
        let b = bus!
        await bus.publish(CounterEvent(value: 1))
        await bus.publish(CounterEvent(value: 2))
        await bus.publish(CounterEvent(value: 3))

        let values = Ref<[Int]>([])
        let exp = expectation(description: "replay values")
        let task = Task {
            for await event in await b.stream(CounterEvent.self, replay: 2) {
                values.value.append(event.value)
                if values.value.count == 2 { exp.fulfill() }
            }
        }

        await fulfillment(of: [exp], timeout: 1)
        task.cancel()
        XCTAssertEqual(values.value, [2, 3])
    }

    func testMetricsTrackPublishedAndDropped() async {
        await bus.use(DropMiddleware())
        await bus.publish(LoginEvent(username: "dropped"))
        let metrics = await bus.metrics
        XCTAssertEqual(metrics.totalPublished, 1)
        XCTAssertEqual(metrics.totalDroppedByMiddleware, 1)
    }

    func testActionUsesCorrectBus() async {
        let customBus = EventBus()
        let count = Ref<Int>(0)
        await customBus.on(LoginEvent.self) { _ in count.value += 1 }

        let action = customBus.action { LoginEvent(username: "Alice") }
        action()
        await drain()

        XCTAssertEqual(count.value, 1)
    }
}
