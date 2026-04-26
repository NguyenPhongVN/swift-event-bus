import SwiftUI

// MARK: - SwiftUI Environment

public extension EnvironmentValues {
    /// The ``EventBus`` injected into this part of the SwiftUI hierarchy.
    ///
    /// Defaults to ``EventBus/shared``. Override with the `.eventBus(_:)` view modifier.
    @Entry var eventBus: EventBus = .shared
}

public extension View {
    /// Injects `bus` into the SwiftUI environment for all descendant views and property wrappers.
    func eventBus(_ bus: EventBus) -> some View {
        environment(\.eventBus, bus)
    }
}
