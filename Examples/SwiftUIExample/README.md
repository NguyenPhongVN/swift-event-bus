# SwiftUI Example

This folder contains a minimal SwiftUI example app showing how to:

- inject a feature-scoped `EventBus` into the environment
- publish events from UI actions
- observe the latest event with `@EventListener`
- keep cross-screen subscriptions alive with `.onEventLifeCycle`

Files:

- `SwiftUIExampleApp.swift`
- `ContentView.swift`
- `SessionEvents.swift`

The example is intentionally source-only so the package can stay SPM-first.
Create a new Xcode SwiftUI app, add the `EventBus` package, then drop these files in.
