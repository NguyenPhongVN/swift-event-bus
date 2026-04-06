// swift-tools-version: 6.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "EventBus",
    platforms: [
        .iOS(.v17),        // iOS 16.0+ for latest SwiftUI features
        .macOS(.v15),      // macOS 14.0+ for latest system APIs
        .tvOS(.v17),       // tvOS 17.0+ for latest tvOS features
        .watchOS(.v10),    // watchOS 10.0+ for latest watchOS capabilities
        .visionOS(.v1)     // visionOS 1.0+ for visionOS support
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "EventBus",
            targets: ["EventBus"]
        ),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "EventBus"
        ),
        .testTarget(
            name: "EventBusTests",
            dependencies: ["EventBus"]
        ),
    ],
    swiftLanguageModes: [.v6]
)
