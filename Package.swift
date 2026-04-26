// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "EventBus",
    platforms: [
        .iOS(.v17),
        .macOS(.v15),
        .tvOS(.v17),
        .watchOS(.v10),
        .visionOS(.v1)
    ],
    products: [
        .library(
            name: "EventBus",
            targets: ["EventBus"]
        ),
    ],
    targets: [
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
