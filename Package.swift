// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "NanoGraph",
    platforms: [
        .macOS(.v13),
    ],
    products: [
        .library(
            name: "NanoGraph",
            targets: ["NanoGraph"]
        ),
    ],
    targets: [
        .binaryTarget(
            name: "NanoGraphFFI",
            url: "https://github.com/nanograph/nanograph/releases/download/v0.9.1/NanoGraphFFI.xcframework.zip",
            checksum: "ffb1c18ceb0c43cd393866fc767cb8d35a126e7be2c254ac70e33cc37f20957e"
        ),
        .target(
            name: "CNanoGraph",
            path: "Sources/CNanoGraph",
            publicHeadersPath: "include"
        ),
        .target(
            name: "NanoGraph",
            dependencies: ["CNanoGraph", "NanoGraphFFI"],
            path: "Sources/NanoGraph"
        ),
        .testTarget(
            name: "NanoGraphTests",
            dependencies: ["NanoGraph"],
            path: "Tests/NanoGraphTests"
        ),
    ]
)
