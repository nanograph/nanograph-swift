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
            url: "https://github.com/nanograph/nanograph/releases/download/v1.3.0/NanoGraphFFI.xcframework.zip",
            checksum: "d155bbe517a74c97dcc00ee5511a2e649195875bf52205af10b0b8ed71fa728f"
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
