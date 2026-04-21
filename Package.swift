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
            url: "https://github.com/nanograph/nanograph/releases/download/v1.2.1/NanoGraphFFI.xcframework.zip",
            checksum: "57b57b76fdc3d2100458b1414ec3343d31ddb1fc8eee71f9940a16adfb963ad9"
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
