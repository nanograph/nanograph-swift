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
            url: "https://github.com/nanograph/nanograph/releases/download/v1.0.0/NanoGraphFFI.xcframework.zip",
            checksum: "453ae2bce4c1a9d3cf607f433bc10b0b34698caca10acf0d3e82ff205eaf2c80"
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
