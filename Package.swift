// swift-tools-version: 5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "CollectionStore",
    platforms: [.iOS(.v16), .macOS(.v13)],
    products: [
        .library(
            name: "CollectionStore",
            targets: ["CollectionStore"]),
    ],
    dependencies: [
        .package(url: "https://github.com/ccgus/fmdb", .upToNextMajor(from: "2.7.8"))
    ],
    targets: [
        .target(
            name: "CollectionStore",
            dependencies: [.product(name: "FMDB", package: "FMDB")]),
    ]
)
