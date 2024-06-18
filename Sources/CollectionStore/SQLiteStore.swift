//
//  File.swift
//
//
//  Created by Cenk Bilgen on 2024-06-18.
//

import Foundation
import FMDB

public actor SQLiteStore<I: Codable & Equatable>: CollectionStore {
    public let name: String
    // private let dbQueue: FMDatabaseQueue
    private let db: FMDatabase

//    private let insertStatement: FMStatement?
//    private let deleteStatement: FMStatement?

    public init(name: String, databaseURL: URL) {
        self.name = name
        self.db = FMDatabase(url: databaseURL)
        // self.dbQueue = FMDatabaseQueue(url: databaseURL)!
        self.db.open()
        db.executeStatements("CREATE TABLE IF NOT EXISTS \(name) (data BLOB)")
//        self.insertStatement = try? db.prepare("INSERT INTO \(name) (data) VALUES (?)").statement
//        self.deleteStatement = try? db.prepare("DELETE FROM \(name) WHERE data = ?").statement
    }

    public init(name: String) {
        let fileURL = URL.applicationSupportDirectory
            .appending(path: "\(name)-store.sql", directoryHint: .notDirectory)
        self.init(name: name, databaseURL: fileURL)
    }

    public func insert(item: I) async throws {
        try db.executeUpdate("INSERT INTO \(name) (data) VALUES (?)", values: [item])
    }

    public func remove(item: I) async throws {
        try db.executeUpdate("DELETE FROM \(name) WHERE data = ?", values: [item])
    }

    public func query() async throws -> [I] {
        let resultSet = try db.executeQuery("SELECT data FROM \(name)", values: nil)
        // just one col
        var collection: [I] = []
        repeat {
            if let data = resultSet.data(forColumnIndex: 0) {
                do {
                    let item = try JSONDecoder().decode(I.self, from: data)
                    collection.append(item)
                } catch {
                    print(error)
                }
            }
        } while resultSet.next()
        return collection
    }
}

struct FMDBError: Error {
    let message: String
}
