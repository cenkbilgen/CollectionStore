//
//  File.swift
//
//
//  Created by Cenk Bilgen on 2024-06-18.
//

import Foundation
import FMDB
import SQLite3

public actor SQLiteStore<I: Codable & Equatable>: CollectionStore {
    public let name: String
    // private let dbQueue: FMDatabaseQueue
    private let db: FMDatabase

//    private let insertStatement: FMStatement?
//    private let deleteStatement: FMStatement?

    let encoder: JSONEncoder
    let decoder: JSONDecoder

    public init(name: String, databaseURL: URL) {
        self.name = name
        do {
            try FileManager.default.createDirectory(at: databaseURL.deletingLastPathComponent(), withIntermediateDirectories: true, attributes: nil)
        } catch {
            print("Failed to create directory: \(error.localizedDescription)")
        }

        self.db = FMDatabase(url: databaseURL)
        // self.dbQueue = FMDatabaseQueue(url: databaseURL)!
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        let isOpen = self.db.open(withFlags: flags)
        print("\(name) is open \(isOpen)")

//        if I.self is (any Identifiable) {
//            db.executeStatements("CREATE TABLE IF NOT EXISTS \(name) (id STRING PRIMARY KEY, data BLOB)")
//        } else {
//            db.executeStatements("CREATE TABLE IF NOT EXISTS \(name) (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB)")
        let createTableQuery = """
    CREATE TABLE IF NOT EXISTS \(name) (
        data BLOB UNIQUE
    );
    """
        db.executeStatements(createTableQuery)
//        }
        //id INTEGER PRIMARY KEY AUTOINCREMENT,
//        self.insertStatement = try? db.prepare("INSERT INTO \(name) (data) VALUES (?)").statement
//        self.deleteStatement = try? db.prepare("DELETE FROM \(name) WHERE data = ?").statement
        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
    }

    public init(name: String) {
        let fileURL = URL.applicationSupportDirectory
            .appending(path: "\(name)-store.sql", directoryHint: .notDirectory)
        self.init(name: name, databaseURL: fileURL)
    }
    
    deinit {
        db.close()
    }

    public func insert(item: I) async throws {
        let data = try encoder.encode(item)
        try db.executeUpdate("INSERT OR IGNORE INTO \(name) (data) VALUES (?)", values: [data])
    }

    public func insert<C: Collection<I>>(items: C) async throws {
        try db.beginTransaction()
        do {
            for item in items {
                let data = try encoder.encode(item)
                try db.executeUpdate("INSERT OR IGNORE INTO \(name) (data) VALUES (?)", values: [data])
            }
            try db.commit()
        } catch {
            try db.rollback()
            throw error
        }
    }

    public func remove(item: I) async throws {
        let data = try encoder.encode(item)
        try db.executeUpdate("DELETE FROM \(name) WHERE data = ?", values: [data])
    }
    
    public func queryStream(bufferSize: Int? = nil, continueOnRecordFail: Bool = true) throws -> AsyncStream<I> {
        let (stream, continuation) = AsyncStream.makeStream(of: I.self, bufferingPolicy: bufferSize == nil ? .unbounded : .bufferingNewest(bufferSize!))
        let resultSet = try db.executeQuery("SELECT data FROM \(name)", values: nil)

        defer {
            resultSet.close()
        }

        Task {
            repeat {
                guard !Task.isCancelled else {
                    break
                }
                if let data = resultSet.data(forColumnIndex: 0) {
                    do {
                        let item = try decoder.decode(I.self, from: data)
                        continuation.yield(item)
                    } catch {
                        print("\(name) decode error: \(error)")
                        if !continueOnRecordFail {
                            continuation.finish()
                            break
                        }
                    }
                }
            } while resultSet.next()
            continuation.finish()
        }
        return stream
    }


    public func query() async throws -> [I] {
        var collection: [I] = []
        let stream = try queryStream(continueOnRecordFail: true)
        for await item in stream {
            collection.append(item)
        }
        return collection
    }

}

struct FMDBError: Error {
    let message: String
}
