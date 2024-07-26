//
//  File.swift
//  
//
//  Created by Cenk Bilgen on 2024-07-26.
//

import Foundation
import FMDB
import SQLite3

public actor SQLiteJSONStore<I: Codable & Equatable>: CollectionStore {
    
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

        let createTableQuery = """
           CREATE TABLE IF NOT EXISTS \(name) (
               json TEXT
           )
           """
        db.executeStatements(createTableQuery)

        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
    }

    public init(name: String) {
        let fileURL = URL.applicationSupportDirectory
            .appending(path: "\(name)-store-json.sql", directoryHint: .notDirectory)
        self.init(name: name, databaseURL: fileURL)
    }

    func jsonValue(item: I) throws -> String {
        let data = try encoder.encode(item)
        guard let string = String(data: data, encoding: .utf8) else {
            throw CollectionStoreError.jsonStringUTF8Failed
        }
        return string
    }

    public func insert(item: I) throws {
        let value = try jsonValue(item: item)
        try db.executeUpdate("INSERT INTO \(name) (json) VALUES (?)", values: [value])
    }

    public func insert<C: Collection<I>>(items: C) async throws {
        try db.beginTransaction()
        do {
            for item in items {
                try insert(item: item)
            }
            try db.commit()
        } catch {
            try db.rollback()
            throw error
        }
    }

    public func query() async throws -> [I] {
        let resultSet = try db.executeQuery("SELECT json FROM \(name)", values: nil)
        // just one col
        var collection: [I] = []
        repeat {
            if let data = resultSet.data(forColumnIndex: 0) {
                do {
                    let item = try decoder.decode(I.self, from: data)
                    collection.append(item)
                } catch {
                    print("\(name) : \(error)")
                }
            }
        } while resultSet.next()
        return collection
    }

    public func query<I: Codable, V: Codable>(key: KeyPath<I, V>, value: V) throws -> [I] {
        let keyName = NSExpression(forKeyPath: key).keyPath
        let query = """
            SELECT json FROM \(name) WHERE json_extract(json, '$.\(keyName)') = ?
            """

        let resultSet = try db.executeQuery(query, values: [String(describing: value)])
        var records: [I] = []
        while resultSet.next() == true {
            if let jsonString = resultSet.string(forColumn: "json") {
                if let jsonData = jsonString.data(using: .utf8) {
                    let record = try JSONDecoder().decode(I.self, from: jsonData)
                    records.append(record)
                }
            }
        }
        return records
    }

    // TODO: speicify the keyName, just safer and easier
    public func fuzzyQuery<I: Codable>(key: KeyPath<I, String>, keyName: String, pattern: String) throws -> [I] {
        let query = """
            SELECT json FROM \(name) WHERE json_extract(json, '$.\(keyName)') LIKE ?
            """

        let resultSet = try db.executeQuery(query, values: ["%\((pattern).escaped())%"])
        var records: [I] = []
        while resultSet.next() == true {
            if let jsonString = resultSet.string(forColumn: "json") {
                if let jsonData = jsonString.data(using: .utf8) {
                    let record = try JSONDecoder().decode(I.self, from: jsonData)
                    records.append(record)
                }
            }
        }
        return records
    }

    public func remove(item: I) async throws {
        let value = try jsonValue(item: item)
        try db.executeUpdate("DELETE FROM \(name) WHERE json = ?", values: [value])
    }

    public func remove<I: Codable, V: Codable>(key: KeyPath<I, V>, value: V) {
            let keyName = NSExpression(forKeyPath: key).keyPath
            let deleteQuery = """
            DELETE FROM \(name) WHERE json_extract(json, '$.\(keyName)') = ?
            """

            do {
                try db.executeUpdate(deleteQuery, values: [String(describing: value)])
            } catch {
                print("Failed to delete record by key-path and value: \(error.localizedDescription)")
            }
        }

    // MARK: Todo
    
    public func queryStream(bufferSize: Int? = nil) throws -> AsyncStream<I> {
        let (stream, continuation) = AsyncStream.makeStream(of: I.self, bufferingPolicy: bufferSize == nil ? .unbounded : .bufferingNewest(bufferSize!))
        let resultSet = try db.executeQuery("SELECT data FROM \(name)", values: nil)
        // just one col
        Task {
            repeat {
                if let data = resultSet.data(forColumnIndex: 0) {
                    do {
                        let item = try JSONDecoder().decode(I.self, from: data)
                        continuation.yield(item)
                    } catch {
                        print("\(name) : \(error)")
                        continuation.finish()
                    }
                }
            } while resultSet.next()
            continuation.finish()
        }

        return stream
    }

}

// MARK: Utility

// Utility extension to escape SQLite wildcards in patterns
private extension String {
    func escaped() -> String {
        return self
            .replacingOccurrences(of: "%", with: "\\%")
            .replacingOccurrences(of: "_", with: "\\_")
    }
}
