//
//  File.swift
//  
//
//  Created by Cenk Bilgen on 2024-07-26.
//

import Foundation
import FMDB
import SQLite3

// TODO: Make his SQLite based stores have their own protocol

public actor SQLiteJSONStore<I: Codable & Equatable>: CollectionStore {
    
    public let name: String
    private let db: FMDatabase

    let encoder: JSONEncoder
    let decoder: JSONDecoder
    
    public var isNew: Bool
    
    public init(name: String, databaseURL: URL) {
        self.name = name
        do {
            try FileManager.default.createDirectory(at: databaseURL.deletingLastPathComponent(), withIntermediateDirectories: true, attributes: nil)
        } catch {
            print("Failed to create directory: \(error.localizedDescription)")
        }

        self.db = FMDatabase(url: databaseURL)
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        let isOpen = self.db.open(withFlags: flags)
        print("\(name) is open \(isOpen)")
        
        self.isNew = !db.tableExists(name)

        let createTableQuery = """
           CREATE TABLE IF NOT EXISTS \(name) (
               json TEXT
           )
           """
        // for FTS5, then use MATCH instead of LIKE
//        let createTableQuery = """
//               CREATE VIRTUAL TABLE IF NOT EXISTS Person USING fts5(
//                   json,
//                   tokenize = 'porter'
//               );
//               """

        db.executeStatements(createTableQuery)

        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
    }

    public init(name: String) {
        let fileURL = URL.applicationSupportDirectory
            .appending(path: "\(name)-json-store.sql", directoryHint: .notDirectory)
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
        isNew = false
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

    public func queryStream(bufferSize: Int? = nil, continueOnRecordFail: Bool = true) throws -> AsyncStream<I> {
        let (stream, continuation) = AsyncStream.makeStream(of: I.self, bufferingPolicy: bufferSize == nil ? .unbounded : .bufferingNewest(bufferSize!))
        let resultSet = try db.executeQuery("SELECT json FROM \(name)", values: nil)

        defer {
            resultSet.close()
        }

        Task {
            repeat {
                guard !Task.isCancelled else {
                    break
                }
                if let jsonString = resultSet.string(forColumn: "json") {
                    do {
                        if let jsonData = jsonString.data(using: .utf8) {
                            let item = try decoder.decode(I.self, from: jsonData)
                            continuation.yield(item)
                        } else {
                            throw CollectionStoreError.jsonStringUTF8Failed
                        }
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

extension SQLiteJSONStore {
    public func modificationDate() throws -> Date? {
        let values = try db.databaseURL?.resourceValues(forKeys: [.contentModificationDateKey])
        return values?.contentModificationDate
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
