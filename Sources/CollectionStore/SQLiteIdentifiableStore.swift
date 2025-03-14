//
//  SQLiteIdentifiableStore.swift
//  CollectionStore
//
//  Created by Cenk Bilgen on 2025-03-13.
//

import Foundation
import FMDB
import SQLite3

public actor SQLiteIdentifiableStore<I: Codable & Equatable & Identifiable>: CollectionStore where I.ID: CustomStringConvertible {
    public let name: String
    private let db: FMDatabase

    let encoder: JSONEncoder
    let decoder: JSONDecoder

    init(name: String, databaseURL: URL) {
        self.name = name
        do {
            try FileManager.default.createDirectory(at: databaseURL.deletingLastPathComponent(), withIntermediateDirectories: true, attributes: nil)
        } catch {
            print("Failed to create directory: \(error.localizedDescription)")
        }

        self.db = FMDatabase(url: databaseURL)
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        let isOpen = self.db.open(withFlags: flags)
        
        if isOpen {
            print("\(name) is open \(isOpen)")
            
            // Check database integrity, Xcode complains. User could have force closed app before deinit and close called
//            let integrityCheck = db.executeQuery("PRAGMA integrity_check;", withArgumentsIn: [])
//            if let integrityCheck = integrityCheck, integrityCheck.next() {
//                let result = integrityCheck.string(forColumnIndex: 0)
//                print("Database integrity check: \(result ?? "unknown")")
//            }
            
            db.executeStatements("PRAGMA journal_mode = DELETE;")
            db.executeStatements("PRAGMA cache_size = -2048;") // Use about 2MB of memory for cache
            db.executeStatements("PRAGMA synchronous = NORMAL;")
            
            let createTableQuery = """
                CREATE TABLE IF NOT EXISTS \(name) (
                    id TEXT PRIMARY KEY,
                    data BLOB
                );
                """
        
            let result = db.executeStatements(createTableQuery)
            if !result {
                print(db.lastError().localizedDescription)
            }
              
//            if !db.executeStatements(createTableQuery) {
//                print("Error creating table: \(db.lastErrorMessage())")
//            }
        } else {
            print("Failed to open database: \(db.lastErrorMessage())")
        }
        
        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
        
    #if DEBUG
        self.diagnoseTable()
    #endif
    }

    public init(name: String) {
        let fileURL = URL.applicationSupportDirectory
            .appending(path: "\(name)-identifiable-store.sql", directoryHint: .notDirectory)
        self.init(name: name, databaseURL: fileURL)
    }
    
    deinit {
        db.close()
    }

    public func insert(item: I) async throws {
        let data = try encoder.encode(item)
        let idString = item.id.description
        try db.executeUpdate(
            "INSERT OR REPLACE INTO \(name) (id, data) VALUES (?, ?)",
            values: [idString as NSString, data as NSData]
        )
    }

    public func insert<C: Collection<I>>(items: C) async throws {
        try db.beginTransaction()
        do {
            for item in items {
                try await insert(item: item)
            }
            try db.commit()
        } catch {
            try db.rollback()
            throw error
        }
    }

    public func remove(item: I) async throws {
        let idString = item.id.description
        try db.executeUpdate("DELETE FROM \(name) WHERE id = ?", values: [idString])
    }

    public func removeById(id: I.ID) async throws {
        let idString = id.description
        try db.executeUpdate("DELETE FROM \(name) WHERE id = ?", values: [idString])
    }

    /** NOTE:This will not work. FMDB ResultSet is not thread-safe and although the columnCount is 1 as expected. It becomes zero in the Task closure.
    
     public func queryStreamBAD(bufferSize: Int? = nil, continueOnRecordFail: Bool = true) throws -> AsyncStream<I> {
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

                if let data = resultSet.data(forColumnIndex: 0) { // mem error
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
     **/
    
    public func queryStream(bufferSize: Int? = nil, continueOnRecordFail: Bool = true) throws -> AsyncStream<I> {
        let (stream, continuation) = AsyncStream.makeStream(of: I.self, bufferingPolicy: bufferSize == nil ? .unbounded : .bufferingNewest(bufferSize!))
        var allResults: [Data] = []
        let results = try db.executeQuery("SELECT data FROM \(name)", values: nil)
        print("Initial columnCount: \(results.columnCount)")

        // we need to save this while on this thread, hence the awkwardness
        while results.next() {
            if let data = results.data(forColumnIndex: 0) {
                allResults.append(data)
            }
        }
        results.close()
        print("\(name) loaded \(allResults.count) records.")

        // Now process the extracted data in the Task
        Task {
            for data in allResults {
                guard !Task.isCancelled else {
                    break
                }
                do {
                    let item = try decoder.decode(I.self, from: data)
                    continuation.yield(item)
                } catch {
                    print("\(name) decode error: \(error)")

                    if !continueOnRecordFail {
                        break
                    }
                }
            }
            continuation.finish()
        }
        return stream
    }
    
    public func query() async throws -> [I] {
        var collection: [I] = []
        let resultSet = try db.executeQuery("SELECT data FROM \(name)", values: nil)
        while resultSet.next() {
            if let data = resultSet.data(forColumnIndex: 0) {
                do {
                    let item = try decoder.decode(I.self, from: data)
                    collection.append(item)
                } catch {
                    print("\(name) decode error: \(error)")
                }
            }
        }
        resultSet.close()
        return collection
    }

    public func queryOLD() async throws -> [I] {
        var collection: [I] = []
        let stream = try queryStream(continueOnRecordFail: true)
        for await item in stream {
            collection.append(item)
        }

        return collection
    }

    public func getById(id: I.ID) async throws -> I? {
        let idString = id.description
        let resultSet = try db.executeQuery("SELECT data FROM \(name) WHERE id = ?", values: [idString])

        defer {
            resultSet.close()
        }

        if resultSet.next(), let data = resultSet.data(forColumnIndex: 0) {
            return try decoder.decode(I.self, from: data)
        }

        return nil
    }

    public func exists(id: I.ID) async throws -> Bool {
        let idString = id.description
        let resultSet = try db.executeQuery("SELECT 1 FROM \(name) WHERE id = ? LIMIT 1", values: [idString])

        defer {
            resultSet.close()
        }

        return resultSet.next()
    }
    
    
    public func diagnoseTable() {
        print("Diagnosing table \(name)...")

        // Check if table exists
        let tableExists = db.tableExists(name)
        print("Table exists: \(tableExists)")

        if tableExists {
            // Get table info
            if let tableInfo = try? db.executeQuery("PRAGMA table_info(\(name))", values: nil) {
                print("Table columns:")
                while tableInfo.next() {
                    let colName = tableInfo.string(forColumn: "name") ?? "unknown"
                    let colType = tableInfo.string(forColumn: "type") ?? "unknown"
                    print("  - \(colName) (\(colType))")
                }
                tableInfo.close()
            }

            // Count records
            if let countResult = try? db.executeQuery("SELECT COUNT(*) FROM \(name)", values: nil) {
                if countResult.next() {
                    let count = countResult.int(forColumnIndex: 0)
                    print("Record count: \(count)")
                }
                countResult.close()
            }

            // Check for sample record
            if let sampleResult = try? db.executeQuery("SELECT * FROM \(name) LIMIT 1", values: nil) {
                print("Sample record columns: \(sampleResult.columnCount)")
                let columnNames = (sampleResult.columnNameToIndexMap.allKeys as? [String]) ?? []
                if sampleResult.next() {
                    print("  Column names: \(columnNames.joined(separator: ", "))")
                } else {
                    print("  No records found")
                }
                sampleResult.close()
            }
        }
    }
}
