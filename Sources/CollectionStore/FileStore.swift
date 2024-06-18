//
//  File.swift
//  
//
//  Created by Cenk Bilgen on 2024-06-18.
//

import Foundation

public actor FileStore<I: Codable & Equatable>: CollectionStore {
    public let name: String
    private let fileURL: URL
    private var collection: [I]?
    private var isSaved: Bool

    public init(name: String) throws {
        self.name = name
        self.fileURL = URL.applicationSupportDirectory
            .appending(path: "\(name)-store.json", directoryHint: .notDirectory)

        let data = try Data(contentsOf: fileURL)
        do {
            let collection = try JSONDecoder().decode([I].self, from: data)
            self.collection = collection
        } catch {
            print(error.localizedDescription)
            self.collection = []
        }
        self.isSaved = true
    }

    public func query() async throws -> [I] {
       collection ?? []
    }

    public func insert(item: I) async throws {
        collection?.append(item)
        isSaved = false
    }

    public func remove(item: I) async throws {
        guard let index = collection?.firstIndex(where: {
            $0 == item
        }) else {
            return
        }
        collection?.remove(at: index)
        isSaved = false
    }

    // NOTE: Call save manually when appropriate
    public func save() throws {
        guard let collection else {
            return
        }
        guard !isSaved else {
            print("No changes to save.")
            return
        }
        let data = try JSONEncoder().encode(collection)
        try data.write(to: fileURL)
        isSaved = true
    }
}
