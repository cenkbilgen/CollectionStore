//
//  File.swift
//  
//
//  Created by Cenk Bilgen on 2024-06-19.
//

import SwiftUI
import CollectionStore
import Observation

@Observable
class Model {
    let storage: SQLiteStore<Int>

    init(name: String) {
        let storage = SQLiteStore<Int>(name: name)
        self.storage = storage

        Task { [storage] in
            values = try await storage.query()
        }
    }

    var values: [Int] = []

    func add(_ i: Int) {
        values.append(i)
        Task {
            try await storage.insert(item: i)
        }
    }

    func delete(_ i: Int) {
        values.removeAll { n in
            n == i
        }
        Task {
            try await storage.remove(item: i)
        }
    }
}

struct ContentView: View {
    @State var model = Model(name: "favourites1")
    var body: some View {
        VStack {
            List(model.values.indices, id: \.self) { index in
                Text(model.values[index].formatted())
                    .contentShape(Rectangle())
                    .onTapGesture {
                        model.delete(model.values[index])
                    }
            }
            Button("Add") {
                model.add(Int.random(in: 10..<99))
            }
            .buttonBorderShape(.capsule)
            .frame(maxWidth: .infinity)
        }
        .padding()
    }
}

#Preview {
    ContentView()
}
