Simple persistence store, when keeping a Collection of value types is all you need.

```swift
struct Dog: Codable, Equatable {
  let name: String
  let age: Int
}
```
```swift
let store = SQLiteStore<Dog>(name: "Dogs")

try await store.insert(items: [Dog(name: "Fido", age: 10),                                         
                               Dog(name: "Rex", age: 5)])

// ... retrieve values

let dogs = try await store.query()
```
