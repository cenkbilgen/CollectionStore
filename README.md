Simple persistence store, when keeping a Collection of value types is all you need.


```swift
// make a store of your value type. Anything `Codable` and `Equatable`.
struct Dog: Codable, Equatable {
  let name: String
  let age: Int
}

let store = SQLiteStore<Dog>(name: "Dogs")
```

```swift
// store values
try await store.insert(items: [Dog(name: "Fido", age: 10),                                         
                               Dog(name: "Rex", age: 5)])
```

```swift 
// retrieve values
let dogs = try await store.query()
```

----

To "INSERT OR REPLACE", make the record type `Identifiable` and use `SQLiteIndentifiableStore`.  Primary key will be the `id`.
