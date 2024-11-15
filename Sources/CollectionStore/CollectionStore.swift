import Foundation
import Observation

protocol CollectionStore {
    associatedtype C: RandomAccessCollection
    var name: String { get }
    func insert(item: C.Element) async throws
    func remove(item: C.Element) async throws
    func query() async throws -> C
}

enum CollectionStoreError: Error {
    case jsonStringUTF8Failed
}

protocol DateStamped {
    var modifyDate: Date? { get set }
}
