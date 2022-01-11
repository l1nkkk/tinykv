# Hints
- 应该使用 `badger.Txn` 去实现 Reader 接口
- badger 不支持 CF，借助 `kv/util/engine_util` 模拟CF，通过往key加前缀的方式
- 使用修复版本的badgerDB `github.com/Connor1996/badger`
- `Discard()` 和关闭迭代器不要忘记


