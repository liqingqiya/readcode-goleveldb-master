# goleveldb 代码学习

**goleveldb 是 Leveldb 的 go 版本实现，基本实现了 LSM Tree 所有的功能，非常具有学习意义**。

本仓库是拉出来写注释的，记录最详细的学习过程。LSM Tree


# 作者公众号：奇伢云存储

# 源码博客

# 模块学习

## cache

## filter

## iterator

- [iterator/array_iter.go](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/leveldb/iterator/array_iter.go)
- [iterator/indexed_iter.go](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/leveldb/iterator/indexed_iter.go)
- [iterator/iter.go](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/leveldb/iterator/iter.go)
- [iterator/merged_iter.go](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/leveldb/iterator/merged_iter.go)


## journal

- [journal/journal.go](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/leveldb/journal/journal.go)

## memdb

- [memdb/memdb.go](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/leveldb/memdb/memdb.go)

## opt

## storage

## table
