# goleveldb 代码学习

**goleveldb 是 Leveldb 的 go 版本实现，基本实现了 LSM Tree 所有的功能，非常具有学习意义**。

本仓库是拉出来写注释的，记录最详细的学习过程。LSM Tree

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


# Python 解析 leveldb/rocksdb

这几个 Python 脚本是奇伢深入理解 LSM Tree 的数据结构之后，用 Python 写的解析规则，能够解析 leveldb 和 rocksdb 的 manifest 文件和 sst 文件，并且是兼容的。理解它们能让你更加深入了解 LSM 的设计。

## 解析 Manifest 文件：

- [parser/parse_db.py](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/manualtest/parser/parse_db.py)

截图如下：

manifest 的 journal 结构：

![](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/docs/statics/Jietu20211219-184825.jpg)

sst 的增删日志：

![](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/docs/statics/Jietu20211219-184859.jpg)

当前在用的 sst 文件：

![](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/docs/statics/Jietu20211219-184915.jpg)

## 解析 sst 文件

- [parser/parse_sst.py](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/manualtest/parser/parse_sst.py)

sst 文件的 block 信息：

![](https://github.com/liqingqiya/readcode-goleveldb-master/blob/master/docs/statics/Jietu20211219-185656.jpg)
