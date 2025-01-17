// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/golang/snappy"

	"github.com/syndtr/goleveldb/leveldb/cache"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Reader errors.
var (
	ErrNotFound       = errors.ErrNotFound
	ErrReaderReleased = errors.New("leveldb/table: reader released")
	ErrIterReleased   = errors.New("leveldb/table: iterator released")
)

// ErrCorrupted describes error due to corruption. This error will be wrapped
// with errors.ErrCorrupted.
type ErrCorrupted struct {
	Pos    int64
	Size   int64
	Kind   string
	Reason string
}

func (e *ErrCorrupted) Error() string {
	return fmt.Sprintf("leveldb/table: corruption on %s (pos=%d): %s", e.Kind, e.Pos, e.Reason)
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// 通用 block 的抽象
type block struct {
	bpool          *util.BufferPool //
	bh             blockHandle      // block 位置
	data           []byte           // 内存数据
	restartsLen    int              // 有多少个 restart 点
	restartsOffset int              // restart 数组从哪个偏移开始
}

// 找到哪个 restart point 范围，
// 找到大致范围之后再一个个找；
func (b *block) seek(cmp comparer.Comparer, rstart, rlimit int, key []byte) (index, offset int, err error) {
	// 在这个 restart point 数组中，查找到这个最小的大于它的位置，然后往前推一个
	/*
			<= key <
				|
		｜-------entry-----｜-----entry-----|
		/                   \
		r index-1            r index
	*/
	index = sort.Search(b.restartsLen-rstart-(b.restartsLen-rlimit), func(i int) bool {
		// i 这个位置的 restart point 的 offset 是啥？
		offset := int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*(rstart+i):]))
		offset++                                    // shared always zero, since this is a restart point
		v1, n1 := binary.Uvarint(b.data[offset:])   // key length
		_, n2 := binary.Uvarint(b.data[offset+n1:]) // value length
		m := offset + n1 + n2

		// 找到个最小的比这个 key 大的；
		// 其实也就是找到 key 落到哪个 restart point 范围
		return cmp.Compare(b.data[m:m+int(v1)], key) > 0
	}) + rstart - 1

	if index < rstart {
		// The smallest key is greater-than key sought.
		index = rstart
	}

	// b.restartsOffset 是 restart point 数组开始的位置
	// index 是数组的第几个元素
	// 所以这计算出来的 offset 就是这个 restart 点的数据 offset
	offset = int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*index:]))
	return
}

//
func (b *block) restartIndex(rstart, rlimit, offset int) int {
	return sort.Search(b.restartsLen-rstart-(b.restartsLen-rlimit), func(i int) bool {
		// 从 restart point 数组中找到最小的那个，大于 offset 的
		return int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*(rstart+i):])) > offset
	}) + rstart - 1
}

// 第 index restart 点的偏移
func (b *block) restartOffset(index int) int {
	return int(binary.LittleEndian.Uint32(b.data[b.restartsOffset+4*index:]))
}

// 获取到 offset 位置的 entry 的 kv 对
func (b *block) entry(offset int) (key, value []byte, nShared, n int, err error) {
	if offset >= b.restartsOffset {
		if offset != b.restartsOffset {
			err = &ErrCorrupted{Reason: "entries offset not aligned"}
		}
		return
	}
	v0, n0 := binary.Uvarint(b.data[offset:])       // Shared prefix length
	v1, n1 := binary.Uvarint(b.data[offset+n0:])    // Key length
	v2, n2 := binary.Uvarint(b.data[offset+n0+n1:]) // Value length
	m := n0 + n1 + n2
	n = m + int(v1) + int(v2)
	if n0 <= 0 || n1 <= 0 || n2 <= 0 || offset+n > b.restartsOffset {
		err = &ErrCorrupted{Reason: "entries corrupted"}
		return
	}
	key = b.data[offset+m : offset+m+int(v1)]
	value = b.data[offset+m+int(v1) : offset+n]
	nShared = int(v0)
	return
}

func (b *block) Release() {
	b.bpool.Put(b.data)
	b.bpool = nil
	b.data = nil
}

type dir int

const (
	dirReleased dir = iota - 1
	dirSOI
	dirEOI
	dirBackward
	dirForward
)

// 对一个通用 block 的迭代封装
type blockIter struct {
	tr            *Reader       // sst 读句柄
	block         *block        // 对应的 block
	blockReleaser util.Releaser //
	releaser      util.Releaser //
	key, value    []byte        //
	offset        int           // 当前 iter 偏移
	// Previous offset, only filled by Next.
	prevOffset   int
	prevNode     []int
	prevKeys     []byte
	restartIndex int
	// Iterator direction.
	dir dir
	// Restart index slice range.
	riStart int // 标明数组的起始位置
	riLimit int // 标明 restart points 数组的长度
	// Offset slice range.
	offsetStart     int // entry 的 offset 开始位置
	offsetRealStart int // offset 真实的起始位置
	offsetLimit     int // entry 的 end 的位置 ( 也是 restart point 的起始位置 )
	// Error.
	err error
}

// block 迭代器遇到错误的时候会把 err 存起来，也会重置一些字段
func (i *blockIter) sErr(err error) {
	i.err = err
	i.key = nil
	i.value = nil
	i.prevNode = nil
	i.prevKeys = nil
}

// 重置
func (i *blockIter) reset() {
	if i.dir == dirBackward {
		i.prevNode = i.prevNode[:0]
		i.prevKeys = i.prevKeys[:0]
	}
	i.restartIndex = i.riStart
	i.offset = i.offsetStart
	i.dir = dirSOI
	i.key = i.key[:0]
	i.value = nil
}

// 看是否是第一个？
func (i *blockIter) isFirst() bool {
	switch i.dir {
	case dirForward:
		return i.prevOffset == i.offsetRealStart
	case dirBackward:
		return len(i.prevNode) == 1 && i.restartIndex == i.riStart
	}
	return false
}

// 是否是最后一个？
func (i *blockIter) isLast() bool {
	switch i.dir {
	case dirForward, dirBackward:
		return i.offset == i.offsetLimit
	}
	return false
}

func (i *blockIter) First() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	if i.dir == dirBackward {
		i.prevNode = i.prevNode[:0]
		i.prevKeys = i.prevKeys[:0]
	}
	i.dir = dirSOI
	return i.Next()
}

func (i *blockIter) Last() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	if i.dir == dirBackward {
		i.prevNode = i.prevNode[:0]
		i.prevKeys = i.prevKeys[:0]
	}
	i.dir = dirEOI
	return i.Prev()
}

func (i *blockIter) Seek(key []byte) bool {
	if i.err != nil {
		// 如果有 pendin 的 err，则直接返回 false
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	// 找到 key 所在的 restart point 位置范围？
	ri, offset, err := i.block.seek(i.tr.cmp, i.riStart, i.riLimit, key)
	if err != nil {
		i.sErr(err)
		return false
	}
	i.restartIndex = ri
	i.offset = max(i.offsetStart, offset)
	if i.dir == dirSOI || i.dir == dirEOI {
		i.dir = dirForward
	}

	// 通过 restart point 定位到大致的片段，接下来就要逐个遍历看看喽
	for i.Next() {
		// 逐个比较 key，找到个 >= 的就可以了
		if i.tr.cmp.Compare(i.key, key) >= 0 {
			// seek 找到了个位置 [ , )
			return true
		}
	}
	return false
}

// 迭代器步进
func (i *blockIter) Next() bool {
	if i.dir == dirEOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	if i.dir == dirSOI {
		i.restartIndex = i.riStart
		i.offset = i.offsetStart
	} else if i.dir == dirBackward {
		i.prevNode = i.prevNode[:0]
		i.prevKeys = i.prevKeys[:0]
	}
	for i.offset < i.offsetRealStart {
		key, value, nShared, n, err := i.block.entry(i.offset)
		if err != nil {
			i.sErr(i.tr.fixErrCorruptedBH(i.block.bh, err))
			return false
		}
		if n == 0 {
			i.dir = dirEOI
			return false
		}
		i.key = append(i.key[:nShared], key...)
		i.value = value
		i.offset += n
	}
	if i.offset >= i.offsetLimit {
		i.dir = dirEOI
		if i.offset != i.offsetLimit {
			i.sErr(i.tr.newErrCorruptedBH(i.block.bh, "entries offset not aligned"))
		}
		return false
	}
	// 获取一个 entry
	key, value, nShared, n, err := i.block.entry(i.offset)
	if err != nil {
		i.sErr(i.tr.fixErrCorruptedBH(i.block.bh, err))
		return false
	}
	if n == 0 {
		i.dir = dirEOI
		return false
	}
	i.key = append(i.key[:nShared], key...)
	i.value = value
	i.prevOffset = i.offset
	i.offset += n
	i.dir = dirForward
	return true
}

func (i *blockIter) Prev() bool {
	if i.dir == dirSOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	var ri int
	if i.dir == dirForward {
		// Change direction.
		i.offset = i.prevOffset
		if i.offset == i.offsetRealStart {
			i.dir = dirSOI
			return false
		}
		ri = i.block.restartIndex(i.restartIndex, i.riLimit, i.offset)
		i.dir = dirBackward
	} else if i.dir == dirEOI {
		// At the end of iterator.
		i.restartIndex = i.riLimit
		i.offset = i.offsetLimit
		if i.offset == i.offsetRealStart {
			i.dir = dirSOI
			return false
		}
		ri = i.riLimit - 1
		i.dir = dirBackward
	} else if len(i.prevNode) == 1 {
		// This is the end of a restart range.
		i.offset = i.prevNode[0]
		i.prevNode = i.prevNode[:0]
		if i.restartIndex == i.riStart {
			i.dir = dirSOI
			return false
		}
		i.restartIndex--
		ri = i.restartIndex
	} else {
		// In the middle of restart range, get from cache.
		n := len(i.prevNode) - 3
		node := i.prevNode[n:]
		i.prevNode = i.prevNode[:n]
		// Get the key.
		ko := node[0]
		i.key = append(i.key[:0], i.prevKeys[ko:]...)
		i.prevKeys = i.prevKeys[:ko]
		// Get the value.
		vo := node[1]
		vl := vo + node[2]
		i.value = i.block.data[vo:vl]
		i.offset = vl
		return true
	}
	// Build entries cache.
	i.key = i.key[:0]
	i.value = nil
	offset := i.block.restartOffset(ri)
	if offset == i.offset {
		ri--
		if ri < 0 {
			i.dir = dirSOI
			return false
		}
		offset = i.block.restartOffset(ri)
	}
	i.prevNode = append(i.prevNode, offset)
	for {
		key, value, nShared, n, err := i.block.entry(offset)
		if err != nil {
			i.sErr(i.tr.fixErrCorruptedBH(i.block.bh, err))
			return false
		}
		if offset >= i.offsetRealStart {
			if i.value != nil {
				// Appends 3 variables:
				// 1. Previous keys offset
				// 2. Value offset in the data block
				// 3. Value length
				i.prevNode = append(i.prevNode, len(i.prevKeys), offset-len(i.value), len(i.value))
				i.prevKeys = append(i.prevKeys, i.key...)
			}
			i.value = value
		}
		i.key = append(i.key[:nShared], key...)
		offset += n
		// Stop if target offset reached.
		if offset >= i.offset {
			if offset != i.offset {
				i.sErr(i.tr.newErrCorruptedBH(i.block.bh, "entries offset not aligned"))
				return false
			}

			break
		}
	}
	i.restartIndex = ri
	i.offset = offset
	return true
}

func (i *blockIter) Key() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.key
}

func (i *blockIter) Value() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.value
}

func (i *blockIter) Release() {
	if i.dir != dirReleased {
		i.tr = nil
		i.block = nil
		i.prevNode = nil
		i.prevKeys = nil
		i.key = nil
		i.value = nil
		i.dir = dirReleased
		if i.blockReleaser != nil {
			i.blockReleaser.Release()
			i.blockReleaser = nil
		}
		if i.releaser != nil {
			i.releaser.Release()
			i.releaser = nil
		}
	}
}

func (i *blockIter) SetReleaser(releaser util.Releaser) {
	if i.dir == dirReleased {
		panic(util.ErrReleased)
	}
	if i.releaser != nil && releaser != nil {
		panic(util.ErrHasReleaser)
	}
	i.releaser = releaser
}

func (i *blockIter) Valid() bool {
	return i.err == nil && (i.dir == dirBackward || i.dir == dirForward)
}

func (i *blockIter) Error() error {
	return i.err
}

type filterBlock struct {
	bpool      *util.BufferPool // 池化
	data       []byte           // filter 数据，字节数组
	oOffset    int
	baseLg     uint
	filtersNum int
}

// 查找 key 是否“不存在”
func (b *filterBlock) contains(filter filter.Filter, offset uint64, key []byte) bool {
	i := int(offset >> b.baseLg)
	if i < b.filtersNum {
		o := b.data[b.oOffset+i*4:]
		n := int(binary.LittleEndian.Uint32(o))
		m := int(binary.LittleEndian.Uint32(o[4:]))
		if n < m && m <= b.oOffset {
			return filter.Contains(b.data[n:m], key)
		} else if n == m {
			return false
		}
	}
	return true
}

func (b *filterBlock) Release() {
	b.bpool.Put(b.data)
	b.bpool = nil
	b.data = nil
}

// index block 的迭代器的对应结构
type indexIter struct {
	*blockIter
	tr    *Reader
	slice *util.Range
	// Options
	fillCache bool
}

func (i *indexIter) Get() iterator.Iterator {
	value := i.Value()
	if value == nil {
		return nil
	}
	dataBH, n := decodeBlockHandle(value)
	if n == 0 {
		return iterator.NewEmptyIterator(i.tr.newErrCorruptedBH(i.tr.indexBH, "bad data block handle"))
	}

	var slice *util.Range
	if i.slice != nil && (i.blockIter.isFirst() || i.blockIter.isLast()) {
		slice = i.slice
	}
	return i.tr.getDataIterErr(dataBH, slice, i.tr.verifyChecksum, i.fillCache)
}

// Reader is a table reader.
type Reader struct {
	mu     sync.RWMutex
	fd     storage.FileDesc
	reader io.ReaderAt
	cache  *cache.NamespaceGetter
	err    error
	bpool  *util.BufferPool
	// Options
	o              *opt.Options
	cmp            comparer.Comparer
	filter         filter.Filter
	verifyChecksum bool

	dataEnd                   int64
	metaBH, indexBH, filterBH blockHandle
	indexBlock                *block
	filterBlock               *filterBlock
}

func (r *Reader) blockKind(bh blockHandle) string {
	switch bh.offset {
	case r.metaBH.offset:
		return "meta-block"
	case r.indexBH.offset:
		return "index-block"
	case r.filterBH.offset:
		if r.filterBH.length > 0 {
			return "filter-block"
		}
	}
	return "data-block"
}

func (r *Reader) newErrCorrupted(pos, size int64, kind, reason string) error {
	return &errors.ErrCorrupted{Fd: r.fd, Err: &ErrCorrupted{Pos: pos, Size: size, Kind: kind, Reason: reason}}
}

func (r *Reader) newErrCorruptedBH(bh blockHandle, reason string) error {
	return r.newErrCorrupted(int64(bh.offset), int64(bh.length), r.blockKind(bh), reason)
}

func (r *Reader) fixErrCorruptedBH(bh blockHandle, err error) error {
	if cerr, ok := err.(*ErrCorrupted); ok {
		cerr.Pos = int64(bh.offset)
		cerr.Size = int64(bh.length)
		cerr.Kind = r.blockKind(bh)
		return &errors.ErrCorrupted{Fd: r.fd, Err: cerr}
	}
	return err
}

// 读出一个 block 结构
func (r *Reader) readRawBlock(bh blockHandle, verifyChecksum bool) ([]byte, error) {
	// 分配一个能装这个 block 的内存块
	data := r.bpool.Get(int(bh.length + blockTrailerLen))
	// 把数据读到 data 中
	if _, err := r.reader.ReadAt(data, int64(bh.offset)); err != nil && err != io.EOF {
		return nil, err
	}

	// 是否做 crc 校验？
	if verifyChecksum {
		n := bh.length + 1
		checksum0 := binary.LittleEndian.Uint32(data[n:])
		checksum1 := util.NewCRC(data[:n]).Value()
		if checksum0 != checksum1 {
			r.bpool.Put(data)
			return nil, r.newErrCorruptedBH(bh, fmt.Sprintf("checksum mismatch, want=%#x got=%#x", checksum0, checksum1))
		}
	}

	// 根据 block 的 type 来做处理，这里的 type 主要是否有压缩，压缩了就要解压嘛
	switch data[bh.length] {
	case blockTypeNoCompression:
		data = data[:bh.length]
	case blockTypeSnappyCompression:
		decLen, err := snappy.DecodedLen(data[:bh.length])
		if err != nil {
			r.bpool.Put(data)
			return nil, r.newErrCorruptedBH(bh, err.Error())
		}
		decData := r.bpool.Get(decLen)
		decData, err = snappy.Decode(decData, data[:bh.length])
		r.bpool.Put(data)
		if err != nil {
			r.bpool.Put(decData)
			return nil, r.newErrCorruptedBH(bh, err.Error())
		}
		data = decData
	default:
		r.bpool.Put(data)
		return nil, r.newErrCorruptedBH(bh, fmt.Sprintf("unknown compression type %#x", data[bh.length]))
	}
	return data, nil
}

func (r *Reader) readBlock(bh blockHandle, verifyChecksum bool) (*block, error) {
	data, err := r.readRawBlock(bh, verifyChecksum)
	if err != nil {
		return nil, err
	}
	restartsLen := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	b := &block{
		bpool:          r.bpool,
		bh:             bh,
		data:           data,
		restartsLen:    restartsLen,
		restartsOffset: len(data) - (restartsLen+1)*4,
	}
	return b, nil
}

func (r *Reader) readBlockCached(bh blockHandle, verifyChecksum, fillCache bool) (*block, util.Releaser, error) {
	if r.cache != nil {
		var (
			err error
			ch  *cache.Handle
		)
		if fillCache {
			ch = r.cache.Get(bh.offset, func() (size int, value cache.Value) {
				var b *block
				b, err = r.readBlock(bh, verifyChecksum)
				if err != nil {
					return 0, nil
				}
				return cap(b.data), b
			})
		} else {
			ch = r.cache.Get(bh.offset, nil)
		}
		if ch != nil {
			b, ok := ch.Value().(*block)
			if !ok {
				ch.Release()
				return nil, nil, errors.New("leveldb/table: inconsistent block type")
			}
			return b, ch, err
		} else if err != nil {
			return nil, nil, err
		}
	}

	b, err := r.readBlock(bh, verifyChecksum)
	return b, b, err
}

func (r *Reader) readFilterBlock(bh blockHandle) (*filterBlock, error) {
	data, err := r.readRawBlock(bh, true)
	if err != nil {
		return nil, err
	}
	n := len(data)
	if n < 5 {
		return nil, r.newErrCorruptedBH(bh, "too short")
	}
	m := n - 5
	oOffset := int(binary.LittleEndian.Uint32(data[m:]))
	if oOffset > m {
		return nil, r.newErrCorruptedBH(bh, "invalid data-offsets offset")
	}
	// 构建一个 filter block
	b := &filterBlock{
		bpool:      r.bpool,
		data:       data,
		oOffset:    oOffset,
		baseLg:     uint(data[n-1]),
		filtersNum: (m - oOffset) / 4,
	}
	return b, nil
}

func (r *Reader) readFilterBlockCached(bh blockHandle, fillCache bool) (*filterBlock, util.Releaser, error) {
	if r.cache != nil {
		var (
			err error
			ch  *cache.Handle
		)
		if fillCache {
			ch = r.cache.Get(bh.offset, func() (size int, value cache.Value) {
				var b *filterBlock
				b, err = r.readFilterBlock(bh)
				if err != nil {
					return 0, nil
				}
				return cap(b.data), b
			})
		} else {
			ch = r.cache.Get(bh.offset, nil)
		}
		if ch != nil {
			b, ok := ch.Value().(*filterBlock)
			if !ok {
				ch.Release()
				return nil, nil, errors.New("leveldb/table: inconsistent block type")
			}
			return b, ch, err
		} else if err != nil {
			return nil, nil, err
		}
	}

	b, err := r.readFilterBlock(bh)
	return b, b, err
}

func (r *Reader) getIndexBlock(fillCache bool) (b *block, rel util.Releaser, err error) {
	if r.indexBlock == nil {
		return r.readBlockCached(r.indexBH, true, fillCache)
	}
	return r.indexBlock, util.NoopReleaser{}, nil
}

// 获取一个 filter block
func (r *Reader) getFilterBlock(fillCache bool) (*filterBlock, util.Releaser, error) {
	if r.filterBlock == nil {
		return r.readFilterBlockCached(r.filterBH, fillCache)
	}
	return r.filterBlock, util.NoopReleaser{}, nil
}

func (r *Reader) newBlockIter(b *block, bReleaser util.Releaser, slice *util.Range, inclLimit bool) *blockIter {
	bi := &blockIter{
		tr:            r,
		block:         b,
		blockReleaser: bReleaser,
		// Valid key should never be nil.
		key:             make([]byte, 0),
		dir:             dirSOI,
		riStart:         0,
		riLimit:         b.restartsLen,
		offsetStart:     0,
		offsetRealStart: 0,
		offsetLimit:     b.restartsOffset,
	}
	if slice != nil {
		if slice.Start != nil {
			if bi.Seek(slice.Start) {
				// 第几个 restart ？
				bi.riStart = b.restartIndex(bi.restartIndex, b.restartsLen, bi.prevOffset)
				// 这个位置的偏移
				bi.offsetStart = b.restartOffset(bi.riStart)
				bi.offsetRealStart = bi.prevOffset
			} else {
				bi.riStart = b.restartsLen
				bi.offsetStart = b.restartsOffset
				bi.offsetRealStart = b.restartsOffset
			}
		}
		if slice.Limit != nil {
			if bi.Seek(slice.Limit) && (!inclLimit || bi.Next()) {
				bi.offsetLimit = bi.prevOffset
				bi.riLimit = bi.restartIndex + 1
			}
		}
		bi.reset()
		if bi.offsetStart > bi.offsetLimit {
			bi.sErr(errors.New("leveldb/table: invalid slice range"))
		}
	}
	return bi
}

func (r *Reader) getDataIter(dataBH blockHandle, slice *util.Range, verifyChecksum, fillCache bool) iterator.Iterator {
	b, rel, err := r.readBlockCached(dataBH, verifyChecksum, fillCache)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}
	return r.newBlockIter(b, rel, slice, false)
}

func (r *Reader) getDataIterErr(dataBH blockHandle, slice *util.Range, verifyChecksum, fillCache bool) iterator.Iterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		return iterator.NewEmptyIterator(r.err)
	}

	return r.getDataIter(dataBH, slice, verifyChecksum, fillCache)
}

// NewIterator creates an iterator from the table.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// table. And a nil Range.Limit is treated as a key after all keys in
// the table.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The returned iterator is not safe for concurrent use and should be released
// after use.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (r *Reader) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		return iterator.NewEmptyIterator(r.err)
	}

	fillCache := !ro.GetDontFillCache()
	indexBlock, rel, err := r.getIndexBlock(fillCache)
	if err != nil {
		return iterator.NewEmptyIterator(err)
	}

	// sst 文件的 data block 的索引块: index block
	// 一般查找，先查索引，确认落到哪个 datablock ，再去查找 datablock 内部的内容
	index := &indexIter{
		blockIter: r.newBlockIter(indexBlock, rel, slice, true),
		tr:        r,
		slice:     slice,
		fillCache: !ro.GetDontFillCache(),
	}
	return iterator.NewIndexedIterator(index, opt.GetStrict(r.o, ro, opt.StrictReader))
}

func (r *Reader) find(key []byte, filtered bool, ro *opt.ReadOptions, noValue bool) (rkey, value []byte, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	indexBlock, rel, err := r.getIndexBlock(true)
	if err != nil {
		return
	}
	defer rel.Release()

	index := r.newBlockIter(indexBlock, nil, nil, true)
	defer index.Release()

	if !index.Seek(key) { // seek 到 >= key 的位置(注意，这个位置不一定 == key)
		if err = index.Error(); err == nil {
			err = ErrNotFound
		}
		return
	}

	dataBH, n := decodeBlockHandle(index.Value())
	if n == 0 {
		r.err = r.newErrCorruptedBH(r.indexBH, "bad data block handle")
		return nil, nil, r.err
	}

	// The filter should only used for exact match.
	if filtered && r.filter != nil {
		// filter block 就是对读的优化嘛，减少一些无谓的 io 穿透到磁盘
		filterBlock, frel, ferr := r.getFilterBlock(true)
		if ferr == nil {
			// 查找 filter 里是否有 key，这里其实就是一个 bloom filter 的实现
			if !filterBlock.contains(r.filter, dataBH.offset, key) {
				frel.Release()
				return nil, nil, ErrNotFound
			}
			frel.Release()
		} else if !errors.IsCorrupted(ferr) {
			return nil, nil, ferr
		}
	}

	data := r.getDataIter(dataBH, nil, r.verifyChecksum, !ro.GetDontFillCache())
	if !data.Seek(key) { // data block seek 到 >= key 的位置（注意，不一定 == key）
		data.Release()
		if err = data.Error(); err != nil {
			return
		}

		// The nearest greater-than key is the first key of the next block.
		if !index.Next() {
			if err = index.Error(); err == nil {
				err = ErrNotFound
			}
			return
		}

		dataBH, n = decodeBlockHandle(index.Value())
		if n == 0 {
			r.err = r.newErrCorruptedBH(r.indexBH, "bad data block handle")
			return nil, nil, r.err
		}

		data = r.getDataIter(dataBH, nil, r.verifyChecksum, !ro.GetDontFillCache())
		if !data.Next() {
			data.Release()
			if err = data.Error(); err == nil {
				err = ErrNotFound
			}
			return
		}
	}

	// Key doesn't use block buffer, no need to copy the buffer.
	rkey = data.Key() // >= key 的位置的 key，这个 key 不一定是结果。外面要做判断 ukey 的比较；
	if !noValue {
		if r.bpool == nil {
			value = data.Value()
		} else {
			// Value does use block buffer, and since the buffer will be
			// recycled, it need to be copied.
			value = append([]byte{}, data.Value()...)
		}
	}
	data.Release()
	return
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
// If filtered is true then the nearest 'block' will be checked against
// 'filter data' (if present) and will immediately return ErrNotFound if
// 'filter data' indicates that such pair doesn't exist.
//
// The caller may modify the contents of the returned slice as it is its
// own copy.
// It is safe to modify the contents of the argument after Find returns.
func (r *Reader) Find(key []byte, filtered bool, ro *opt.ReadOptions) (rkey, value []byte, err error) {
	return r.find(key, filtered, ro, false)
}

// FindKey finds key that is greater than or equal to the given key.
// It returns ErrNotFound if the table doesn't contain such key.
// If filtered is true then the nearest 'block' will be checked against
// 'filter data' (if present) and will immediately return ErrNotFound if
// 'filter data' indicates that such key doesn't exist.
//
// The caller may modify the contents of the returned slice as it is its
// own copy.
// It is safe to modify the contents of the argument after Find returns.
func (r *Reader) FindKey(key []byte, filtered bool, ro *opt.ReadOptions) (rkey []byte, err error) {
	rkey, _, err = r.find(key, filtered, ro, true)
	return
}

// Get gets the value for the given key. It returns errors.ErrNotFound
// if the table does not contain the key.
//
// The caller may modify the contents of the returned slice as it is its
// own copy.
// It is safe to modify the contents of the argument after Find returns.
func (r *Reader) Get(key []byte, ro *opt.ReadOptions) (value []byte, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	rkey, value, err := r.find(key, false, ro, false)
	if err == nil && r.cmp.Compare(rkey, key) != 0 {
		value = nil
		err = ErrNotFound
	}
	return
}

// OffsetOf returns approximate offset for the given key.
//
// It is safe to modify the contents of the argument after Get returns.
func (r *Reader) OffsetOf(key []byte) (offset int64, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.err != nil {
		err = r.err
		return
	}

	indexBlock, rel, err := r.readBlockCached(r.indexBH, true, true)
	if err != nil {
		return
	}
	defer rel.Release()

	index := r.newBlockIter(indexBlock, nil, nil, true)
	defer index.Release()
	if index.Seek(key) {
		dataBH, n := decodeBlockHandle(index.Value())
		if n == 0 {
			r.err = r.newErrCorruptedBH(r.indexBH, "bad data block handle")
			return
		}
		offset = int64(dataBH.offset)
		return
	}
	err = index.Error()
	if err == nil {
		offset = r.dataEnd
	}
	return
}

// Release implements util.Releaser.
// It also close the file if it is an io.Closer.
func (r *Reader) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if closer, ok := r.reader.(io.Closer); ok {
		closer.Close()
	}
	if r.indexBlock != nil {
		r.indexBlock.Release()
		r.indexBlock = nil
	}
	if r.filterBlock != nil {
		r.filterBlock.Release()
		r.filterBlock = nil
	}
	r.reader = nil
	r.cache = nil
	r.bpool = nil
	r.err = ErrReaderReleased
}

// 创建一个 table 的 reader
// NewReader creates a new initialized table reader for the file.
// The fi, cache and bpool is optional and can be nil.
//
// The returned table reader instance is safe for concurrent use.
func NewReader(f io.ReaderAt, size int64, fd storage.FileDesc, cache *cache.NamespaceGetter, bpool *util.BufferPool, o *opt.Options) (*Reader, error) {
	if f == nil {
		return nil, errors.New("leveldb/table: nil file")
	}

	r := &Reader{
		fd:             fd,
		reader:         f,
		cache:          cache,
		bpool:          bpool,
		o:              o,
		cmp:            o.GetComparer(),
		verifyChecksum: o.GetStrict(opt.StrictBlockChecksum),
	}

	if size < footerLen {
		r.err = r.newErrCorrupted(0, size, "table", "too small")
		return r, nil
	}

	// 算出 footer 在文件的偏移的位置
	footerPos := size - footerLen
	var footer [footerLen]byte
	// 把这个 footer 读出来，里面可是有 metaindex，index 的信息
	if _, err := r.reader.ReadAt(footer[:], footerPos); err != nil && err != io.EOF {
		return nil, err
	}
	// footer 魔数校验
	if string(footer[footerLen-len(magic):footerLen]) != magic {
		r.err = r.newErrCorrupted(footerPos, footerLen, "table-footer", "bad magic number")
		return r, nil
	}

	var n int
	// 把 meta 的 block handle 解析出来
	// Decode the metaindex block handle.
	r.metaBH, n = decodeBlockHandle(footer[:])
	if n == 0 {
		r.err = r.newErrCorrupted(footerPos, footerLen, "table-footer", "bad metaindex block handle")
		return r, nil
	}

	// 把 index 的 block handle 解析出来
	// Decode the index block handle.
	r.indexBH, n = decodeBlockHandle(footer[n:])
	if n == 0 {
		r.err = r.newErrCorrupted(footerPos, footerLen, "table-footer", "bad index block handle")
		return r, nil
	}

	// 根据 block handle 定位，把 meta 的 block 读出来
	// Read metaindex block.
	metaBlock, err := r.readBlock(r.metaBH, true)
	if err != nil {
		if errors.IsCorrupted(err) {
			r.err = err
			return r, nil
		}
		return nil, err
	}

	// 数据 block 的结束位置就是 meta block 开始的位置
	// Set data end.
	r.dataEnd = int64(r.metaBH.offset)

	// Read metaindex.
	metaIter := r.newBlockIter(metaBlock, nil, nil, true)
	// 迭代一个个 meta 对象
	for metaIter.Next() {
		key := string(metaIter.Key())
		if !strings.HasPrefix(key, "filter.") {
			continue
		}
		fn := key[7:]
		if f0 := o.GetFilter(); f0 != nil && f0.Name() == fn {
			// 设置 filter 句柄
			r.filter = f0
		} else {
			for _, f0 := range o.GetAltFilters() {
				if f0.Name() == fn {
					r.filter = f0
					break
				}
			}
		}
		if r.filter != nil {
			filterBH, n := decodeBlockHandle(metaIter.Value())
			if n == 0 {
				continue
			}
			r.filterBH = filterBH
			// Update data end.
			r.dataEnd = int64(filterBH.offset)
			break
		}
	}
	metaIter.Release()
	metaBlock.Release()

	// Cache index and filter block locally, since we don't have global cache.
	if cache == nil {
		r.indexBlock, err = r.readBlock(r.indexBH, true)
		if err != nil {
			if errors.IsCorrupted(err) {
				r.err = err
				return r, nil
			}
			return nil, err
		}
		if r.filter != nil {
			// 如果存在 filter block 的话，那么就读出来
			r.filterBlock, err = r.readFilterBlock(r.filterBH)
			if err != nil {
				if !errors.IsCorrupted(err) {
					return nil, err
				}

				// Don't use filter then.
				r.filter = nil
			}
		}
	}

	return r, nil
}
