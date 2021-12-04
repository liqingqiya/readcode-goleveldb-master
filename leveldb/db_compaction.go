// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errCompactionTransactExiting = errors.New("leveldb: compaction transact exiting")
)

type cStat struct {
	duration time.Duration
	read     int64
	write    int64
}

func (p *cStat) add(n *cStatStaging) {
	p.duration += n.duration
	p.read += n.read
	p.write += n.write
}

func (p *cStat) get() (duration time.Duration, read, write int64) {
	return p.duration, p.read, p.write
}

type cStatStaging struct {
	start    time.Time
	duration time.Duration
	on       bool
	read     int64
	write    int64
}

func (p *cStatStaging) startTimer() {
	if !p.on {
		p.start = time.Now()
		p.on = true
	}
}

func (p *cStatStaging) stopTimer() {
	if p.on {
		p.duration += time.Since(p.start)
		p.on = false
	}
}

type cStats struct {
	lk    sync.Mutex
	stats []cStat
}

func (p *cStats) addStat(level int, n *cStatStaging) {
	p.lk.Lock()
	if level >= len(p.stats) {
		newStats := make([]cStat, level+1)
		copy(newStats, p.stats)
		p.stats = newStats
	}
	p.stats[level].add(n)
	p.lk.Unlock()
}

func (p *cStats) getStat(level int) (duration time.Duration, read, write int64) {
	p.lk.Lock()
	defer p.lk.Unlock()
	if level < len(p.stats) {
		return p.stats[level].get()
	}
	return
}

func (db *DB) compactionError() {
	var err error
noerr:
	// No error.
	for {
		select {
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
				goto haserr
			}
		case <-db.closeC:
			return
		}
	}
haserr:
	// Transient error.
	for {
		select {
		case db.compErrC <- err:
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
				goto noerr
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
			}
		case <-db.closeC:
			return
		}
	}
hasperr:
	// Persistent error.
	for {
		select {
		case db.compErrC <- err:
		case db.compPerErrC <- err:
		case db.writeLockC <- struct{}{}:
			// Hold write lock, so that write won't pass-through.
			db.compWriteLocking = true
		case <-db.closeC:
			if db.compWriteLocking {
				// We should release the lock or Close will hang.
				<-db.writeLockC
			}
			return
		}
	}
}

type compactionTransactCounter int

func (cnt *compactionTransactCounter) incr() {
	*cnt++
}

// 压缩事务的接口封装
type compactionTransactInterface interface {
	run(cnt *compactionTransactCounter) error
	revert() error
}

// 执行 compact 的事务
func (db *DB) compactionTransact(name string, t compactionTransactInterface) {
	defer func() {
		// 如果发生了 panic ，那么走 revert 流程
		if x := recover(); x != nil {
			if x == errCompactionTransactExiting {
				if err := t.revert(); err != nil {
					db.logf("%s revert error %q", name, err)
				}
			}
			panic(x)
		}
	}()

	const (
		backoffMin = 1 * time.Second
		backoffMax = 8 * time.Second
		backoffMul = 2 * time.Second
	)
	var (
		backoff  = backoffMin
		backoffT = time.NewTimer(backoff)
		lastCnt  = compactionTransactCounter(0)

		disableBackoff = db.s.o.GetDisableCompactionBackoff()
	)
	for n := 0; ; n++ {
		// Check whether the DB is closed.
		if db.isClosed() {
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		} else if n > 0 {
			db.logf("%s retrying N·%d", name, n)
		}

		// Execute.
		cnt := compactionTransactCounter(0)
		err := t.run(&cnt)
		if err != nil {
			db.logf("%s error I·%d %q", name, cnt, err)
		}

		// Set compaction error status.
		select {
		case db.compErrSetC <- err:
		case perr := <-db.compPerErrC:
			if err != nil {
				db.logf("%s exiting (persistent error %q)", name, perr)
				db.compactionExitTransact()
			}
		case <-db.closeC:
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		}
		if err == nil {
			return
		}
		if errors.IsCorrupted(err) {
			db.logf("%s exiting (corruption detected)", name)
			db.compactionExitTransact()
		}

		if !disableBackoff {
			// Reset backoff duration if counter is advancing.
			if cnt > lastCnt {
				backoff = backoffMin
				lastCnt = cnt
			}

			// Backoff.
			backoffT.Reset(backoff)
			if backoff < backoffMax {
				backoff *= backoffMul
				if backoff > backoffMax {
					backoff = backoffMax
				}
			}
			select {
			case <-backoffT.C:
			case <-db.closeC:
				db.logf("%s exiting", name)
				db.compactionExitTransact()
			}
		}
	}
}

// 函数式接口
type compactionTransactFunc struct {
	runFunc    func(cnt *compactionTransactCounter) error
	revertFunc func() error
}

func (t *compactionTransactFunc) run(cnt *compactionTransactCounter) error {
	return t.runFunc(cnt)
}

func (t *compactionTransactFunc) revert() error {
	if t.revertFunc != nil {
		return t.revertFunc()
	}
	return nil
}

// 对 compactionTransact 的一层封装
func (db *DB) compactionTransactFunc(name string, run func(cnt *compactionTransactCounter) error, revert func() error) {
	db.compactionTransact(name, &compactionTransactFunc{run, revert})
}

// 用 panic 跳流程？这合适吗
func (db *DB) compactionExitTransact() {
	panic(errCompactionTransactExiting)
}

func (db *DB) compactionCommit(name string, rec *sessionRecord) {
	// 可以并行的后台执行各种 compact ，但是递交的时候只能是串行的；
	// compCommitLK 就是用来保证串行化的
	db.compCommitLk.Lock()
	defer db.compCommitLk.Unlock() // Defer is necessary.

	// 只有 run，没有 revert。修改和递交引用关系就在此。
	db.compactionTransactFunc(name+"@commit", func(cnt *compactionTransactCounter) error {
		return db.s.commit(rec, true)
	}, nil)
}

func (db *DB) memCompaction() {
	// 取只读的 memdb 出来
	mdb := db.getFrozenMem()
	if mdb == nil {
		return
	}
	defer mdb.decref()

	db.logf("memdb@flush N·%d S·%s", mdb.Len(), shortenb(mdb.Size()))

	// Don't compact empty memdb.
	if mdb.Len() == 0 {
		db.logf("memdb@flush skipping")
		// drop frozen memdb
		db.dropFrozenMem()
		return
	}

	// Pause table compaction.
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseC <- (chan<- struct{})(resumeC):
	case <-db.compPerErrC:
		close(resumeC)
		resumeC = nil
	case <-db.closeC:
		db.compactionExitTransact()
	}

	var (
		rec        = &sessionRecord{}
		stats      = &cStatStaging{}
		flushLevel int
	)

	// 执行 memdb 的 flush（ minor compaction ）
	// Generate tables.
	db.compactionTransactFunc("memdb@flush", func(cnt *compactionTransactCounter) (err error) {
		stats.startTimer()
		// 把 readonly memdb flush 到磁盘
		flushLevel, err = db.s.flushMemdb(rec, mdb.DB, db.memdbMaxLevel)
		stats.stopTimer()
		return
	}, func() error {
		// 回退操作也是简单，删除这个新创建的就好
		for _, r := range rec.addedTables {
			db.logf("memdb@flush revert @%d", r.num)
			if err := db.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: r.num}); err != nil {
				return err
			}
		}
		return nil
	})

	rec.setJournalNum(db.journalFd.Num)
	rec.setSeqNum(db.frozenSeq)

	// 无害的 compact 做完了（只是新增一些无害的文件），该递交了；
	// 而为了的 commit 其实就是修改索引，最后变更指向关系。
	// Commit.
	stats.startTimer()
	db.compactionCommit("memdb", rec)
	stats.stopTimer()

	db.logf("memdb@flush committed F·%d T·%v", len(rec.addedTables), stats.duration)

	// Save compaction stats
	for _, r := range rec.addedTables {
		stats.write += r.size
	}
	db.compStats.addStat(flushLevel, stats)
	atomic.AddUint32(&db.memComp, 1)

	// flush 完成，释放掉 frozen mem
	// Drop frozen memdb.
	db.dropFrozenMem()

	// Resume table compaction.
	if resumeC != nil {
		select {
		case <-resumeC:
			close(resumeC)
		case <-db.closeC:
			db.compactionExitTransact()
		}
	}

	// Trigger table compaction.
	db.compTrigger(db.tcompCmdC)
}

// 执行 table  compact 的
type tableCompactionBuilder struct {
	db           *DB            // 数据库句柄
	s            *session       //
	c            *compaction    // 代表一个 compaction 状态
	rec          *sessionRecord //
	stat0, stat1 *cStatStaging  //

	snapHasLastUkey bool   //
	snapLastUkey    []byte //
	snapLastSeq     uint64 //
	snapIter        int    //
	snapKerrCnt     int    //
	snapDropCnt     int    //

	kerrCnt int //
	dropCnt int //

	minSeq    uint64 //
	strict    bool   //
	tableSize int    // table 文件的大小

	tw *tWriter
}

// 把 key/value 写入到对应的文件 writer 里去
func (b *tableCompactionBuilder) appendKV(key, value []byte) error {
	// Create new table if not already.
	if b.tw == nil {
		// Check for pause event.
		if b.db != nil {
			select {
			case ch := <-b.db.tcompPauseC:
				b.db.pauseCompaction(ch)
			case <-b.db.closeC:
				// 如果关闭了，那么走异常跳出
				b.db.compactionExitTransact()
			default:
			}
		}

		// Create new table.
		var err error
		b.tw, err = b.s.tops.create(b.tableSize)
		if err != nil {
			return err
		}
	}

	// Write key/value into table.
	return b.tw.append(key, value)
}

// 判断是否要刷上一刷 ?
func (b *tableCompactionBuilder) needFlush() bool {
	return b.tw.tw.BytesLen() >= b.tableSize
}

func (b *tableCompactionBuilder) flush() error {
	t, err := b.tw.finish()
	if err != nil {
		return err
	}
	b.rec.addTableFile(b.c.sourceLevel+1, t)
	b.stat1.write += t.size
	b.s.logf("table@build created L%d@%d N·%d S·%s %q:%q", b.c.sourceLevel+1, t.fd.Num, b.tw.tw.EntriesLen(), shortenb(int(t.size)), t.imin, t.imax)
	b.tw = nil
	return nil
}

func (b *tableCompactionBuilder) cleanup() {
	if b.tw != nil {
		b.tw.drop()
		b.tw = nil
	}
}

// 运行 compact 任务
func (b *tableCompactionBuilder) run(cnt *compactionTransactCounter) error {
	snapResumed := b.snapIter > 0
	hasLastUkey := b.snapHasLastUkey // The key might has zero length, so this is necessary.
	lastUkey := append([]byte{}, b.snapLastUkey...)
	lastSeq := b.snapLastSeq
	b.kerrCnt = b.snapKerrCnt
	b.dropCnt = b.snapDropCnt
	// Restore compaction state.
	b.c.restore()

	defer b.cleanup()

	b.stat1.startTimer()
	defer b.stat1.stopTimer()

	// 创建一个迭代器，从这个里面将捞出来一个个 key/value
	iter := b.c.newIterator()
	defer iter.Release()
	// 遍历这个迭代器
	for i := 0; iter.Next(); i++ {
		// 计数跑了多少次循环
		// Incr transact counter.
		cnt.incr()

		// Skip until last state.
		if i < b.snapIter {
			continue
		}

		resumed := false
		if snapResumed {
			resumed = true
			snapResumed = false
		}

		ikey := iter.Key()
		ukey, seq, kt, kerr := parseInternalKey(ikey)

		if kerr == nil {
			shouldStop := !resumed && b.c.shouldStopBefore(ikey)

			if !hasLastUkey || b.s.icmp.uCompare(lastUkey, ukey) != 0 {
				// First occurrence of this user key.

				// 看是否需要轮转出一个 sst 文件
				// Only rotate tables if ukey doesn't hop across.
				if b.tw != nil && (shouldStop || b.needFlush()) {
					if err := b.flush(); err != nil {
						return err
					}

					// Creates snapshot of the state.
					b.c.save()
					b.snapHasLastUkey = hasLastUkey
					b.snapLastUkey = append(b.snapLastUkey[:0], lastUkey...)
					b.snapLastSeq = lastSeq
					b.snapIter = i
					b.snapKerrCnt = b.kerrCnt
					b.snapDropCnt = b.dropCnt
				}
				// 记录一下，给下一次用来做判断
				hasLastUkey = true
				lastUkey = append(lastUkey[:0], ukey...)
				lastSeq = keyMaxSeq
			}

			switch {
			case lastSeq <= b.minSeq:
				// 如果是相同的 key，并且已经有新的 entry 了的，那么旧的就应该被抛弃
				// Dropped because newer entry for same user key exist
				fallthrough // (A)
			case kt == keyTypeDel && seq <= b.minSeq && b.c.baseLevelForKey(lastUkey):
				// 如果是被删除的 key
				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger seq numbers
				// (3) data in layers that are being compacted here and have
				//     smaller seq numbers will be dropped in the next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				lastSeq = seq
				b.dropCnt++
				continue
			default:
				lastSeq = seq
			}
		} else {
			if b.strict {
				return kerr
			}

			// Don't drop corrupted keys.
			hasLastUkey = false
			lastUkey = lastUkey[:0]
			lastSeq = keyMaxSeq
			b.kerrCnt++
		}

		// 写入到新的 sst 文件
		if err := b.appendKV(ikey, iter.Value()); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// 迭代完成，最后 flush 一次
	// Finish last table.
	if b.tw != nil && !b.tw.empty() {
		return b.flush()
	}
	return nil
}

// 压缩事务如果失败了，那么回退的逻辑就在这里
func (b *tableCompactionBuilder) revert() error {
	// 把这次压缩了的文件全部删除掉
	for _, at := range b.rec.addedTables {
		b.s.logf("table@build revert @%d", at.num)
		if err := b.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: at.num}); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) tableCompaction(c *compaction, noTrivial bool) {
	defer c.release()

	rec := &sessionRecord{}
	rec.addCompPtr(c.sourceLevel, c.imax)

	if !noTrivial && c.trivial() {
		t := c.levels[0][0]
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.sourceLevel+1)
		rec.delTable(c.sourceLevel, t.fd.Num)
		rec.addTableFile(c.sourceLevel+1, t)
		db.compactionCommit("table-move", rec)
		return
	}

	var stats [2]cStatStaging
	for i, tables := range c.levels {
		for _, t := range tables {
			stats[i].read += t.size
			// Insert deleted tables into record
			rec.delTable(c.sourceLevel+i, t.fd.Num)
		}
	}
	sourceSize := int(stats[0].read + stats[1].read)
	minSeq := db.minSeq()
	db.logf("table@compaction L%d·%d -> L%d·%d S·%s Q·%d", c.sourceLevel, len(c.levels[0]), c.sourceLevel+1, len(c.levels[1]), shortenb(sourceSize), minSeq)

	// 压缩事务的实现
	b := &tableCompactionBuilder{
		db:        db,
		s:         db.s,
		c:         c,
		rec:       rec,
		stat1:     &stats[1],
		minSeq:    minSeq,
		strict:    db.s.o.GetStrict(opt.StrictCompaction),
		tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
	}
	// 直接在这里调用 db.compactionTransact
	db.compactionTransact("table@build", b)

	// Commit.
	stats[1].startTimer()
	db.compactionCommit("table", rec)
	stats[1].stopTimer()

	resultSize := int(stats[1].write)
	db.logf("table@compaction committed F%s S%s Ke·%d D·%d T·%v", sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), b.kerrCnt, b.dropCnt, stats[1].duration)

	// Save compaction stats
	for i := range stats {
		db.compStats.addStat(c.sourceLevel+1, &stats[i])
	}
	switch c.typ {
	case level0Compaction:
		atomic.AddUint32(&db.level0Comp, 1)
	case nonLevel0Compaction:
		atomic.AddUint32(&db.nonLevel0Comp, 1)
	case seekCompaction:
		atomic.AddUint32(&db.seekComp, 1)
	}
}

func (db *DB) tableRangeCompaction(level int, umin, umax []byte) error {
	db.logf("table@compaction range L%d %q:%q", level, umin, umax)
	if level >= 0 {
		if c := db.s.getCompactionRange(level, umin, umax, true); c != nil {
			db.tableCompaction(c, true)
		}
	} else {
		// Retry until nothing to compact.
		for {
			compacted := false

			// Scan for maximum level with overlapped tables.
			v := db.s.version()
			m := 1
			for i := m; i < len(v.levels); i++ {
				tables := v.levels[i]
				if tables.overlaps(db.s.icmp, umin, umax, false) {
					m = i
				}
			}
			v.release()

			for level := 0; level < m; level++ {
				if c := db.s.getCompactionRange(level, umin, umax, false); c != nil {
					db.tableCompaction(c, true)
					compacted = true
				}
			}

			if !compacted {
				break
			}
		}
	}

	return nil
}

func (db *DB) tableAutoCompaction() {
	if c := db.s.pickCompaction(); c != nil {
		db.tableCompaction(c, false)
	}
}

func (db *DB) tableNeedCompaction() bool {
	v := db.s.version()
	defer v.release()
	return v.needCompaction()
}

// resumeWrite returns an indicator whether we should resume write operation if enough level0 files are compacted.
func (db *DB) resumeWrite() bool {
	v := db.s.version()
	defer v.release()
	if v.tLen(0) < db.s.o.GetWriteL0PauseTrigger() {
		return true
	}
	return false
}

func (db *DB) pauseCompaction(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	case <-db.closeC:
		db.compactionExitTransact()
	}
}

type cCmd interface {
	ack(err error)
}

type cAuto struct {
	// Note for table compaction, an non-empty ackC represents it's a compaction waiting command.
	ackC chan<- error
}

func (r cAuto) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

type cRange struct {
	level    int
	min, max []byte
	ackC     chan<- error
}

func (r cRange) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

// This will trigger auto compaction but will not wait for it.
func (db *DB) compTrigger(compC chan<- cCmd) {
	select {
	case compC <- cAuto{}:
	default:
	}
}

// This will trigger auto compaction and/or wait for all compaction to be done.
func (db *DB) compTriggerWait(compC chan<- cCmd) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cAuto{ch}:
	case err = <-db.compErrC:
		return
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

// Send range compaction request.
func (db *DB) compTriggerRange(compC chan<- cCmd, level int, min, max []byte) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cRange{level, min, max, ch}:
	case err := <-db.compErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

func (db *DB) mCompaction() {
	var x cCmd

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		select {
		case x = <-db.mcompCmdC:
			switch x.(type) {
			case cAuto:
				db.memCompaction()
				x.ack(nil)
				x = nil
			default:
				panic("leveldb: unknown command")
			}
		case <-db.closeC:
			return
		}
	}
}

// major compact 的触发过程
func (db *DB) tCompaction() {
	var (
		x     cCmd
		waitQ []cCmd
	)

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		for i := range waitQ {
			waitQ[i].ack(ErrClosed)
			waitQ[i] = nil
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		if db.tableNeedCompaction() {
			select {
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			default:
			}
			// Resume write operation as soon as possible.
			if len(waitQ) > 0 && db.resumeWrite() {
				for i := range waitQ {
					waitQ[i].ack(nil)
					waitQ[i] = nil
				}
				waitQ = waitQ[:0]
			}
		} else {
			for i := range waitQ {
				waitQ[i].ack(nil)
				waitQ[i] = nil
			}
			waitQ = waitQ[:0]
			select {
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			}
		}
		if x != nil {
			switch cmd := x.(type) {
			case cAuto:
				if cmd.ackC != nil {
					// Check the write pause state before caching it.
					if db.resumeWrite() {
						x.ack(nil)
					} else {
						waitQ = append(waitQ, x)
					}
				}
			case cRange:
				x.ack(db.tableRangeCompaction(cmd.level, cmd.min, cmd.max))
			default:
				panic("leveldb: unknown command")
			}
			x = nil
		}
		db.tableAutoCompaction()
	}
}
