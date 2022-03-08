#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
import struct
import logging.handlers
from typing import Iterator

from .sst import InternalKey, parse_internal_key, uvarint

header_size = 7
block_size = 32*1024

fullChunkType = 1
firstChunkType = 2
middleChunkType = 3
lastChunkType = 4

# leveldb 标准
recComparer = 1
recJournalNum = 2
recNextFileNum = 3
recSeqNum = 4
recCompPtr = 5
recDelTable = 6
recAddTable = 7
recPrevJournalNum = 9

# rocksdb 扩展
kMinLogNumberToKeep = 10
kNewFile2 = 100
kNewFile3 = 102
kNewFile4 = 103
kColumnFamily = 200
kColumnFamilyAdd = 201
kColumnFamilyDrop = 202
kMaxColumnFamily = 203
kInAtomicGroup = 300


class SStTableMeta:
    def __init__(self, level, num, size=None, imin=None, imax=None, imin_seq=None, imax_seq=None) -> None:
        self.level = level
        self.num = num
        self.size = size
        self.imin = imin
        self.imax = imax
        self.imin_seq = imin_seq
        self.imax_seq = imax_seq

        self.init_parse()

    def __str__(self) -> str:
        s = "L:{}\tNum:{}".format(self.level, self.num)
        if self.size is not None:
            s += "size:{}".format(self.size)
        if self.imin is not None:
            s += "imin:{}".format(self.imin)
        if self.imax is not None:
            s += "imax:{}".format(self.imax)
        if self.imin_seq is not None:
            s += "imin_seq:{}".format(self.imin_seq)
        if self.imax_seq is not None:
            s += "imax_seq:{}".format(self.imax_seq)
        return s

    def init_parse(self):
        if self.imin is not None:
            self.imin = InternalKey(self.imin)
        if self.imax is not None:
            self.imax = InternalKey(self.imax)

    def header(self):
        # key 是 table 的 key，value 是对象属性的名字
        header = {
            "level": "level",
            "num": "num",
            "size": "size",
            "imin": "imin",
            "imax": "imax",
            "imin_seq": "imin_seq",
            "imax_seq": "imax_seq",
        }
        return list(header.keys())

    def generate_row(self):
        headers = self.header()
        row = []
        for h in headers:
            value = getattr(self, h)
            if isinstance(value, list):
                value = "len:{}".format(len(value))
            row.append(value)
        return row


class SessionRecord:
    def __init__(self, data=None) -> None:
        self.data = data
        self.pos = 0

        self.has_rec = 0
        self.comparer = ""
        self.journal_num = 0
        self.prev_journal_num = 0
        self.next_file_num = 0
        self.seq_num = 0
        self.comp_ptrs = []
        self.added_tables = []
        self.delete_tables = []

        # rocksdb
        self.column_family_id = None
        self.column_family_name = None
        self.column_family_add = None
        self.column_family_del = None

        if self.data is not None:
            self.decode()

        self.header()

    def __str__(self) -> str:
        s = "Session:\ncomparer:{}\njournal_num:{}\nprev_journal_num:{}\nnext_file_num:{}\nseq_num:{}\ncomp_ptrs:{}\nadded_tables:{}\ndelete_tables:{}".format(
            self.comparer, self.journal_num, self.prev_journal_num, self.next_file_num, self.seq_num, self.comp_ptrs, self.added_tables, self.delete_tables
        )

        return s

    def header(self):
        self.table_headers = {
            "has_rec": "has_rec",
            "comparer": "comparer",
            "journal_num": "journal_num",
            "prev_journal_num": "prev_journal_num",
            "next_file_num": "next_file_num",
            "seq_num": "seq_num",
            "comp_ptrs": "comp_ptrs",
            "added_tables": "added_tables",
            "delete_tables": "delete_tables",

            # rocksdb
            "column_family_id": "cf_id",
            "column_family_name": "cf_name",
            "column_family_add": "cf_add",
            "column_family_del": "cf_del",
        }
        return list(self.table_headers.values())

    def generate_row(self):
        row = []
        for h in self.table_headers:
            value = getattr(self, h)
            if isinstance(value, list):
                value = "len:{}".format(len(value))
            row.append(value)
        return row

    def read_uvarint(self):
        x, n = uvarint(self.data[self.pos:])
        self.pos += n

        return x

    def read_bytes(self):
        length = self.read_uvarint()
        buf = self.data[self.pos:self.pos+length]
        self.pos += length
        return buf

    def is_done(self):
        return self.pos >= len(self.data)

    def decode(self):
        while not self.is_done():
            rec = self.read_uvarint()
            if rec == recComparer:
                self.comparer = self.read_bytes()
            elif rec == recJournalNum:
                num = self.read_uvarint()
                self.journal_num = num
            elif rec == recPrevJournalNum:
                num = self.read_uvarint()
                self.prev_journal_num = num
            elif rec == recNextFileNum:
                num = self.read_uvarint()
                self.next_file_num = num
            elif rec == recSeqNum:
                num = self.read_uvarint()
                self.seq_num = num
            elif rec == recCompPtr:
                level = self.read_uvarint()
                ikey = self.read_bytes()
                self.comp_ptrs.append([level, ikey])
            elif rec == recAddTable:
                level = self.read_uvarint()
                num = self.read_uvarint()
                size = self.read_uvarint()
                imin = self.read_bytes()
                imax = self.read_bytes()

                self.added_tables.append(
                    SStTableMeta(level, num, size, imin, imax))

            elif rec == recDelTable:
                level = self.read_uvarint()
                num = self.read_uvarint()
                self.delete_tables.append(SStTableMeta(level, num))

            elif rec == kMaxColumnFamily:
                print("rocksdb: {}".format(rec))
                cf_id = self.read_uvarint()
                print("max column family:%d".format(cf_id))

            elif rec == kMinLogNumberToKeep:
                print("rocksdb: {}".format(rec))
                min_log_num_to_keep = self.read_uvarint()
                print("min log num to kee:%d".format(min_log_num_to_keep))

            elif rec == kNewFile2:
                level = self.read_uvarint()
                num = self.read_uvarint()
                size = self.read_uvarint()
                imin = self.read_bytes()
                imax = self.read_bytes()
                imin_seq = self.read_uvarint()
                imax_seq = self.read_uvarint()

                self.added_tables.append(
                    SStTableMeta(level, num, size, imin, imax, imin_seq, imax_seq))

            elif rec == kNewFile3:
                raise Exception
            elif rec == kNewFile4:
                raise Exception
            elif rec == kColumnFamily:
                self.column_family_id = self.read_uvarint()
            elif rec == kColumnFamilyAdd:
                self.column_family_name = self.read_bytes()
                self.column_family_add = True
            elif rec == kColumnFamilyDrop:
                self.column_family_del = True
            elif rec == kInAtomicGroup:
                raise Exception
            else:
                raise Exception


class JournalRecord:
    def __init__(self, seq, jr) -> None:
        self.seq = seq
        self.jr = jr
        self.data = b""
        self.err = None

    def read_journal(self):
        jr = self.jr

        while True:
            while jr.i == jr.j:
                if jr.last == True:
                    return len(self.data)
                self.err = jr.next_chunk()
                if self.err is not None:
                    if self.err == EOFError:
                        return len(self.data)
                    raise

            self.data = self.data + jr.buf[jr.i:jr.j]

            if jr.last == True:
                return len(self.data)

            jr.Next()


class Journal:
    def __init__(self, fd) -> None:
        assert fd is not None

        self.fd = fd

        # 给迭代器用的字段，记录一些当前迭代器的状态
        self.seq = 0
        self.i = 0
        self.j = 0
        self.n = 0
        self.buf = None
        self.err = None
        self.last = True

    def Next(self) -> (JournalRecord):
        self.seq += 1
        self.i = self.j
        while True:
            err = self.next_chunk()
            if err is None:
                break
            else:
                return None

        return JournalRecord(self.seq, self)

    def next_chunk(self):
        while True:
            if self.j + header_size <= self.n:
                checksum = struct.unpack("<I", self.buf[self.j+0:self.j+4])[0]
                length = struct.unpack("<H", self.buf[self.j+4:self.j+6])[0]
                chunk_type = self.buf[self.j + 6]
                unproc_block = self.n - self.j
                if checksum == 0 and length == 0 and chunk_type == 0:
                    raise

                self.i = self.j + header_size
                self.j = self.j + header_size + length

                if self.j > self.n:
                    raise

                if chunk_type == fullChunkType or chunk_type == lastChunkType:
                    self.last = True
                else:
                    self.last = False
                return

            if self.n < block_size and self.n > 0:
                self.err = EOFError
                return self.err

            self.buf = os.read(self.fd, block_size)
            if not self.buf or len(self.buf) == 0:
                return EOFError

            self.i, self.j, self.n = 0, 0, len(self.buf)


class Manifest:
    def __init__(self, path):
        assert path is not None
        self.path = path

        fd = os.open(path, os.O_RDONLY)
        self.fd = fd
        assert fd is not None

        self.jr = Journal(fd)

    def recover_journals(self):
        jrs = []
        while True:
            # 取一条日志
            jr_rec = self.jr.Next()
            if jr_rec is None:
                break
            # 把单条完整的日志读取来
            length = jr_rec.read_journal()
            session_record = jr_rec.data
            session = SessionRecord(session_record)
            jrs.append(session)
        return jrs
