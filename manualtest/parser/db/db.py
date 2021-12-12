#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
import struct
import logging.handlers
from typing import Iterator

from .sst import parse_internal_key, uvarint

header_size = 7
block_size = 32*1024

fullChunkType = 1
firstChunkType = 2
middleChunkType = 3
lastChunkType = 4

recComparer = 1
recJournalNum = 2
recNextFileNum = 3
recSeqNum = 4
recCompPtr = 5
recDelTable = 6
recAddTable = 7
recPrevJournalNum = 9


class SStTableMeta:
    def __init__(self, level, num, size=None, imin=None, imax=None) -> None:
        self.level = level
        self.num = num
        self.size = size
        self.imin = imin
        self.imax = imax

        self.init_parse()

    def __str__(self) -> str:
        s = "L:{}\tNum:{}".format(self.level, self.num)
        if self.size is not None:
            s += "size:{}".format(self.size)
        if self.imin is not None:
            s += "imin:{}".format(self.imin)
        if self.imax is not None:
            s += "imax:{}".format(self.imax)
        return s

    def init_parse(self):
        if self.imin is not None:
            self.imin = parse_internal_key(self.imin)
        if self.imax is not None:
            self.imax = parse_internal_key(self.imax)

    def header(self):
        # key 是 table 的 key，value 是对象属性的名字
        header = {
            "level": "level",
            "num": "num",
            "size": "size",
            "imin": "imin",
            "imax": "imax",
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

        if self.data is not None:
            self.decode()

    def __str__(self) -> str:
        s = "Session:\ncomparer:{}\njournal_num:{}\nprev_journal_num:{}\nnext_file_num:{}\nseq_num:{}\ncomp_ptrs:{}\nadded_tables:{}\ndelete_tables:{}".format(
            self.comparer, self.journal_num, self.prev_journal_num, self.next_file_num, self.seq_num, self.comp_ptrs, self.added_tables, self.delete_tables
        )

        return s

    def header(self):
        header = {
            "has_rec": "has_rec",
            "comparer": "comparer",
            "journal_num": "journal_num",
            "prev_journal_num": "prev_journal_num",
            "next_file_num": "next_file_num",
            "seq_num": "seq_num",
            "comp_ptrs": "comp_ptrs",
            "added_tables": "added_tables",
            "delete_tables": "delete_tables",
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
                    return self.data
                self.err = jr.next_chunk()
                if self.err is not None:
                    if self.err == EOFError:
                        return self.data
                    raise

            self.data = self.data + jr.buf[jr.i:jr.j]
            return len(self.data)


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
