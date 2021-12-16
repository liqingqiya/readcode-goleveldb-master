#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
import logging.handlers
import struct
import snappy

from .util import uvarint, put_uvarint

block_type_index = "index"
block_type_data = "data"
block_type_meta_index = "meta_index"
block_type_filter = "filter"

blockTrailerLen = 5
footerLen = 48
goleveldb_magic = b"\x57\xfb\x80\x8b\x24\x75\x47\xdb"

# rocksdb 
rocksdb_magic = b'\xf7\xcf\xf4\x85\xb7A\xe2\x88'
rocksdb_footerlen = 1+2*20+4+8

max_footer_len = max(footerLen, rocksdb_footerlen)

keyTypeDel = 0
keyTypeVal = 1


class BlockHandle:
    def __init__(self, offset, length):
        self.offset = offset
        self.length = length

    def __str__(self) -> str:
        return "(off:{}, len:{})".format(self.offset, self.length)


class InternalKey:
    def __init__(self, ikey) -> None:
        ukey, seq, kt = parse_internal_key(ikey)
        self.ukey = ukey
        self.seq = seq
        self.kt = kt

    def __str__(self) -> str:
        return "key:{}\nseq:{}\nkt:{}".format(self.ukey, self.seq, self.kt)

class Footer:
    def __init__(self, data) -> None:
        self.raw_data = data

        self.version = None
        self.checksum_type = None
        self.magic = None

        self.meta_bh = None
        self.index_bh = None

        self.sst_type = None

        self.parse(data)

    def __str__(self) -> str:
        return "---- Footer details: ----\n\tversion:{}\n\tchecksum_type:{}\n\tmagic:{}\n\tmeta_bh:{}\n\tindex_bh:{}\n\tsst_type:{}\n\t".format(
            self.version, self.checksum_type, self.magic, self.meta_bh, self.index_bh, self.sst_type)

    def parse(self, raw_data):
        assert footerLen <= len(raw_data) <= max_footer_len

        # 校验魔数
        pos = len(raw_data) - 8
        if raw_data[pos: pos+8] == goleveldb_magic:
            self.sst_type = "goleveldb"
        elif raw_data[pos: pos+8] == rocksdb_magic:
            self.sst_type = "rocksdb"
        else:
            print("magic is invalid")
            raise
        
        # leveldb: metaindex(20) / index(20) / magic(8)

        pos = 0
        if self.sst_type == "rocksdb":
            # rocksdb: checksum_type(1) / metaindex(20) / index(20) / version(4) / magic(8)
            ver_ptr = max_footer_len - 8 - 4
            self.version = struct.unpack("<I", raw_data[ver_ptr:ver_ptr+4])
            self.checksum_type, n = uvarint(raw_data)
            assert n == 1
            pos += n

        self.meta_bh, n = decode_block_handle(raw_data[pos:])
        if n == 0:
            raise
        self.index_bh, n = decode_block_handle(raw_data[pos+n:])
        if n == 0:
            raise

class Block:

    def __init__(self, bh, data, restarts_len, restarts_offset, block_type=None) -> None:
        self.type = block_type
        self.bh = bh
        self.data = data
        self.restarts_len = restarts_len
        self.restarts_offset = restarts_offset

        self.offset = 0
        self.prev_offset = 0
        self.key = b""
        self.value = b""

    def header(self):
        header = {
            "type": "type",
            "bh": "bh",
            "data": "data",
            "restarts_len": "restarts_len",
            "restarts_offset": "restarts_offset",
        }
        return list(header.keys())

    def generate_row(self):
        headers = self.header()
        row = []
        for h in headers:
            value = getattr(self, h)
            if isinstance(value, list):
                value = "len:{}".format(len(value))
            if h == "data":
                value = "len:{}".format(len(value))
            row.append(value)
        return row

    def __str__(self):
        return "Block: bh:{}\n\tdata/{}: {}...\n\trestarts_len: {}\n\trestarts_offset: {}".format(
            self.bh, len(
                self.data), self.data[:16], self.restarts_len, self.restarts_offset
        )

    def entry(self, offset):
        if offset >= self.restarts_offset:
            if offset != self.restarts_offset:
                raise
            return b"", b"", 0, 0
        # 共享字节数
        v0, n0 = uvarint(self.data[offset:])
        # 非共享字节数
        v1, n1 = uvarint(self.data[offset + n0:])
        # value 长度
        v2, n2 = uvarint(self.data[offset+n0+n1:])

        # 前面三个数字的总字节数
        m = n0+n1+n2
        # 这条 entry 的总字节数
        n = m + v1 + v2

        if n0 <= 0 or n1 <= 0 or n2 <= 0 or offset + n > self.restarts_offset:
            raise

        # 非共享的 key
        key = self.data[offset + m: offset+m + v1]
        # value
        value = self.data[offset + m + v1:offset + n]
        # 共享的字节数
        nshared = v0

        return key, value, nshared, n

    def Next(self):
        key, value, nshared, n = self.entry(self.offset)
        if n == 0:
            # 结束
            return False

        self.key = self.key[:nshared] + key
        self.value = value
        self.prev_offset = self.offset
        self.offset += n

        return True

    def Key(self):
        return self.key

    def Value(self):
        return self.value

    def Scan(self, fn):
        while self.Next():
            fn(self.Key(), self.Value())


class Sst:
    def __init__(self, path):
        assert path is not None
        self.path = path

        fd = os.open(path, os.O_RDONLY)
        self.fd = fd
        assert fd is not None

        stat = os.fstat(fd)
        size = stat.st_size
        self.size = size

        self.footer = self.read_footer()
        self.meta_bh, self.index_bh = self.footer.meta_bh, self.footer.index_bh

    def read_block(self, block_type, bh, verifyCrc):
        data = self.read_raw_block(bh, verifyCrc)

        restarts_len = struct.unpack("<I", data[len(data)-4:])[0]
        restarts_offset = len(data) - (restarts_len+1)*4

        return Block(bh, data, restarts_len, restarts_offset, block_type=block_type)

    def read_raw_block(self, bh, verifyCrc):
        readn = bh.length + blockTrailerLen

        data = os.pread(self.fd, readn, bh.offset)
        if not data or len(data) == 0:
            return None

        if verifyCrc:
            n = bh.length + 1
            crc0 = struct.unpack("<I", data[n:])
            # crc1 = leveldb_crc_mask(crc32(data[:n]))
            # print("crc0: ", crc0)
            # print("crc1: ", crc1)

        is_compress = data[bh.length]
        assert isinstance(is_compress, int)

        data = data[:bh.length]
        if is_compress == 1:
            data = snappy.uncompress(data[:bh.length])

        return data

    def read_footer(self):
        footer_pos = self.size - max_footer_len
        footer_buf = os.pread(self.fd, max_footer_len, footer_pos)

        # 校验魔数
        if footer_buf[max_footer_len-8:] == goleveldb_magic:
            self.sst_type = "goleveldb"
        elif footer_buf[max_footer_len-8:] == rocksdb_magic:
            self.sst_type = "rocksdb"
        else:
            raise
        
        if self.sst_type == "goleveldb":
            footer_buf = footer_buf[max_footer_len-footerLen:]

        footer = Footer(footer_buf)

        return footer

    def read_metablock(self):
        return self.read_block(block_type_meta_index, self.meta_bh, True)

    def read_indexblock(self):
        return self.read_block(block_type_index, self.index_bh, True)

    def read_datablock(self, bh):
        return self.read_block(block_type_data, bh, True)


def encode_block_handle(offset, length):
    buf_n = [0] * 8
    n = put_uvarint(buf_n, offset)
    buf_m = [0] * 8
    m = put_uvarint(buf_m, length)
    return buf_n[:n]+buf_m[:m], n + m


def decode_block_handle(buf):
    offset, n = uvarint(buf)
    length, m = uvarint(buf[n:])
    if n == 0 or m == 0:
        return None, 0
    return BlockHandle(offset, length), n+m


def make_internal_key(key, seq, kt):
    ikey = bytes(key, "utf-8")
    seq_kt = seq << 8 | kt
    ikey = ikey + struct.pack("<Q", seq_kt)
    return ikey


def parse_internal_key(ikey):
    if len(ikey) < 8:
        raise
    seq_kt = struct.unpack("<Q", ikey[len(ikey) - 8:])[0]
    seq, kt = seq_kt >> 8, seq_kt & 0xff
    if kt > keyTypeVal:
        raise
    ukey = ikey[:len(ikey)-8]
    return ukey, seq, kt
