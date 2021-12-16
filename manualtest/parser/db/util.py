#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import struct


def show_table(header, rows):
    from tabulate import tabulate
    print(tabulate(rows, headers=header, tablefmt="pretty", showindex="always", stralign="left"))


def last_byte(x):
    return x & 0xff


def put_uvarint(buf, x):
    i = 0
    while x >= 0x80:
        buf[i] = last_byte(x) | 0x80
        x >>= 7
        i += 1
    buf[i] = last_byte(x)
    return i+1


def uvarint(buf):
    x, s = 0, 0
    for i, bs in enumerate(buf):
        b = bs
        assert isinstance(bs, (int, bytes))
        if isinstance(bs, bytes):
            b = int(bs.hex(), 16)
        if b < 0x80:
            if i > 9 or i == 9 and b > 1:
                return 0, -(i+1)
            return x | int(b) << s, i+1

        x |= int(b & 0x7f) << s
        s += 7

    return 0, 0


def leveldb_crc_mask(v):
    return (v >> 15 | v << 17) + 0xa282ead8
