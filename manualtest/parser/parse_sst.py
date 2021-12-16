#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import optparse
import os
import logging.handlers

from db.sst import Sst, decode_block_handle
from db.sst import parse_internal_key
from db.util import show_table


def create_sst(path):
    try:
        sst = Sst(path)
        return sst
    except Exception:
        print("path:<{}> invalid ? exit".format(path))
        exit(1)


def main(options):
    if not os.path.exists(options.path):
        print("path:{} is not exist".format(options.path))
        return

    options.path = os.path.abspath(options.path)
    print("==== show options ====")
    print(options)
    print("==== options ====")
    print()

    # 创建 sst 文件对象
    sst = create_sst(options.path)
    print(sst.footer)

    blocks = []

    # meta index block，针对 filter 的元数据块
    metablock = sst.read_metablock()
    blocks.append(metablock)

    def show_keyvalue(key, value):
        print(key, value)

    metablock.Scan(show_keyvalue)

    # index block，针对 data block 的元数据索引
    index = sst.read_indexblock()
    blocks.append(index)

    while index.Next():
        key, value = index.Key(),  index.Value()
        bh, n = decode_block_handle(value)
        datablock = sst.read_datablock(bh)

        blocks.append(datablock)

        kv_header = ["ukey", "seq", "kt", "value"]
        kv_rows = []

        def get_user_key_value(ikey, value):
            ukey, seq, kt = parse_internal_key(ikey)
            kv_rows.append([ukey, seq, kt, len(value)])

        if options.show_detail > 0:
            datablock.Scan(get_user_key_value)

        show_table(kv_header, kv_rows)

    block_header = None
    block_rows = []
    for block in blocks:
        if block_header is None:
            block_header = block.header()
        row = block.generate_row()
        block_rows.append(row)

    show_table(block_header, block_rows)


if __name__ == "__main__":
    usage = "usage: %prog [options] arg"
    parser = optparse.OptionParser(usage)

    # sst 文件路径
    parser.add_option("-f", "--path", type=str, dest="path",
                      help="stt table file path")

    # 是否打印 key/value
    parser.add_option("-s", "--show_detail", type=int, default=0,
                      dest="show_detail", help="print all key/value.")

    args = parser.parse_args()
    options = args[0]

    main(options)
