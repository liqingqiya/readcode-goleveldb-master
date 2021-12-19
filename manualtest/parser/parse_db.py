#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import optparse
import os
import logging.handlers

from db.db import (
    Journal,
    SessionRecord,
    Manifest
)
from db.db import SStTableMeta
from db.util import show_table


def main(options):
    mani = Manifest(options.path)
    sessions = mani.recover_journals()

    session_header = SessionRecord().header()
    session_rows = []

    sst_header = ["op"] + SStTableMeta(0, 0).header()
    sst_rows = []

    sst_alives_rows = []
    sst_adds = {}
    sst_dels = {}

    for sess in sessions:
        row = sess.generate_row()
        session_rows.append(row)
        for t in sess.added_tables:
            row = ["Add"] + t.generate_row()
            sst_rows.append(row)
            sst_adds[t.num] = t

        for t in sess.delete_tables:
            row = ["Del"] + t.generate_row()
            sst_rows.append(row)
            sst_dels[t.num] = t

    # alives
    alives_nums = set(sst_adds.keys()) - set(sst_dels.keys())
    for num, t in sst_adds.items():
        if num not in alives_nums:
            continue
        row = ["Live"] + t.generate_row()
        sst_alives_rows.append(row)

    print("===== Session Records ====")
    show_table(session_header, session_rows)
    print()
    print()

    print("==== sst tables ====")
    show_table(sst_header, sst_rows)
    print()
    print()

    print("==== alive sst tables ====")
    show_table(sst_header, sst_alives_rows)
    print()
    print()


if __name__ == "__main__":
    usage = "usage: %prog [options] arg"
    parser = optparse.OptionParser(usage)

    # 互斥模式
    parser.add_option("-f", "--file", type=str, dest="path",
                      help="manifest file path")

    args = parser.parse_args()
    options = args[0]

    main(options)
