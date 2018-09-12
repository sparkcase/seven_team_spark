#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
sys.path.append("../")
from db_config import mysql_config
import mysql.connector as myconn


def save_by_batch(sql, data):
    conn = myconn.connect(host=mysql_config["host"], user=mysql_config["user"], passwd=mysql_config["passwd"], db=mysql_config["db"])
    cur = conn.cursor()
    try:
        cur.executemany(sql, tuple(data))
        conn.commit()
    except:
        conn.rollback()
    finally:
        cur.close()
        conn.close()
