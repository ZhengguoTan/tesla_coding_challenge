#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 
# Author: Zhengguo Tan <zhengguo.tan@gmail.com>
# Date: 02.01.2022

import json
import urllib
from pandas.io import sql
import requests

import pandas as pd

import sqlalchemy 
from sqlalchemy import create_engine

import sqlite3


# %% Task 1: load the results of the API call into a variable
def task1(url="https://data.messari.io/api/v1/markets?limit=40", file_name="markets.json"):

    req  = urllib.request.Request(url)
    res  = urllib.request.urlopen(req)

    data = res.read()

    values = json.loads(data)

    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(values, f, ensure_ascii=False, indent=4)

    return values

# %% Task 2: convert the data from JSON format into tabular
def task2(values):

    tab_all = pd.DataFrame.from_dict(values["data"])

    cols_to_keep=['id', 'exchange_name', 'base_asset_symbol', 'last_trade_at']

    tab_out = tab_all[cols_to_keep]

    print("task2 outpus: ")
    print(tab_out)
    print("\n")

    return tab_out

# %% Task 3: keep 'volume_last_24_hours'
#            drop duplicated 'base_asset_symbol'
def task3(values):

    tab_all = pd.DataFrame.from_dict(values["data"])

    tab_24_notna = tab_all[tab_all['volume_last_24_hours'].notna()]

    tab_sel = tab_24_notna.sort_values('volume_last_24_hours', ascending=False).drop_duplicates('base_asset_symbol').sort_index()

    cols_to_keep=['id', 'exchange_name', 'base_asset_symbol', 'last_trade_at']

    tab_out = tab_sel[cols_to_keep]

    print("task3 outpus: ")
    print(tab_out)
    print("\n")

    return tab_out

# %% Task 4: write table to SQL
def task4(tab):

    # engine = create_engine('sqlite://', echo=False)

    conn = sqlite3.connect('test_db')
    c = conn.cursor()

    c.execute('CREATE TABLE IF NOT EXISTS market_info (id, exchange_name, base_asset_symbol, last_trade_at)')
    conn.commit()

    tab.to_sql('market_info', conn, if_exists='replace', index=False) #, 
        # dtype={"id":sqlalchemy.types.Text(), 
        #     "exchange_name":sqlalchemy.types.VARCHAR(length=20), 
        #     "base_asset_symbol":sqlalchemy.types.VARCHAR(length=6), 
        #     "last_trade_at":sqlalchemy.types.VARCHAR(length=20)})


    print("task4: to sql")

    c.execute("SELECT * FROM market_info")

    for row in c.fetchall():
        print(row)

    print("fetch sql done.")

    return 0

# %% 
if __name__ == "__main__":

    values = task1()

    tab2 = task2(values)
    tab3 = task3(values)
    
    task4(tab3)
