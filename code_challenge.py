#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 
# Author: Zhengguo (Frank) Tan <zhengguo.tan@gmail.com>
# Date: 02.01.2022

# To run the code, you can simply execute the following command line in a Linux terminal (e.g. Ubuntu):
# python3 code_challenge.py


import json
import urllib
from pandas.io import sql
import requests

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

import sqlalchemy 
from sqlalchemy import create_engine

import sqlite3

import time

# %% Part 1 Step 1
# %% Task 1: load the results of the API call into a variable
def part1_step1_task1(url="https://data.messari.io/api/v1/markets?limit=40", file_name="markets.json"):

    req  = urllib.request.Request(url)
    res  = urllib.request.urlopen(req)

    data = res.read()

    values = json.loads(data)

    # save as "markets.json"
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(values, f, ensure_ascii=False, indent=4)

    return values

# %% Task 2: convert the data from JSON format into tabular
def part1_step1_task2(values):

    tab_all = pd.DataFrame.from_dict(values["data"])

    cols_to_keep=['id', 'exchange_name', 'base_asset_symbol', 'last_trade_at']

    tab_out = tab_all[cols_to_keep]

    print("task2 outputs: ")
    print(tab_out)
    print("\n")

    return tab_out

# %% Task 3: keep 'volume_last_24_hours'
#            drop duplicated 'base_asset_symbol'
def part1_step1_task3(values, display=False):

    tab_all = pd.DataFrame.from_dict(values["data"])

    # ONLY keep those that have had a trade volume_last_24_hours
    tab_24_notna = tab_all[tab_all['volume_last_24_hours'].notna()]

    if display:
        print(tab_24_notna)

    # If there is more than one market per base asset 
    # (base_asset_symbol), keep the one with the larger volume
    tab_sel = tab_24_notna.sort_values('volume_last_24_hours', ascending=False).drop_duplicates('base_asset_symbol').sort_index()

    cols_to_keep=['id', 'exchange_name', 'base_asset_symbol', 'last_trade_at']

    tab_out = tab_sel[cols_to_keep]

    print("task3 outputs: ")
    print(tab_out)
    print("\n")

    return tab_out

# %% Task 4: write table to SQL
def part1_step1_task4(tab):

    conn = sqlite3.connect('test_db')
    c = conn.cursor()

    c.execute('CREATE TABLE IF NOT EXISTS market_info (id, exchange_name, base_asset_symbol, last_trade_at)')
    conn.commit()

    tab.to_sql('market_info', conn, if_exists='replace', index=False)#, 
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


# %% Part 1 Step 2
# %% Task 1: load the results of the API call into a variable
def part1_step2_task1(url="https://data.messari.io/api/v1/assets?limit=30", file_name="assets.json"):

    req  = urllib.request.Request(url)
    res  = urllib.request.urlopen(req, timeout=500)

    data = res.read()

    values = json.loads(data)

    # save as assets.json
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(values, f, ensure_ascii=False, indent=4)

    return values

# %% Task 2: convert the data from JSON format into tabular
def part1_step2_task2(values):

    tab_all = pd.json_normalize(values["data"])

    cols_to_keep=['id', 'symbol', 'profile.category', 'metrics.market_data.price_btc', 'metrics.market_data.price_usd']

    tab_out = tab_all[cols_to_keep]

    str_status    = values["status"]
    str_timestamp = str_status["timestamp"]

    num_rows = tab_out.shape[0]

    for n in range(num_rows):

        tab_out.loc[n,"timestamp"] = str_timestamp
        tab_out.loc[n,"status"] = "0"

    print("task2 outputs: ")
    print(tab_out)
    print("\n")


    # detect 'anomalous' data 
    # The logic here is to detect null data entry
    is_NaN     = tab_out.isnull()
    rows_w_NaN = is_NaN.any(axis=1)
    print(rows_w_NaN)
    tab_out.loc[rows_w_NaN,"status"] = "1" 


    return tab_out

# %% Task 3: keep assets which exist in part1
def part1_step2_task3(part2_df, part1_df):

    # reads in 'base_asset_symbol' from part1 step1
    options = part1_df['base_asset_symbol']

    # Only keep those assets, 
    # which are also a base asset of the 40 markets 
    # extracted in Step 1
    tab_out = part2_df[part2_df['symbol'].isin(options)]

    print("task3 outputs: ")
    print(tab_out)
    print("\n")

    return tab_out

# %% to asset_info table
def part1_step2_tosql(tab, if_exists='fail'):

    conn = sqlite3.connect('test_db')
    c = conn.cursor()

    c.execute('CREATE TABLE IF NOT EXISTS asset_info (id, symbol, "profile.category", "metrics.market_data.price_btc", "metrics.market_data.price_usd", timestamp, status)')
    conn.commit()

    tab.to_sql('asset_info', conn, if_exists=if_exists, index=False)

    print("to sql")

    c.execute("SELECT * FROM asset_info")

    for row in c.fetchall():
        print(row)

    print("fetch sql done.")

# %% encapsulate all functions in step2
def part1_step2(part1_step1_tab3, if_exists='fail'):

    part1_step2_values = part1_step2_task1()

    part1_step2_tab2 = part1_step2_task2(part1_step2_values)
    part1_step2_tab3 = part1_step2_task3(part1_step2_tab2, part1_step1_tab3)

    part1_step2_tosql(part1_step2_tab3, if_exists)

# %% 
if __name__ == "__main__":

    print("****** Part 1 - step 1 ******")

    part1_step1_values = part1_step1_task1()

    part1_step1_tab2 = part1_step1_task2(part1_step1_values)
    part1_step1_tab3 = part1_step1_task3(part1_step1_values)
    
    part1_step1_task4(part1_step1_tab3)

    print("****** Part 1 - step2 ******")

    part1_step2(part1_step1_tab3, 'replace')

    print("****** Part 2 ******")

    # run Step2 every minute
    for num_calls in range(10):

        time.sleep(60)
        part1_step2(part1_step1_tab3, 'append')
