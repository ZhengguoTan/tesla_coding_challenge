#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 
# Author: Zhengguo Tan <zhengguo.tan@gmail.com>
# Date: 02.01.2022

import json
import urllib
import requests

import pandas as pd # version 1.0.3


# %% Task 1: load the results of the API call into a variable
def task1(url="https://data.messari.io/api/v1/assets?limit=30", file_name="assets.json"):

    req  = urllib.request.Request(url)
    res  = urllib.request.urlopen(req, timeout=500)

    data = res.read()

    values = json.loads(data)

    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(values, f, ensure_ascii=False, indent=4)

    return values

# %% Task 2: convert the data from JSON format into tabular
def task2(values):

    tab_all = pd.json_normalize(values["data"])

    cols_to_keep=['id', 'symbol', 'profile.category', 'metrics.market_data.price_btc', 'metrics.market_data.price_usd']

    tab_out = tab_all[cols_to_keep]

    str_status    = values["status"]
    str_timestamp = str_status["timestamp"]

    num_rows = tab_out.shape[0]

    for n in range(num_rows):

        tab_out.loc[n,"timestamp"] = str_timestamp
        tab_out.loc[n,"status"] = 0

    print("task2 outpus: ")
    print(tab_out)
    print("\n")

    return tab_out


# %% 
if __name__ == "__main__":

    values = task1()
    tab2 = task2(values)
