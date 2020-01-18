# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 08:21:52 2019

@author: ABMRazin
"""

import pandas as pd
import numpy as np
import pymysql
from  sqlalchemy import create_engine

conn = pymysql.connect(host = "localhost", user = "root", passwd = "13Tallabagh")
cur = conn.cursor()

cur.execute("use financial_database1")

engine = create_engine("mysql+pymysql://root: 13Tallabagh@localhost: 3306/financial_database1")

# For factors including / enclose it within small slash (`xxxx`)
def z_score(date, factorDct):
    
    # Creating the sql query that will be used to retrieve data from the factor table
    sqlQuery = "select"
    sqlString = " symbol_id, ticker, quarter, sector"
    
    factorLst = []
    weightLst = []
    for factor, weight in factorDct.items():
        sqlString = sqlString + ", " + factor
        
        factorLst.append(factor)
        weightLst.append(weight)

    sqlQuery = sqlQuery + sqlString
    add = " from factors where date > '{0}'".format(date)
    sqlQuery = sqlQuery + add
    print (sqlQuery)
    
    df = pd.read_sql_query(sqlQuery, con = engine)
    
    # Converting data type to one format so that transform function can be carried out later
    for factor, weight in factorDct.items():
        try:
            df[factor] = df[factor].apply(lambda x: pd.np.nan if x == None else x)
            df[factor] = df[factor].apply(lambda x: float(x))
        except:
            continue
    
    # Calculating z-score of each company for each factor using sector averages and standard deviations of each factor 
    zscore = lambda x: (x - x.mean())/x.std()
    
    df_transform = df.groupby("sector")[factorLst].transform(zscore)
    df_transform = df_transform.fillna(0)
    
    for col in df_transform.columns:
        df_transform = df_transform.rename(columns = {col: col + "_" + "zscore"})
    
    columnLst = list(df_transform.columns)
    
    for i in range(len(columnLst)):
        df_transform[columnLst[i]] = df_transform[columnLst[i]] * weightLst[i]

    # Adjusting factors where lower is better
    if "6mon_std_zscore" in columnLst:
        df_transform["6mon_std_zscore"] = df_transform["6mon_std_zscore"] * -1

    if "12mon_std_zscore" in columnLst:
        df_transform["12mon_std_zscore"] = df_transform["12mon_std_zscore"] * -1

    if "36mon_std_zscore" in columnLst:
        df_transform["36mon_std_zscore"] = df_transform["36mon_std_zscore"] * -1

    if "netdebt_ebitda_zscore" in columnLst:
        df_transform["netdebt_ebitda_zscore"] = df_transform["netdebt_ebitda_zscore"] * -1
    
    df_transform["score"] = df_transform.sum(axis = 1)
    
    df = pd.concat([df, df_transform], axis = 1)
    
    # Rank companies based on scores
    df["rank"] = df["score"].rank(ascending = False)
    
    df = df.sort_values(by = ["score"], ascending = False)
    
    return df

#df = z_score(date = "2019-06-30", factorDct = {"3mon_return": 0.10, "ocf_yield": 0.15, "fcf_yield": 0.30, "gaap_eps_yield": 0.30, "adjoi_ev": 0.15})    

# Pick top 15% company from every sector 
def basket(date, factorDct):
    df = z_score(date, factorDct)
    
    # Sorting within groups based on column "score"
    p = 0.15
    
    df = (df.groupby("sector", group_keys = False).apply(lambda x: x.nlargest(int(len(x) * p), "score")))
    
    # Getting name of each company
    security = pd.read_sql_query("select id, security from symbol", con = engine)
    security = security.rename(columns = {"id": "symbol_id"})
    
    df = df.merge(security, how = "inner", on = "symbol_id")
    
    return df

# Example
df = basket(date = "2019-06-30", factorDct = {"6mon_std": 0.25, "12mon_std": 0.50, "36mon_std": 0.25})

# Factors available:
# Momentum factors
#1) 3mon_return
#2) 6mon_return
#3) 9mon_return
#4) 12mon_return
#5) 12mon_sharpe
    
# Volatility factors
#1) 6mon_std
#2) 12mon_std
#3) 36mon_std    

# Value factors
#1) ocf_yield
#2) fcf_yield
#3) adjoi_ev
#4) ebitda_ev
#5) gaap_eps_yield
#6) b_p
#7) div_yield
#8) shareholder_yield

# Quality factors
#1) roa
#2) roic
#3) netdebt_ebitda
#4) fcf_conversion
#5) ebitda_margin
#6) ebitda_margin_exp

# Growth factors
#1) sales_growth
#2) fcf_growth
#3) gaapeps_growth



