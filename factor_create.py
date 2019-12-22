# -*- coding: utf-8 -*-
"""
Created on Sun Dec 15 12:21:17 2019

@author: ABMRazin
"""

# The purpose of the factor_create module is to use the price and financial data tables in the database
# to create value, growth, momentum, volatility and quality factors that can be used to create 
# factor-based security baskets for investing

# See factor model script to see how these factors are used to to create factor-based models


import pandas as pd
import pymysql
from  sqlalchemy import create_engine
import math

from functools import reduce

conn = pymysql.connect(host = [host_name], user = [user_name], passwd = [password])
cur = conn.cursor()

cur.execute("use financial_database1")

engine = create_engine("mysql+pymysql://[user_name]:[password]@[host_name]: [port_no.]/[database_name]")


def cfs(ticker):
    
    # Gets cash flow statement data from the database
    
    sqlQuery = "select symbol_id, ticker, quarter, cashfromoperations, capex, acquisitionspend, sharebuyback, da, sbcomp from cash_flow_statement where ticker = '{0}'".format(ticker)
    df = pd.read_sql_query(sqlQuery, con = engine)
    
    # Rename the columns to a shorter form
    df = df.rename(columns = {"cashfromoperations": "ocf", "acquisitionspend": "acquisition", 
                                     "sharebuyback": "buyback"})
    df["fcf"] = df["ocf"] - df["capex"]
            
    return df


def ttm_cfs(ticker):
    
    # Calculate TTM cfs line items from quarterly data
    
    df = cfs(ticker)
    df = df[df["quarter"] != "0"]
    df = df.dropna(subset = ["quarter"])
    ttm_df = df
    
    df = df.set_index("quarter")
    ttm_df = ttm_df.set_index("quarter")
    
    for date in df.index:
#        print (date)
        quarter = date[0:2]
        year = int(date[2:])
            
        if quarter[0:2] == "Q1" or quarter[0:2] == "Q2" or quarter[0:2] == "Q3":
            try:
                ttm_df.loc[date, "ocf":"fcf"] = df.loc[date, "ocf":"fcf"] + df.loc["FY"+str(year-1), "ocf":"fcf"] - df.loc[quarter+str(year-1), "ocf":"fcf"]
            except:
                ttm_df.loc[date, "ocf":"fcf"] = None
                continue
        elif quarter[0:2] == "FY" or quarter[0:2] == "Q4":
            try:
                ttm_df.loc[date, "ocf":"fcf"] = df.loc[date, "ocf":"fcf"]
            except:
                ttm_df.loc[date, "ocf":"fcf"] = None
                continue
    
    ttm_df = ttm_df.dropna()
    
    return df, ttm_df


def ins(ticker):
    
    # Gets income statement data from database
    
    sqlQuery = "select symbol_id, ticker, quarter, sales, operatingincome, pretaxincome, netincome, impairmentexpense, incometax, interestexpense, restructuringexpense, litigationexpense, extinguishmentdebt, nonoperatingexpense, gaapdilutedeps, dps, dilutedsharesos from income_statement where ticker = '{0}'".format(ticker)
    df = pd.read_sql_query(sqlQuery, con = engine)
    
    df = df.rename(columns = {"operatingincome": "opincome", "impairmentexpense": "impairment", 
                              "restructuringexpense": "restructuring", "litigationexpense": "litigation", 
                              "nonoperatingexpense": "nonopexpense", "gaapdilutedeps": "gaapeps", "dilutedsharesos": "sharesos"})
#    print (df)
    if df["opincome"][0] == 0.0:
        df["adjoi"] = df["pretaxincome"] + df["restructuring"] + df["litigation"] + df["impairment"] + df["nonopexpense"] + df["interestexpense"] - df["extinguishmentdebt"]
    else:
        df["adjoi"] = df["opincome"] + df["restructuring"] + df["litigation"] + df["impairment"]

    return df


def ttm_ins(ticker):
    
    # Calculate TTM ins line items from quarterly data
    
    df = ins(ticker)
    df = df[df["quarter"] != "0"]
    df = df.dropna(subset = ["quarter"])
    ttm_df = df
    
    df = df.set_index("quarter")
    ttm_df = ttm_df.set_index("quarter")
    
    for date in df.index:
        quarter = date[0:2]
        year = int(date[2:])
        
        if quarter == "Q1":
            try:
                ttm_df.loc[date, "sales": "adjoi"] = df.loc[date, "sales": "adjoi"] + df.loc["FY"+str(year-1), "sales": "adjoi"] - df.loc["Q1"+str(year-1), "sales": "adjoi"]
            except:
                ttm_df.loc[date, "sales": "adjoi"] = None
                continue
        elif quarter == "Q2":
            try:
                ttm_df.loc[date, "sales": "adjoi"] = df.loc[date, "sales": "adjoi"] + df.loc["Q1"+str(year), "sales": "adjoi"] + df.loc["FY"+str(year-1), "sales": "adjoi"] - df.loc["Q1"+str(year-1), "sales": "adjoi"] - df.loc["Q2"+str(year-1), "sales": "adjoi"]
            except:
                ttm_df.loc[date, "sales": "adjoi"] = None
                continue
        elif quarter == "Q3":
            try:
                ttm_df.loc[date, "sales": "adjoi"] = df.loc[date, "sales": "adjoi"] + df.loc["Q1"+str(year), "sales": "adjoi"] + df.loc["Q2"+str(year), "sales": "adjoi"] + df.loc["FY"+str(year-1), "sales": "adjoi"] - df.loc["Q1"+str(year-1), "sales": "adjoi"] - df.loc["Q2"+str(year-1), "sales": "adjoi"] - df.loc["Q3"+str(year-1), "sales": "adjoi"]
            except:
                ttm_df.loc[date, "sales": "adjoi"] = None
                continue
        elif quarter == "FY" or quarter == "Q4":
            try:
                ttm_df.loc[date, "sales": "adjoi"] = df.loc[date, "sales": "adjoi"]
            except:
                ttm_df.loc[date, "sales": "adjoi"] = None
                continue
    
    ttm_df = ttm_df.dropna()
    
    cols_to_keep = ["symbol_id", "ticker", "sales", "opincome", "pretaxincome", "netincome", "incometax", "gaapeps", "dps", "adjoi", "sharesos"]
    ttm_df = ttm_df[cols_to_keep]
    
    return df, ttm_df


def bs(ticker):
    
    # Gets balance sheet data from the database
    
    sqlQuery = "select symbol_id, ticker, quarter, cashandcashequivalents, assets, shorttermdebt, longtermdebt, equity from balance_sheet where ticker = '{0}'".format(ticker)
    df = pd.read_sql_query(sqlQuery, con = engine)
    
    df = df.rename(columns = {"cashandcashequivalents": "cash", "shorttermdebt": "stdebt",
                              "longtermdebt": "ltdebt"})
        
    df["netdebt"] = df["stdebt"] + df["ltdebt"] - df["cash"]
    
    return df


def merge(ticker):
    
    # Merge the two TTM data dataframes (cfs, ins) with the bs dataframe  
    
    cfs = ttm_cfs(ticker)[1]
    cfs = cfs.reset_index()
    ins = ttm_ins(ticker)[1]
    ins = ins.reset_index()
    bsh = bs(ticker)
    
    dfs = [cfs, ins, bsh]
    
    ttm_df = reduce(lambda left, right: pd.merge(left, right, how = "inner", on = ["quarter", "symbol_id", "ticker"]), dfs)
    
    ttm_df["ebitda"] = ttm_df["adjoi"] + ttm_df["da"] + ttm_df["sbcomp"]
    
    return ttm_df


def fetch_price(ticker, start_date, end_date):
    
    # Daily price data is fetched from the database
    # Rolling returns and standards deviations are calculated for certain time frames 
    
    cur.execute("select symbol.id, symbol.ticker, symbol.sector, daily_price.date, daily_price.close, daily_price.adjClose from symbol join daily_price on symbol.id = daily_price.symbol_id where symbol.ticker = (%s) and daily_price.date >= (%s) and daily_price.date <= (%s)", (ticker, start_date, end_date))
    data = cur.fetchall()
    
    id = []
    ticker = []
    sector = []
    date = []
    close = []
    adjClose = []
    
    df = pd.DataFrame()
    for i in data:
        id.append(i[0])
        ticker.append(i[1])
        sector.append(i[2])
        date.append(i[3])
        close.append(i[4])
        adjClose.append(i[5])
    
    #df[["id", "ticker", "sector", "date", "close", "adj_close"]] = [id, ticker, sector, date, close, adjClose]
    
    df["id"] = id    
    df["ticker"] = ticker
    df["sector"] = sector    
    df["date"] = date
    df["close"] = close
    df["adj_close"] = adjClose
    
    df["daily_return"] = df["adj_close"].pct_change(1)

    # Momemtum factors    
    df["3mon_return"] = df["daily_return"].rolling(window = 63).sum()
    df["6mon_return"] = df["daily_return"].rolling(window = 126).sum()
    df["9mon_return"] = df["daily_return"].rolling(window = 189).sum()
    df["12mon_return"] = df["daily_return"].rolling(window = 252).sum()
    
    # Volatility factors
    df["6mon_std"] = (df["daily_return"].rolling(window = 126).std())*math.sqrt(252)
    df["12mon_std"] = (df["daily_return"].rolling(window = 252).std())*math.sqrt(252)
    df["36mon_std"] = (df["daily_return"].rolling(window = 756).std())*math.sqrt(252)
    
    # Excludes the risk-free rate for the time being (Assumes div yield is equal to risk-free)
    df["12mon_sharpe"] = df["12mon_return"]/df["12mon_std"]
        
    return df


def factor(ticker, start_date, end_date, resample_period = "Q"):
    
    
    # Uses the price dataframe and merged fundamental dataframe to calculate no. of factors
    # Factors are divided into value, quality, growth, volatility and momentum factors and no. of factors
    # are calculated for each factor
    
    
    ttm_df = merge(ticker)
    ttm_df["quarter"] = ttm_df["quarter"].str.replace("FY", "Q4")
    
    price_df = fetch_price(ticker, start_date, end_date)
    price_df["quarter"] = pd.PeriodIndex(price_df["date"], freq = "Q")
    price_df["quarter"] = price_df["quarter"].apply(lambda x: str(x))
    price_df["quarter"] = price_df["quarter"].str[4:6] + price_df["quarter"].str[0:4]
    price_df = price_df[["date", "ticker", "sector", "close", "adj_close", "quarter", "3mon_return", "6mon_return", "9mon_return", "12mon_return", "6mon_std", "12mon_std", "36mon_std", "12mon_sharpe"]]
    
    factor_df = price_df.merge(ttm_df, how = "inner", on = ["quarter", "ticker"])
    
    factor_df = factor_df.set_index("date")
    factor_df = factor_df.resample(resample_period).last()
    factor_df = factor_df.reset_index()
    
    # Calculation of factors
    # Value factors
    try:
        factor_df["ocf_yield"] = factor_df["ocf"]/(factor_df["adj_close"] * factor_df["sharesos"])
    except:
        factor_df["ocf_yield"] = None
    try:
        factor_df["fcf_yield"] = factor_df["fcf"]/(factor_df["adj_close"] * factor_df["sharesos"])
    except:
        factor_df["fcf_yield"] = None
    try:
        factor_df["adjoi/ev"] = factor_df["adjoi"]/((factor_df["adj_close"] * factor_df["sharesos"]) + factor_df["netdebt"])
    except:
        factor_df["adjoi/ev"] = None
    try:
        factor_df["ebitda/ev"] = factor_df["ebitda"]/((factor_df["adj_close"] * factor_df["sharesos"]) + factor_df["netdebt"])
    except:
        factor_df["ebitda/ev"] = None   
    try:
        factor_df["gaap_pe"] = factor_df["gaapeps"]/factor_df["adj_close"]
    except:
        factor_df["gaap_pe"] = None
    try:
        factor_df["b/p"] = factor_df["equity"]/(factor_df["adj_close"] * factor_df["sharesos"])
    except:
        factor_df["b/p"] = None
    try:
        factor_df["div_yield"] = factor_df["dps"]/factor_df["adj_close"]
    except:
        factor_df["div_yield"] = None
    try:
        factor_df["shareholder_yield"] = ((factor_df["dps"] * factor_df["sharesos"]) + factor_df["buyback"])/(factor_df["adj_close"] * factor_df["sharesos"])
    except:
        factor_df["shareholder_yield"] = None
    
    # Quality factors
    try:
        factor_df["roa"] = factor_df["adjoi"]/factor_df["assets"]
    except:
        factor_df["roa"] = None
    try:
        factor_df["roic"] = factor_df["adjoi"]/(factor_df["equity"] + factor_df["ltdebt"] + factor_df["stdebt"])
    except:
        factor_df["roic"] = None
    try:
        factor_df["netdebt_ebitda"] = (factor_df["ltdebt"] + factor_df["stdebt"] - factor_df["cash"])/factor_df["ebitda"]
    except:
        factor_df["netdebt_ebitda"] = None
    try:
        factor_df["fcf_conversion"] = (factor_df["fcf"]/factor_df["sharesos"])/factor_df["gaapeps"]
    except:
        factor_df["fcf_conversion"] = None
    try:
        factor_df["ebitda_margin"] = factor_df["ebitda"]/factor_df["sales"]
        factor_df["ebitda_margin_exp"] = factor_df["ebitda_margin"].diff()
    except:
        factor_df["ebitda_margin"] = None
        factor_df["ebitda_margin_exp"] = None
    
    # Growth factors
    try:
        factor_df["sales_growth"] = (factor_df["sales"]/factor_df["sharesos"]).pct_change(1)
    except:
        factor_df["sales_growth"] = None
    try:
        factor_df["fcf_growth"] = (factor_df["fcf"]/factor_df["sharesos"]).pct_change(1)
    except:
        factor_df["fcf_growth"] = None    
    try:
        factor_df["gaapeps_growth"] = factor_df["gaapeps"].pct_change(1)
    except:
        factor_df["gaapeps_growth"] = None

        
    return factor_df


def database(start_date, end_date, resample_period = "Q"):
    
    
    # The function calls the factor module, checks for any tickers that already exist in the factor table
    # quarters in the database for each quarter before importing the factors in the database
    # Also, resamples the dataframe to a quarterly frequency
    
    
    df = pd.read_sql_query("select ticker from symbol", con = engine)
    tickers = df["ticker"].tolist()
    
    failedTickerLst = []
    # Get current quarters from the ttm table
    for ticker in tickers:
        
        # Checks for tickers that already exist in the factor table
        db_quarter_lst = []
        try:
            sqlQuery = "select quarter from factors where ticker = '{0}'".format(ticker)
            db_quarter_lst = pd.read_sql_query(sqlQuery, con = engine)["quarter"].tolist()
            print ("{0} is an existing ticker in the factor table".format(ticker))
        
        except:
            print ("{0} is a new ticker in the factor table".format(ticker))
            pass
        
        
        factor_df = factor(ticker, start_date, end_date, resample_period)
        df_quarter_lst = factor_df["quarter"].tolist()
        
        # Checks for quarters that already exist for a specific ticker to avoid duplication of data
        for q in df_quarter_lst:
            if q in db_quarter_lst:
                factor_df = factor_df[factor_df["quarter"] != q]
                
            else:
                continue
            
            factor_df = factor_df.dropna(thresh = 50)
        
        print (db_quarter_lst)
        print (df_quarter_lst)
        
        cols_to_drop = ["ocf", "capex", "acquisition", "buyback", "da", "sbcomp", "sales", "opincome", "pretaxincome", 
                        "netincome", "incometax", "gaapeps", "dps", "sharesos", "cash", "assets", "stdebt", "ltdebt", "equity"]
        factor_df = factor_df.drop(cols_to_drop, axis = 1)
        
        # Only reatins factor data post 2015 
        try:
            factor_df = factor_df[factor_df["date"] > "2015-12-31"]
        except:
            pass
        
        # Imports the factor dataframe to the factor table
        try:
            factor_df.to_sql(name = "factors", con = engine, if_exists = "append")
        except:
            failedTickerLst.append(ticker)
            continue

    return factor_df, failedTickerLst


#z = database(start_date = "2012-01-01", end_date = "2019-07-10")

# Note the model breaks down for companies that couldn't be scraped from the edgar filings hence no fundamental data for such companies
# in the database. I haven't built a work around for those names intentionally to keep track of names for which there is no fundamental data.

