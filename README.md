# factor-repo

It leverages the financial and price database to create an security basket using a scoring model based on factors. The factor repository consists of 25 different factors in five different factor categories - 1) Momentum, 2) Volatility, 3) Value, 4) Quality and 5) Growth. 

The user needs to pass the end date of the last quarter and a dictionary consisting of factors and asscociated weights as inputs. The z_score function gets the data for all the factors thats passed in the dictionary from the database, reformats the data and calculate scores (normalized by sector) for all companies in the universe. The companies are then ranked based on the scores and a complete dataframe consisting of metadata, raw factor data and the scores are returned. The basket function calls the z_score function and picks top 15% company from each sector and returns the investment basket.  

This repository is part of a bigger project. Other parts of the project builds the database thats used to create the factor repository and the factor model.

1) Scrapes the edgar database to get financial data of S&P500 companies and stores in a MySQL database
https://github.com/razin11/edgar-scraper
2) Creates a price table of S&P500 companies by requesting data from tiingo 
https://github.com/razin11/security-prices

Check out the above repos to get a better sense of the complete project
