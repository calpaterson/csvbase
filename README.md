<div align="center">
    <img alt="csvbase logo" src="https://github.com/calpaterson/csvbase/raw/main/csvbase/static/logo/128x128.png">
</div>

# csvbase

## The grand plan

1. Paste your CSV file, receive free CRUD REST API, easy pandas export, etc
2. ???
3. Profit

## See an example

My current demo is
[meripaterson/stock-exchanges](https://csvbase.com/meripaterson/stock-exchanges),
which is a list of the world's stock exchanges.

If you want to load this dataset into pandas, for example, you would run:

```python
>>> import pandas
>>> # you just put in the same url as on the website, bit of magic here
>>> stock_exchanges = pandas.read_csv("https://csvbase.com/meripaterson/stock-exchanges")
>>> stock_exchanges
     csvbase_row_id      Continent                    Country                                     Name   MIC Last changed
0                 1         Africa                    Lesotho                                    HYBSE   NaN   2019-03-25
1                 2           Asia                Kazakhstan     Astana International Financial Centre  AIXK   2018-11-18
2                 3         Africa              South Africa                                     ZAR X  ZARX   2018-11-18
3                 4  South America                 Argentina              Bolsas y Mercados Argentinos   NaN   2018-04-02
4                 5  North America  United States of America                   Delaware Board of Trade   NaN   2018-04-02
..              ...            ...                        ...                                      ...   ...          ...
246             247  North America  United States of America                  Long-Term Stock Exchange  LTSE   2020-09-14
247             248  North America  United States of America   Miami International Securities Exchange  MIHI   2020-09-24
248             249  North America  United States of America                         Members' Exchange   NaN   2020-09-24
249             250         Africa                  Zimbabwe             Victoria Falls Stock Exchange   NaN   2020-11-01
250             251           Asia                     China                    Beijing Stock Exchange   NaN   2021-12-27

[251 rows x 6 columns]
```

## In a little more details

This repo has the full sourcecode for the website.

The website is free to use, to the limit of my cheapo VM.

At the moment it's is alpha-quality: "if you break it you get to keep both
pieces".  APIs may be tweaked but what is there now probably won't change
hugely.

Csvbase is licensed under the AGPLv3 or later.
