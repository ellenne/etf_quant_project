import yfinance as yf

ticker = "IWDA.AS"
df = yf.download(ticker, start="2020-01-01", progress=False)
print(df.head())
print(df.columns)