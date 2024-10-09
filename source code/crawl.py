import vnstock
from datetime import datetime, timedelta
import pandas as pd
import utils

def crawl_symbol() -> list:
    companies = vnstock.listing_companies()
    symbols = companies["ticker"].to_list()
    return symbols

def crawl_message(symbol, start_date, end_date, resolution) -> dict:
    message = vnstock.stock_historical_data(symbol=symbol, 
                                    start_date=str(start_date), 
                                    end_date=str(end_date), resolution='1', type='stock', beautify=True, decor=False)
    message = {key: list(value.values())[0] for key, value in message.to_dict().items()}
    #message["time"] = datetime.strptime(message["time"], '%Y-%m-%d').date()
    return message

def crawl_historical_data(symbols, start_date, end_date, resolution='1', filename="historical_data.csv"):
    historical_data = []
    for symbol in symbols:
        try:
            historical_data.append(vnstock.stock_historical_data(symbol=symbol,
                                    start_date=str(start_date), 
                                    end_date=str(end_date), 
                                    resolution=resolution, 
                                    type='stock', 
                                    beautify=True, 
                                    decor=False).to_dict("records"))
        except:
            print(f"{symbol} is not found")
            continue
    pd.DataFrame(utils.mix_keep_internal_order(historical_data)).to_csv(filename, index=False)

if __name__ == "__main__":
    #symbols = ['VCB', 'BID', 'GAS', 'HPG', 'VHM', 'FPT', 'CTG', 'VIC', 'TCB', 'VPB']
    symbols = crawl_symbol()
    current_datetime = datetime.today()
    current_date = current_datetime.date()
    start_date = current_date - timedelta(days=1)
    end_date = current_date
    #print(crawl_message("SSI", start_date, end_date))
    crawl_historical_data(symbols, start_date, end_date, '1', "StockPrices.csv")

