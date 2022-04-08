import asyncio
import aiohttp
import json
import urllib.parse
import datetime as dt
import pandas as pd
import os


async def run(data_file, instrument, from_date, to_date):
    replay_options = {
        "exchange": "ftx",
        "from": from_date,
        "to": to_date,
        "symbols": [instrument],
        "withDisconnectMessages": False,
        "dataTypes": ["book_snapshot_10_1s"],
    }

    options = urllib.parse.quote_plus(json.dumps(replay_options))

    URL = f"ws://localhost:8001/ws-replay-normalized?options={options}"

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(URL) as websocket:
            async for msg in websocket:
                data = json.loads(msg.data)
                res_str = ''
                for i in range(10):
                    res_str += str(data["asks"][i]["price"]) + ','
                    res_str += str(data["asks"][i]["amount"]) + ','
                    res_str += str(data["bids"][i]["price"]) + ','
                    res_str += str(data["bids"][i]["amount"]) + ','

                data_file.write(f'{data["exchange"]},{data["symbol"]},{data["timestamp"]},{data["localTimestamp"]},{res_str} \n')

def load_tardis_data(dir, sym, date_start, days):

    if not os.path.exists(dir):
        os.makedirs(dir)

    instruments = [f'{sym}/USD', f'{sym}-PERP']

    for d in pd.date_range(date_start, periods=days).tolist():
        print(f"loading {d} data...")
        for n in range(len(instruments)):
            ins = instruments[n]
            file = f"ftx_book_snapshot_10_1s_{d.strftime('%Y-%m-%d')}_{instruments[n].replace('/','-')}.csv"
            data_file = open(f"./{dir}/{file}", "a+")
            header_str = 'exch,symbol,timestamp,local_timestamp,'

            for i in range(10):
                header_str += f'asks[{i}].price'+ ','
                header_str += f'asks[{i}].amount'+ ','
                header_str += f'bids[{i}].price'+ ','
                header_str += f'bids[{i}].amount'+ ','
            data_file.write(f'{header_str}\n')
            asyncio.run(run(data_file, ins, d.strftime('%Y-%m-%d %H:%M:%S'), (d + dt.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')  ))

if __name__ == '__main__':

    sym = 'LOOKS'
    dir = "data_looks_testing"
    date_start = dt.datetime(2022, 3, 22, 00,00,00)
    days = 7

    load_tardis_data(dir, sym, date_start, days)

