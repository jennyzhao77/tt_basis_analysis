import asyncio
import datetime
import os
import requests
import urllib
import pickle
import pandas as pd
import logging
import json
from collections import deque
from log import get_logger
import time as tm
import aiohttp
import numpy as np

import zmq
import zmq.asyncio
from zmq.asyncio import Context


from copy import deepcopy

LOG = get_logger('basisbot','bot.log', logging.INFO)

SPOT_MARGIN=False
PERP_OPEN_RATIO=0.998
# PERP_CLOSE_RATIO=0.994
POS_ACCUMULATION_OPEN_UTIL=0.5
POS_ACCUMULATION_OPEN_SPREAD=1.0
POS_ACCUMULATION_CLOSE_UTIL = 0.75
CLOSE_RATIO_ADDON = 0.00005
OPEN_PERCENTILE = 0.99
CLOSE_PERCENTILE = 0.3
TAKER_FEE=0.0003
POS_LIMIT=50000
OPEN_CLIP_LIMIT = [20, 2000]
CLOSE_CLIP_LIMIT = [20, 2000]
PRINT_BOOK=False
STREAMING=False

class Book:
    timestamp = None
    bids = list()
    asks = list()


    def __repr__(self):
        return f"{self.timestamp.timestamp()}, {self.bids[0][0]}, {self.asks[0][0]}"

class BasisBot:

    def __init__ (self, symbol, dir):
        
        self.spot_files = []
        self.perp_files = []

        self.spot_queue = deque()
        self.perp_queue = deque()
        self.fr_queue = deque()

        self.rolling_open = deque()
        self.rolling_close = deque()

        self.symbol = symbol
        self.dir = dir
        self.pnl = 0
        self.position = 0
        self.pos_limit = POS_LIMIT
        self.pos_avg_spread = 0
        self.clip_limit_open = OPEN_CLIP_LIMIT.copy()
        self.clip_limit_close = CLOSE_CLIP_LIMIT.copy()
        self.open_ratio = PERP_OPEN_RATIO
        self.close_ratio = 0
        self.open_pct = OPEN_PERCENTILE
        self.close_pct = CLOSE_PERCENTILE
        self.pos_accumulation_open_spread = POS_ACCUMULATION_OPEN_SPREAD
        self.pos_accumulation_open_util = POS_ACCUMULATION_OPEN_UTIL
        self.pos_accumulation_close_util = POS_ACCUMULATION_CLOSE_UTIL
        self.close_ratio_addon = CLOSE_RATIO_ADDON
        self.taker_fee = TAKER_FEE

        url = '8000'
        port = 8000
        self.url = f"tcp://{url}:{port}"
        self.ctx = Context.instance()

        ts = f"{int(datetime.datetime.now().timestamp())}"
        self.signal_file = open(f"{self.dir}/{ts}_signal.csv","a+")
        self.signal_file.write('ts, side, size, basis, pos, pnl, pos_avg_spread, PERP_OPEN_RATIO,PERP_CLOSE_RATIO,POS_LIMIT,CLIP_LIMIT_LOWER,CLIP_LIMIT_UPPER\n')
        self.fr_file = open(f"{self.dir}/{ts}_fr.csv","a+")
        self.fr_file.write('ts, pos, rate, payment\n')
        self.fee_file = open(f"{self.dir}/{ts}_fee.csv","a+")
        self.fee_file.write('ts, volume, rate, fee\n')
        self.param_file = open(f"{self.dir}/{ts}_param.csv", "a+")
        self.param_file.write('PERP_OPEN_RATIO,POS_ACCUMULATION_OPEN_SPREAD,OPEN_PERCENTILE,CLOSE_PERCENTILE,OPEN_CLIP_LIMIT,'
                              'CLOSE_CLIP_LIMIT,POS_LIMIT,CLOSE_RATIO_ADDON,POS_ACCUMULATION_CLOSE_UTIL,\n')
        self.param_file.write(f'{PERP_OPEN_RATIO},{POS_ACCUMULATION_OPEN_SPREAD},{OPEN_PERCENTILE},{CLOSE_PERCENTILE},{OPEN_CLIP_LIMIT},{CLOSE_CLIP_LIMIT},'
                              f'{POS_LIMIT},{CLOSE_RATIO_ADDON},{POS_ACCUMULATION_CLOSE_UTIL}\n')


    async def subscribe(self, topics):
        LOG.info(f"Subscribing topic: {topics}")
        sub = self.ctx.socket(zmq.SUB)
        sub.bind(self.url)
        for topic in topics:
            sub.setsockopt(zmq.SUBSCRIBE, topic.encode('utf-8'))
        try:
            while True:
                [topic, msg] = await sub.recv_multipart()
                await self.message_handler(topic.decode('utf-8'), msg.decode('utf-8'))

        except Exception as e:
            LOG.error(e)


    async def message_handler(self, topic, msg):
        msg = json.loads(msg)
        if topic == 'spot':
            book = Book()
            book.timestamp = pd.to_datetime(msg['timestamp'])
            book.asks = msg['asks']
            book.bids = msg['bids']
            self.spot_queue.append(book)
        elif topic == 'perp':
            book = Book()
            book.timestamp = pd.to_datetime(msg['timestamp'])
            book.asks = msg['asks']
            book.bids = msg['bids']
            self.perp_queue.append(book)
        elif topic =='exit':
            LOG.info(f"Result pnl={self.pnl:.4f}, position={self.position}")
            loop = asyncio.get_event_loop()
            loop.stop()

    def saveSignal(self, ts, side, size, basis, pos, pnl ):
        self.signal_file.write(f"{ts},{side},{size}, {basis}, {pos}, {pnl}\n")


    def getFundingRate(self, end, symbol):
        url = f"https://ftx.com/api/funding_rates"
        end = int(end.replace(tzinfo=datetime.timezone.utc).timestamp())
        query = {'end_time': end, 'future': f'{symbol}-PERP'}
        query = urllib.parse.urlencode(query)
        response = requests.get(f"{url}?{query}")
        data = response.json()['result']
        return data[0]['time'], data[0]['rate']

    def getFundingRateFromFile(self, end, symbol):
        df = pd.read_csv(f'{self.dir}/fundings.csv')
        df['ts'] = pd.to_datetime(df['ts'])
        end = end.replace(tzinfo=datetime.timezone.utc)
        df = df[df["ts"] <= end].iloc[-1]

        return df[0].replace(tzinfo=datetime.timezone.utc), df[1]


    async def loadData(self, topic):
        
        if topic=='spot' and self.spot_files:
            LOG.info(f'Reading spot file: {self.spot_files[-1]}')
            df = pd.read_csv(self.spot_files.pop()).to_numpy()

            asks_price = df[:,4:41:4]
            ask_amounts = df[:,5:42:4] 
            bids_price = df[:,6:43:4]
            bids_amounts = df[:,7:44:4]

            for i in range(asks_price.shape[0]):
                book = Book()
                book.timestamp = pd.to_datetime(df[i][2]).replace(tzinfo=datetime.timezone.utc)
                book.bids = [ [bids_price[i][depth], bids_amounts[i][depth] ]  for depth in range(bids_price.shape[1]) ]
                book.asks = [ [asks_price[i][depth], ask_amounts[i][depth] ] for depth in range(asks_price.shape[1]) ]
                self.spot_queue.append(book)

            del df
            return True

        if topic=='perp' and self.perp_files:
            LOG.info(f'Reading perp file: {self.perp_files[-1]}')
            df = pd.read_csv(self.perp_files.pop()).to_numpy()

            asks_price = df[:,4:41:4]
            ask_amounts = df[:,5:42:4]
            bids_price = df[:,6:43:4]
            bids_amounts = df[:,7:44:4]

            for i in range(asks_price.shape[0]):
                book = Book()
                book.timestamp = pd.to_datetime(df[i][2]).replace(tzinfo=datetime.timezone.utc)
                book.bids = [ [bids_price[i][depth], bids_amounts[i][depth] ]  for depth in range(bids_price.shape[1]) ]
                book.asks = [ [asks_price[i][depth], ask_amounts[i][depth] ] for depth in range(asks_price.shape[1]) ]
                self.perp_queue.append(book)

            del df
            return True

        return False

    async def algo(self):

        spot_book = None
        perp_book = None

        _spot_book, _perp_book = None, None
        last_open_time, last_close_time = datetime.datetime(2020,1,1).replace(tzinfo=datetime.timezone.utc), datetime.datetime(2020,1,1).replace(tzinfo=datetime.timezone.utc)
        last_funding = datetime.datetime(2020,1,1).replace(tzinfo=datetime.timezone.utc)
        rolling_start = None; pos_accumulation_start = None;
        
        while True:
            if not self.spot_queue or not self.perp_queue:
                if not STREAMING:
                    res=False
                    if not self.spot_queue:
                        res = await self.loadData('spot')
                    if not self.perp_queue:
                        res = await self.loadData('perp')
                    if not res:
                        LOG.info(f'Reached end of files')
                        break
                else:
                    await asyncio.sleep(0.5)
                    continue

            if self.spot_queue[0].timestamp < self.perp_queue[0].timestamp:
                spot_book = self.spot_queue[0]; self.spot_queue.popleft()
                _spot_book = deepcopy(spot_book)
            else:
                perp_book = self.perp_queue[0]; self.perp_queue.popleft()
                _perp_book = deepcopy(perp_book)

            if not spot_book or not perp_book or abs((spot_book.timestamp - perp_book.timestamp).total_seconds())>=1:
                continue

            ts = min(spot_book.timestamp, perp_book.timestamp)

            ##  apply rolling open/close ratio
            if not rolling_start:
                rolling_start = ts + datetime.timedelta(hours=1)
            if ts <= rolling_start:
                self.rolling_open.append(perp_book.bids[0][0] / spot_book.asks[0][0])
                self.rolling_close.append(perp_book.asks[0][0] / spot_book.bids[0][0])
                continue

            self.rolling_open.popleft(); self.rolling_open.append(perp_book.bids[0][0] / spot_book.asks[0][0])
            self.rolling_close.popleft(); self.rolling_close.append(perp_book.asks[0][0] / spot_book.bids[0][0])

            open_percentile = np.quantile(self.rolling_open, self.open_pct)
            close_percentile = np.quantile(self.rolling_open, self.close_pct)
            if -self.position / self.pos_limit  >= self.pos_accumulation_open_util:
                self.open_ratio = max(self.pos_accumulation_open_spread, open_percentile) # apply a more aggressive floor on rolling open ratio

            else:
                self.open_ratio = max(PERP_OPEN_RATIO, open_percentile) # apply a floor on rolling open ratio

            ## update close ratio only if it's 10bps less than current avg open spread, otherwise we would close at a loss
            if (self.pos_avg_spread - close_percentile) >= 0.001:
                self.close_ratio = close_percentile
            else:
                self.close_ratio = close_percentile - 0.001

            ## if we have accumulated large positions for a long time, we close more aggressively, even at a loss
            positon_filled_pct = - self.position / self.pos_limit

            if not pos_accumulation_start and positon_filled_pct >= self.pos_accumulation_close_util:
                pos_accumulation_start = ts
            elif positon_filled_pct < self.pos_accumulation_close_util:
                pos_accumulation_start = None

            if pos_accumulation_start and (ts - pos_accumulation_start) > datetime.timedelta(hours=1):
                self.close_ratio = self.close_ratio + self.close_ratio_addon * positon_filled_pct * ((ts - pos_accumulation_start).total_seconds() / 3600) ** 2


            if ts >= last_funding:
                time, rate = self.getFundingRate(ts,self.symbol)
                funding_payment = -self.position * rate
                self.pnl += funding_payment
                # LOG.info(f'[funding] ts={ts}, position={self.position}, time={time}, rate={rate}, total={funding_payment:.4f}, new_pnl={self.pnl:.4f}')
                self.fr_file.write(f'{ts}, {self.position}, {rate}, {funding_payment}\n')

                last_funding = ts.replace(minute=0,second=0) + datetime.timedelta(hours=1)



            ## compute open - short PERP, buy SPOT
            if SPOT_MARGIN:
                max_clip = min(self.clip_limit_open[1], self.pos_limit + self.position)
            else:
                max_clip = max(0, min(self.clip_limit_open[1], self.pos_limit + self.position)) #perp pos should be <= 0, since no short selling of spot

            bid_depth_index, ask_depth_index = 0,0
            sum_perp_bid_price, sum_spot_ask_price = 0,0
            result_clip_size = 0
            while max_clip and max(bid_depth_index, ask_depth_index) < len(spot_book.bids):
                take_out_size = min( perp_book.bids[bid_depth_index][1], spot_book.asks[ask_depth_index][1], max_clip )
                if take_out_size==0:
                    break

                sum_perp_bid_price += perp_book.bids[bid_depth_index][0] * take_out_size
                sum_spot_ask_price += spot_book.asks[ask_depth_index][0] * take_out_size

                if sum_perp_bid_price/sum_spot_ask_price <= self.open_ratio:
                    sum_perp_bid_price -= perp_book.bids[bid_depth_index][0] * take_out_size
                    sum_spot_ask_price -= spot_book.asks[ask_depth_index][0] * take_out_size
                    break

                result_clip_size += take_out_size

                perp_book.bids[bid_depth_index][1] -= take_out_size
                spot_book.asks[ask_depth_index][1] -= take_out_size
                max_clip -= take_out_size

                if perp_book.bids[bid_depth_index][1] == 0: bid_depth_index += 1
                if spot_book.asks[ask_depth_index][1] == 0: ask_depth_index += 1
                if max_clip ==0:
                    break

            if result_clip_size >= self.clip_limit_open[0]:
                ## open position !
                if ts - last_open_time > datetime.timedelta(seconds=60):
                    self.pos_avg_spread = (- self.pos_avg_spread * self.position + (sum_perp_bid_price/sum_spot_ask_price) * result_clip_size) / (-self.position + result_clip_size)
                    self.pnl += sum_perp_bid_price - sum_spot_ask_price - (sum_perp_bid_price + sum_spot_ask_price)*self.taker_fee
                    self.position -= result_clip_size

                    LOG.info(f'[Open] ts={ts}, clip_size={result_clip_size}, basis_ratio={sum_perp_bid_price/sum_spot_ask_price:.4f}, '
                             f'new_pos={self.position}, pnl={self.pnl:.4f}')

                    self.signal_file.write(f"{ts}, open, {result_clip_size}, {sum_perp_bid_price/sum_spot_ask_price}, {self.position}, {self.pnl}, {self.pos_avg_spread},{self.open_ratio},{self.close_ratio},"
                                           f"{self.pos_limit},{self.clip_limit_open[0]},{self.clip_limit_open[1]}\n")
                    self.fee_file.write(f"{ts}, {sum_perp_bid_price + sum_spot_ask_price}, {self.taker_fee}, {(sum_perp_bid_price + sum_spot_ask_price)*self.taker_fee}\n")

                    last_open_time = ts

                    if PRINT_BOOK:
                        LOG.info(f'bids: {_perp_book.bids}')
                        LOG.info(f'asks: {_spot_book.asks}')


            ## compute close - buy PERP, sell SPOT
            if SPOT_MARGIN:
                max_clip = min(self.clip_limit_close[1], self.pos_limit - self.position)
            else:
                max_clip = max(0, min(self.clip_limit_close[1], - self.position))#perp pos should be <= 0, since no short selling of spot.

            bid_depth_index, ask_depth_index = 0,0
            sum_perp_ask_price, sum_spot_bid_price = 0,0
            result_clip_size = 0

            while max_clip and max(bid_depth_index, ask_depth_index) < len(spot_book.bids):
                take_out_size = min( perp_book.asks[ask_depth_index][1], spot_book.bids[bid_depth_index][1], max_clip )
                if take_out_size==0:
                    break
                
                sum_perp_ask_price += perp_book.asks[ask_depth_index][0] * take_out_size
                sum_spot_bid_price += spot_book.bids[bid_depth_index][0] * take_out_size

                if sum_perp_ask_price/sum_spot_bid_price >= self.close_ratio:
                    sum_perp_ask_price -= perp_book.asks[ask_depth_index][0] * take_out_size
                    sum_spot_bid_price -= spot_book.bids[bid_depth_index][0] * take_out_size
                    break

                result_clip_size += take_out_size

                perp_book.asks[ask_depth_index][1] -= take_out_size
                spot_book.bids[bid_depth_index][1] -= take_out_size
                max_clip -= take_out_size

                if perp_book.asks[ask_depth_index][1] == 0: ask_depth_index += 1
                if spot_book.bids[bid_depth_index][1] == 0: bid_depth_index += 1
                if max_clip ==0:
                    break

            if result_clip_size >= self.clip_limit_close[0]:
                ## close position !
                if ts - last_close_time > datetime.timedelta(seconds=60):
                    self.pnl += -sum_perp_ask_price + sum_spot_bid_price - (sum_perp_ask_price + sum_spot_bid_price)*self.taker_fee
                    self.position += result_clip_size
                    LOG.info(f'[Close] ts={ts}, clip_size={result_clip_size}, basis_ratio={sum_perp_ask_price/sum_spot_bid_price:.4f}, '
                             f'new_pos={self.position}, pnl={self.pnl:.4f}')

                    self.signal_file.write(f"{ts}, close, {result_clip_size}, {sum_perp_ask_price/sum_spot_bid_price}, {self.position}, {self.pnl}, {self.pos_avg_spread},{self.open_ratio},{self.close_ratio},"
                                           f"{self.pos_limit},{self.clip_limit_close[0]},{self.clip_limit_close[1]}\n")
                    self.fee_file.write(f"{ts}, {sum_perp_ask_price + sum_spot_bid_price}, {self.taker_fee}, {(sum_perp_ask_price + sum_spot_bid_price)*self.taker_fee}\n")

                    last_close_time = ts

                    if PRINT_BOOK:
                        LOG.info(f'asks: {_perp_book.asks}')
                        LOG.info(f'bids: {_spot_book.bids}')

            # await asyncio.sleep(1E-9)


        LOG.info(f"Result pnl={self.pnl:.4f}, position={self.position}")
        loop = asyncio.get_event_loop()
        loop.stop()
        self.signal_file.flush(); self.fee_file.flush(); self.fr_file.flush();self.param_file.flush()
        self.signal_file.close(); self.fee_file.close(); self.fr_file.close();self.param_file.close()


    def run(self):
        loop = asyncio.get_event_loop()
        # loop.run_until_complete(self.loadData())

        files = os.listdir(self.dir)
        self.spot_files = sorted([ f'{self.dir}/{f}'  for f in files if 'book' in f and self.symbol in f and 'USD' in f ], reverse=True)
        self.perp_files = sorted([ f'{self.dir}/{f}'  for f in files if 'book' in f and self.symbol in f and 'PERP' in f], reverse=True)

        # loop.create_task(self.subscribe(['spot','perp','exit']))
        loop.create_task(self.algo())
        # loop.create_task(self.algo)
        loop.run_forever()


if __name__ == '__main__':

    t1 = tm.time()
    bb = BasisBot(symbol="LOOKS", dir='./data_looks_testing')
    bb.run()
    t2 = tm.time()
    print(f"Time elapsed...{(t2-t1)/60} min")