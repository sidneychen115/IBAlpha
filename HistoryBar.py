from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from threading import Thread
from ibapi.common import BarData
from MyContract import  MyContract
from datetime import datetime
import pandas as pd
from ibapi.contract import Contract

class IBMarketData(EClient, EWrapper):

    def __init__(self, ipaddress, portid, clientid, history_queue, history_update_bar_queue):
        EClient.__init__(self, self)
        self.connect(ipaddress, portid, clientid)
        self.history_queue = history_queue
        self.hub_queue = history_update_bar_queue
        #self.live_data_queue = live_data_queue
        self.history_bar_list = []
        self.last_bar_date = ""
        self.cache_list = []
        #self.logger = logger
        thread = Thread(target=self.run, name='IBMarketData', daemon=True)
        thread.start()
        setattr(self, "_thread", thread)

    def historicalData(self, reqId: int, bar: BarData):
        self.history_bar_list.append([bar.date, bar.open, bar.close, bar.high, bar.low, bar.volume])


    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        #last line is incomplete, remove
        last_bar = self.history_bar_list.pop(-1)
        self.last_bar_date = last_bar[0]
        df_history_bar = pd.DataFrame(self.history_bar_list, columns=["Date", "Open", "Close", "High", "Low", "Volume"])
        self.history_queue.put(df_history_bar)

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        bar_dict = {
            'Date': bar.date,
            'Open': bar.open,
            'Close': bar.close,
            'High': bar.high,
            'Low': bar.low,
            'Volume': bar.volume,
            'SMA_30': -1,
            'EMA_8': -1,
            'EMA_14': -1,
            'Change': -1,
            'Gain': -1,
            'Loss': -1,
            'AvgGain': -1,
            'AvgLoss': -1,
            'RS': -1,
            'RSI': -1
        }
        newrow = pd.Series(bar_dict)
        self.hub_queue.put(newrow)


