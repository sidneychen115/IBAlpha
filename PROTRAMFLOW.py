import queue
import time
import logging
from HistoryBar import IBMarketData
from MyContract import MyContract
from IBOrder import IBOrder
from IBOrder import StatusOrder
import pandas as pd
from enum import Enum


class ActionType(Enum):
    noAction = 0
    buyOpen = 1
    sellOpen = 3
    buyClose = 6
    sellClose = 2


class OrderType(Enum):
    market = 0
    limit = 1
    stop = 2


class Strategy():
    def __init__(self, actionType=0, quantity=1, orderType=0, price=0):
        self.action = actionType
        self.quantity = quantity
        self.orderType = orderType
        self.price = price


def int_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    c_format = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)


'''
Calculate and add new rows to the dataframe
'''


def calculateTechnical(df, newrow):
    col_close = df.columns.get_loc('Close')
    col_gain = df.columns.get_loc('Gain')
    col_loss = df.columns.get_loc('Loss')
    col_avggain = df.columns.get_loc('AvgGain')
    col_avgloss = df.columns.get_loc('AvgLoss')
    col_rs = df.columns.get_loc('RS')
    col_rsi = df.columns.get_loc('RSI')
    col_ema8 = df.columns.get_loc('EMA_8')
    col_ema14 = df.columns.get_loc('EMA_14')
    col_sma30 = df.columns.get_loc('SMA_30')
    if df.iloc[-1, col_avggain] < 0:
        avg_gain = df.iloc[1:, col_gain].sum() / 14
        avg_loss = df.iloc[1:, col_loss].sum() / 14
        df.iloc[-1, col_avggain] = avg_gain
        df.iloc[-1, col_avgloss] = avg_loss
        df.iloc[-1, col_rs] = avg_gain / avg_loss
        df.iloc[-1, col_rsi] = 100 - 100 / (1 + df.iloc[-1, col_rs])

    newrow['Change'] = newrow['Close'] - df.iloc[-1, col_close]
    if newrow['Change'] > 0:
        newrow['Gain'] = newrow['Change']
        newrow['Loss'] = 0
    else:
        newrow['Gain'] = 0
        newrow['Loss'] = abs(newrow['Change'])
    newrow['AvgGain'] = (df.iloc[-1, col_avggain] * 13 + newrow['Gain']) / 14
    newrow['AvgLoss'] = (df.iloc[-1, col_avgloss] * 13 + newrow['Loss']) / 14
    newrow['RS'] = newrow['AvgGain'] / newrow['AvgLoss']
    newrow['RSI'] = 100 - 100 / (1 + newrow['RS'])
    newrow['EMA_8'] = (newrow['Close'] - df.iloc[-1, col_ema8]) * 2 / 9 + df.iloc[-1, col_ema8]
    newrow['EMA_14'] = (newrow['Close'] - df.iloc[-1, col_ema14]) * 2 / 15 + df.iloc[-1, col_ema14]
    df = df.append(newrow, ignore_index=True)
    df.iloc[:, col_sma30] = df.loc[:, 'Close'].rolling(window=30).mean()
    return df


def strategyMaker(df, current_position):
    # OPEN Position only when there is no position
    if current_position == 0:
        col_rsi = df.columns.get_loc('RSI')
        col_close = df.columns.get_loc('Close')
        if df.iloc[-2, col_rsi] < 30 and df.iloc[-1, col_rsi] > 31:
            limitPrice = df.iloc[-1, col_close] + 0.2
            strategy = Strategy(ActionType.buyOpen, 1, OrderType.limit, limitPrice)
        elif df.iloc[-2, col_rsi] > 70 and df.iloc[-1, col_rsi] < 69:
            limitPrice = df.iloc[-1, col_close] - 0.2
            strategy = Strategy(ActionType.sellOpen, 1, OrderType.limit, limitPrice)
        else:
            strategy = Strategy(ActionType.noAction)
        return strategy

    '''
    #Sell close 
    if current_position > 0:
        col_close = df.columns.get_loc('Close')
        col_ema8 = df.columns.get_loc('EMA_8')
        col_ema14 = df.columns.get_loc('EMA_14')
        col_sma30 = df.columns.get_loc('SMA_30')
    '''


'''
Main Function
'''


def main():
    '''
    Init logger
    '''
    int_logging()
    logger = logging.getLogger(__name__)

    '''
    Define all variables
    '''
    MARKET_DATA_ID = 1
    ORDER_DATA_ID = 2
    ORDER_ID = -50
    ACCOUNT_SUMMARY_QUEUE = queue.LifoQueue()
    POSITION_QUEUE = queue.LifoQueue()
    HISTORY_QUEUE = queue.Queue()
    HISTORY_UPDATE_BAR_QUEUE = queue.Queue()
    ORDER_STATUS_QUEUE = queue.Queue()
    GOLD_CONTRACT = MyContract.GoldFuture()
    ORDER_DICT = {}

    '''
    istablish IB client connections
    IBMARKET DATA CONNECTION:  Account Summary, History Data 
    IBORDER CONNECTION:  NEXT VALID id.  Position.  Order Update.  Place Order
    '''
    con_market_data = IBMarketData("127.0.0.1", 4002, MARKET_DATA_ID, HISTORY_QUEUE, HISTORY_UPDATE_BAR_QUEUE)
    con_order = IBOrder("127.0.0.1", 4002, ORDER_DATA_ID, ACCOUNT_SUMMARY_QUEUE, POSITION_QUEUE, ORDER_STATUS_QUEUE)

    logger.debug("Set connection completed")

    '''
    Get Next Valid ID.
    '''
    logger.debug("Get valid ID")
    con_order.reqIds(-1)
    counter = 0
    while ORDER_ID < 0 and counter < 5:
        time.sleep(1)
        ORDER_ID = con_order.get_next_valid_id()
        counter += 1
    if ORDER_ID < 0:
        logger.error('Get valid ID failed. Exit')
        exit(-1)
    logger.info("Next valid ID is " + str(ORDER_ID))

    '''
    Get Account Summary. Need current position
    '''
    con_order.reqAccountUpdates(True, "DU1670918")
    counter = 0
    while ACCOUNT_SUMMARY_QUEUE.empty() and counter < 10:
        time.sleep(1)
        counter += 1
    if ACCOUNT_SUMMARY_QUEUE.empty():
        logger.error('Get Account Summary. Exit')
        exit(-1)
    CURRENT_POSITION = ACCOUNT_SUMMARY_QUEUE.get()
    logger.info(str(CURRENT_POSITION))

    '''
    Get History Data.  Key.  Retry 3 times if failed. 
    '''
    logger.debug("Start to request history data")
    con_market_data.reqHistoricalData(15, contract=GOLD_CONTRACT, endDateTime="", durationStr="1 D",
                                      barSizeSetting="5 mins", whatToShow="TRADES", useRTH=0, formatDate=1,
                                      keepUpToDate=True, chartOptions=[])
    counter = 0
    while HISTORY_QUEUE.empty() and counter < 20:
        time.sleep(1)
        counter += 1
    if HISTORY_QUEUE.empty():
        logger.error('Get History Bar Failed. Exit')
        exit(-1)
    DF_HISTORY = HISTORY_QUEUE.get()

    '''
    For Debug

    DF_HISTORY.to_csv('C:\TradingLog\TestData.csv', index=False)
    '''

    logger.debug("Load History Data Complete Successfully")

    '''
    Subscribe to position
    '''
    logger.debug("Subscribe Position Update")
    con_order.reqPositions()

    '''
    Calculate DF_HISTORY
    '''
    pd.set_option('mode.chained_assignment', None)
    df_history = DF_HISTORY.copy()
    df_history['Date'] = pd.to_datetime(df_history['Date'], infer_datetime_format=True)
    df_history.loc[:, 'SMA_30'] = df_history.loc[:, 'Close'].rolling(window=30).mean()
    df_history.loc[:, 'EMA_8'] = df_history.loc[:, 'Close'].ewm(span=8, adjust=False).mean()
    df_history.loc[:, 'EMA_14'] = df_history.loc[:, 'Close'].ewm(span=14, adjust=False).mean()
    df_history.loc[:, 'Change'] = df_history.Close.diff()
    df_history.loc[:, 'Gain'] = df_history['Change']
    df_history.loc[df_history.Gain < 0, 'Gain'] = 0
    df_history.loc[:, 'Loss'] = df_history['Change']
    df_history.loc[df_history.Loss > 0, 'Loss'] = 0
    df_history.loc[:, 'Loss'] = df_history.loc[:, 'Loss'].abs()
    df_history.loc[:, 'AvgGain'] = -1
    df_history.loc[:, 'AvgLoss'] = -1
    df_history.loc[:, 'RS'] = -1
    df_history.loc[:, 'RSI'] = -1

    df_head = df_history.iloc[0:15]
    df_rest = df_history.iloc[15:]

    for row_index, row in df_rest.iterrows():
        df_head = calculateTechnical(df_head, row)

    DF_HISTORY = df_head.copy()
    print(DF_HISTORY)
    '''
      For Debug

       DF_HISTORY.to_csv('C:\TradingLog\TestData.csv', index=False)
      '''

    previous_row = None
    while True:
        time.sleep(1)
        # Update order status. Get all items from the queue snapshot
        statusOrder_List = []
        for _ in range(ORDER_STATUS_QUEUE.qsize()):
            statusOrder_List.append(ORDER_STATUS_QUEUE.get())
        for _ in range(len(statusOrder_List)):
            statusOrder = statusOrder_List[_]
            if statusOrder.orderId in ORDER_DICT:
                ORDER_DICT[statusOrder.orderId].permId = statusOrder.permId
                ORDER_DICT[statusOrder.orderId].status = statusOrder.status
                ORDER_DICT[statusOrder.orderId].filledPrice = statusOrder.filledPrice
                ORDER_DICT[statusOrder.orderId].permId = statusOrder.permId
            else:
                logger.error("One Order is not saved in the dictionary. Order ID: " + str(statusOrder.orderId))

        if not HISTORY_UPDATE_BAR_QUEUE.empty():
            new_row = HISTORY_UPDATE_BAR_QUEUE.get()
            df_temp = DF_HISTORY.copy()
            if previous_row is None:
                previous_row = new_row
            # New 5 mins arrives
            if previous_row['Date'] != new_row['Date']:
                logger.debug("New 5 mins bar arrives. Add to history dataframe")
                DF_HISTORY = calculateTechnical(DF_HISTORY, previous_row)

            df_temp = calculateTechnical(df_temp, new_row)

            # Update current position before strategy
            if not ACCOUNT_SUMMARY_QUEUE.empty():
                CURRENT_POSITION = ACCOUNT_SUMMARY_QUEUE.get()

            strategy = strategyMaker(df_temp, CURRENT_POSITION)

            if strategy.action == ActionType.noAction:
                continue

            if strategy.action == ActionType.buyOpen:
                # Placed open order and stop order
                #placeOrder()

                # Store orders in the dictionary
                openOrder = StatusOrder(orderId=openOrderId, symbol='GC', action=stragety.action,
                                        orderType=stragety.orderType, quantity=stragety.quantity, price=stragety.price)
                ORDER_DICT[openOrderId] = openOrder

                stopOrder = StatusOrder(orderId=stopOrderId, symbol='GC', action=stragety.action * 2,
                                        orderType=OrderType.stop, quantity=stragety.quantity, price=stopPrice,
                                        parentId=openOrderId)
                ORDER_DICT[stopOrderId] = stopOrder

            print(df_temp)
            previous_row = new_row

    '''
    while True:
      time.sleep(x)
      DF_TEMP = DF_HISTORY.COPY
      if new_data queue [*queue*] is not empty:
        get new_data [*Series]

        if new_data.time is different than old_data.time:
          DF_HISTORY = CalculateTechnical(DF_HISTORY, old_data)
          DF_TEMP = DF_HISTORY.COPY

        DF_TEMP = CalculateTechnical(DF_TEMP, new_data)

      stragety = StrategyMaking(DF_TEMP) [Strategy, Class]

      if strategy is open:
        PlaceOrder
        time.sleep(X)
        Get Order status from Orderstatus queue (SIZE = 1)
        Status == Filled 
        if no order status, get reqExecution
        PlaceLimitLossOrder
        time.sleep(X)
        Get Order status from Orderstatus queue (SIZE = 1)
        Status == Filled 
        if no order status, get reqExecution

      if strategy is close:
        PlaceOrder
        time.sleep(X)
        Get Order status from Orderstatus queue (SIZE = 1)
        Status == Filled 
        if no order status, get reqExecution
        Cancel All Orders
        Record PNL
    '''


if __name__ == "__main__":
    main()