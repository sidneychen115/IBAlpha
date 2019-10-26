from ibapi.wrapper import EWrapper
from ibapi.client import EClient
import queue
import time
from threading import Thread
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.order_state import OrderState


class IBOrder(EWrapper, EClient):

    def __init__(self, ipaddress, portid, clientid, account_summary_queue, position_queue):
        EClient.__init__(self, self)
        self.order_queue = queue.Queue()
        self.connect(ipaddress, portid, clientid)
        self.nextOrderID = -50
        self.orderIDOderDict = {}
        #self.logger = logger
        self.as_queue = account_summary_queue
        self.position_queue = position_queue
        thread = Thread(target=self.run, name='IBOrder', daemon=True)
        thread.start()
        setattr(self, "_thread", thread)

    def get_next_valid_id(self):
        oid = self.nextOrderID
        self.nextOrderID += 1
        return oid

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextOrderID = orderId
        #self.logger.info("setting nextValidOrderId: %d", orderId)

    # def error(self, id, errorCode, errorString):
    # Overriden method
    # message = "Error ID:" + str(id) + " Error Code: " + str(errorCode) + " Error: " + errorString
    # self.logger.error(message)
    # print(message)

    def openOrder(self, orderId, contract: Contract, order: Order, orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        order.contract = contract
        statusOrder = StatusOrder(order, orderState)
        self.orderIDOderDict[statusOrder.orderId] = statusOrder

    def orderStatus(self, orderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        # Best effort updating the filled price and status
        if orderId in self.orderIDOderDict:
            self.orderIDOderDict[orderId].status = status
            self.orderIDOderDict[orderId].filledPrice = avgFillPrice

        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
              lastFillPrice, "ClientId:", clientId, "WhyHeld:",
              whyHeld, "MktCapPrice:", mktCapPrice)

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        super().position(account, contract, position, avgCost)
        _position = Position(position, avgCost)
        self.position_queue.put(_position)


    def get_order_status(self, orderId):
        if orderId in self.orderIDOderDict:
            return self.orderIDOderDict[orderId].status
        else:
            return "None"

    def updatePortfolio(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        super().updatePortfolio(contract, position, marketPrice, marketValue,
                                averageCost, unrealizedPNL, realizedPNL, accountName)
        _position = Position(position, averageCost)
        self.as_queue.put(_position)



class StatusOrder():
    def __init__(self, order, orderState):
        self.orderId = order.orderId
        self.permId = order.permId
        self.symbol = order.contract.symbol
        self.action = order.action
        self.orderType = order.orderType
        self.totalQuantity = order.totalQuantity
        self.limPrice = order.lmtPrice
        self.status = orderState.status
        self.filledPrice = 0

class Position():
    def __init__(self, position, avgCost):
        self.position = position
        self.avgCost = avgCost

    def __str__(self):
        return 'Position: %f Avg Cost: %f' % (self.position, self.avgCost)
