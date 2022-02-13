from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.opt import Connection, message
from datetime import datetime as dt
from datetime import time as t
from datetime import timedelta
import numpy as np
import pandas as pd
from time import sleep
from math import floor, ceil
import ta
import smtplib
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText

class APP:
    def __init__(self, symbol, secType, exchange, currency, expiry, quantity, strategies, timeframe, port=7497, clientId=1, nd= 45):
        self.n_days = n_days
        self.next_order_id = -1
        self.active = False
        self.coworkers = []
        self.parent_id, self.pt_id, self.sl_id = 0, 0, 0
        self.parent_order, self.pt_order, self.sl_order = None, None, None
        self.symbol = symbol
        self.secType = secType
        self.exchange = exchange
        self.currency = currency
        self.expiry = expiry
        self.last_position_expiry = expiry
        self.quantity = quantity
        self.port = port
        self.clientId = clientId
        self.account = ''
        self.account_value = 0
        self.position = 0
        self.unrealized_pnl = 0
        self.realized_pnl = 0
        self.reqBarsId = 1
        self.con = None
        self.contract = None
        self.new_period = False
        self.strategies = strategies
        self.tf = timeframe
        self.nq_atr_pr = 115
        self.sma_pr = 18
        self.bar_size = '5 mins' if self.tf == 5 else ('15 mins' if self.tf == 15 else'1 hour')
        if self.tf == 30:
            self.bar_size = '30 mins'
        self.whatToShow = 'MIDPOINT' if self.secType in ['CASH','CMDTY'] else 'TRADES'
        self.time = dt.now() + timedelta(hours = 3)
        self.high_price, self.low_price, self.open_price, self.volume = 0, 0, 0, 0        
        self.table = pd.DataFrame(columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'change', 'gry'])
        self.breakout, self.breakdown, self.breakout15, self.breakdown15 = False, False, False, False
        self.breaking = False
        # weekday of 4 = Friday
        self.weekday, self.day = dt.now().weekday(), dt.now().day
        # yesterday high low close
        self.yesterday_high, self.yesterday_low, self.yesterday_close = 0, 0, 0
        self.vix_open = self.vix_close = 0
        self.vix_change = 999
        self.nq_mid_position = 0
        
    def refresh_attributes(self):
        self.time = dt.now() + timedelta(hours = 3)
        self.active = True
        self.high_price, self.low_price, self.open_price, self.volume = 0, 0, 0, 0        
        self.table = pd.DataFrame(columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'change', 'gry'])
        self.weekday, self.day = dt.now().weekday(), dt.now().day
        self.yesterday_high, self.yesterday_low, self.yesterday_close = 0, 0, 0
    
    
    # Define all the callback functions here
    def server_handler(self, msg):
        if msg.typeName == "nextValidId":
            self.next_order_id = msg.orderId
        #elif msg.typeName == "managedAccounts":
        #    self.account_code = msg.accountsList
        #elif msg.typeName == "updatePortfolio" \
        #        and msg.contract.m_symbol == self.symbol:
        #    self.unrealized_pnl = msg.unrealizedPNL
        #    self.realized_pnl = msg.realizedPNL
        #    self.position = msg.position
        elif msg.typeName == "updatePortfolio"  and msg.contract.m_symbol == self.symbol:
            self.unrealized_pnl = msg.unrealizedPNL
            self.realized_pnl = msg.realizedPNL
            self.position = msg.position
            if self.position != 0:
                self.last_position_expiry = msg.contract.m_expiry
        elif msg.typeName == 'updateAccountValue' and msg.key == 'NetLiquidation':
            self.account_value = float(msg.value)
        elif msg.typeName == 'contractDetails':
            self.expiry = msg.contractDetails.m_contractMonth
            self.contract = self.create_contract(self.symbol, self.secType if self.secType != 'CONTFUT' else 'FUT', self.exchange, self.currency, self.expiry)
            #self.contract = msg.contract
            #import inspect
            #print([name for name,thing in inspect.getmembers(msg)])
        
        
    def realtime_handler(self, msg):
        # constantly update high, low and volume
        if msg.high > self.high_price: self.high_price = msg.high
        if msg.low < self.low_price: self.low_price = msg.low
        self.volume = msg.volume + self.volume
        
        if self.new_period == True:
            self.open_price, self.high_price, self.low_price, self.volume = msg.open, msg.high, msg.low, msg.volume
            self.new_period = False
            
        #print ("{},bid:{}, ask:{}, last:{} ".format(dt.now(), self.bid, self.ask, self.last))
        
        # update time, weekday and date
        self.time = dt.fromtimestamp(int(msg.time) + 5)
        self.weekday = self.time.weekday()
        if self.day != self.time.day:
            self.day = self.time.day
            self.breakout = False
            self.breakdown = False
            self.breaking = False
        
        # close all open positions by limit orders for NQ at 15:59:55
        if self.position != 0 and self.time.hour == 15 and self.time.minute == 59 and self.time.second == 55:
            if self.symbol in ['NQ'] and self.tf == 5:
                print(self.symbol ,'End of day close positions activated, number of open positions closed:', self.position, '\n  limit_price: ', msg.close, '\n')
                temp_contract = self.create_contract(self.symbol, self.secType, self.exchange, self.currency, self.last_position_expiry)
                good_till = (self.time+timedelta(seconds = 9)).strftime('%Y%m%d %H:%M:%S')
                self.place_order(temp_contract, 'BUY' if self.position < 0 else 'SELL', abs(self.position), msg.close, good_till_date = good_till)
        
        # close rest of the positions for NQ if not filled by limit order at 16:00:5
        if self.position != 0 and self.time.hour == 16 and self.time.minute == 00 and self.time.second == 5:
            if self.symbol in ['NQ'] and self.tf == 5:
                print(self.symbol ,'Insurance for end of day close positions activated, number of open positions closed:', self.position, '\n  market_price: ', msg.close, '\n')
                temp_contract = self.create_contract(self.symbol, self.secType, self.exchange, self.currency, self.last_position_expiry)
                self.place_order(temp_contract, 'BUY' if self.position < 0 else 'SELL', abs(self.position))
        
        
        
        # Update price tables when time crosses whole period marks and perform strategies.
        if self.time.minute%self.tf == 0 and self.time.second == 0:
            self.new_period = True
            if self.low_price != 0:
                self.update_tables(self.time - timedelta(minutes = self.tf), self.open_price, self.high_price, self.low_price, msg.close, self.volume)

                    
    def all_start(self, app_list):
        for app in app_list:
            temp = app_list.copy()
            temp.remove(app)
            app.start(temp)
            
    def historical_data_handler(self, msg):
        if 'finished' not in msg.date:
            if msg.reqId == 10:
                if self.time.time() < t(17,0,10):
                    if msg.date < self.time.strftime('%Y%m%d'):
                        self.yesterday_high = float(msg.high)
                        self.yesterday_low = float(msg.low)
                        self.yesterday_close = float(msg.close)
                if self.time.time() > t(17,59,50):
                    if msg.date <= self.time.strftime('%Y%m%d'):
                        self.yesterday_high = float(msg.high)
                        self.yesterday_low = float(msg.low)
                        self.yesterday_close = float(msg.close)
            if msg.reqId == 9:
                if msg.date not in self.table.time.values and (self.time - dt.strptime(msg.date, '%Y%m%d %H:%M:%S')) > timedelta(minutes = self.tf):
                    self.update_tables(msg.date, float(msg.open), float(msg.high), float(msg.low), float(msg.close), float(msg.volume))
                if (self.time - dt.strptime(msg.date, '%Y%m%d %H:%M:%S')) < timedelta(minutes = self.tf):
                    self.open_price = float(msg.open)
                    self.high_price = float(msg.high)
                    self.low_price = float(msg.low)
                    self.volume = float(msg.volume)
            if msg.reqId == 11:
                if msg.date == self.time.strftime('%Y%m%d'):
                    self.vix_open = float(msg.open)
                if msg.date != self.time.strftime('%Y%m%d'):
                    self.vix_close = float(msg.close)
                if self.vix_close !=0 and self.vix_open != 0:
                    self.vix_change = (self.vix_open - self.vix_close)/self.vix_close
    
    # create contract function, returns a contract object
    def create_contract(self, symbol, sec_type, exch, curr, expiry, primex = None):
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_currency = curr
        contract.m_expiry = expiry
        if primex != None:
            contract.m_primaryExch = primex
        if symbol == 'SI':
            contract.m_multiplier = '5000'
        return contract

    # create one order, allows for mkt, lmt, stp, profit taker and stop loss.
    def create_order(self, action, qty, limit_price=None, profit_taker=None, stop_loss=None, trail_stop = None, transmit=True, parentId=None, good_till_date=''):
        
        order = Order()
        # is child order?
        if parentId is not None: order.m_parentId = parentId

        order.m_action = action
        order.m_totalQuantity = qty
        order.m_outsideRth = True
        if good_till_date == '':
            order.m_tif = 'GTC'
        else:
            order.m_tif = 'GTD'
            order.m_goodTillDate = good_till_date

        # make it a profit taker with limit order        
        if profit_taker is not None:
            order.m_orderType = 'LMT'
            order.m_lmtPrice = profit_taker

        # make it a stop loss with stop order
        elif stop_loss is not None:
            order.m_orderType = 'STP'
            order.m_auxPrice = stop_loss
        
        # make it a trail stop with trail amount = trail_stop
        elif trail_stop is not None:
            order.m_orderType = 'TRAIL'
            order.m_auxPrice = trail_stop
            
        # make it a limit order if limit price is given
        elif limit_price is not None:
            order.m_orderType = 'LMT'
            order.m_lmtPrice = limit_price

        # make it a market order
        else:
            order.m_orderType = 'MKT'

        # Only transmit when all other child orders are attached.
        order.m_transmit = transmit

        return(order)
    
    # place a set of bracket orders, include a parent order, a profit taker and a stop loss
    def place_order(self, contract, action, qty, entry_price=None, profit_taker=None, stop_loss=None, trail_stop=None, good_till_date = '', single_stop_trail = False, orderId = None, pt_sl_good_till_date = '', sl_entry = False, futures = True):
        if orderId == None: 
            orderId = self.next_order_id
            
        # place one by one if futures
        if futures == True:
            for i in range(qty):
                if single_stop_trail == False:
                    # place bracket order
                    counter_action = 'BUY' if action == 'SELL' else 'SELL'
                    # create parent order, if no pt, sl, ts, then transimit
                    if sl_entry == True:
                        order = self.create_order(action, 1, None, None, entry_price, transmit=True if profit_taker is None and stop_loss is None and trail_stop is None else False, good_till_date = good_till_date)
                    else:
                        order = self.create_order(action, 1, entry_price, transmit=True if profit_taker is None and stop_loss is None and trail_stop is None else False, good_till_date = good_till_date)
                    self.con.placeOrder(orderId, self.contract, order)       
                    #print("\n Parent order entry price = '{}'".format(main_order_entry))
                    self.parent_id = orderId
                    self.parent_order = order
                    
                    if profit_taker is not None:
                        # create a profit taker, if no sl, ts, then transmit
                        order = self.create_order(counter_action, 1, None, profit_taker, None, None,True if (stop_loss is None and trail_stop is None) else False, orderId, pt_sl_good_till_date)
                        self.con.placeOrder(orderId+1, self.contract, order)  
                        #print("   Profit taker price = '{}'".format(order.m_lmtPrice))
                        self.pt_id = orderId + 1
                        self.pt_order = order
                    # create a stop loss or trail stop, set transmit = True
                    if stop_loss is not None:
                        order = self.create_order(counter_action, 1, None, None, stop_loss, None,True, orderId, pt_sl_good_till_date)
                        self.con.placeOrder(orderId+2, self.contract, order)
                        #print("   Stop loss price = '{}'".format(order.m_auxPrice))
                        self.sl_id = orderId + 2
                        self.sl_order = order
                    elif trail_stop is not None:
                        order = self.create_order(counter_action, 1, None, None, None, trail_stop, True, orderId, '')
                        self.con.placeOrder(orderId+2, self.contract, order)
                        #print("   Trail stop = '{}'".format(order.m_auxPrice))
                        self.sl_id = orderId + 2
                        self.sl_order = order
                
                #place single stop or trail stop order
                else:
                    if stop_loss is not None:
                        order = self.create_order(action, 1, None, None, stop_loss, None,True, None, good_till_date)
                        self.con.placeOrder(orderId, self.contract, order)
                        #print("Single stop order, action = {}, price = '{}'".format(action,order.m_auxPrice))
                    elif trail_stop is not None:
                        order = self.create_order(action, 1, None, None, None, trail_stop,True, None, good_till_date)
                        self.con.placeOrder(orderId, self.contract, order)
                        #print("Single trailing stop order, action = {}, stop gap = '{}'".format(action,order.m_auxPrice))
                orderId += 3
                self.next_order_id = orderId
                
                
        # non futures (e.g. stocks)
        else:
            if single_stop_trail == False:
                # place bracket order
                counter_action = 'BUY' if action == 'SELL' else 'SELL'
    #            if entry_price is None:
    #                main_order_entry = 'market'
    #            else:
    #                main_order_entry = entry_price
                # create parent order, if no pt, sl, ts, then transimit
                if sl_entry == True:
                    order = self.create_order(action, qty, None, None, entry_price, transmit=True if profit_taker is None and stop_loss is None and trail_stop is None else False, good_till_date = good_till_date)
                else:
                    order = self.create_order(action, qty, entry_price, transmit=True if profit_taker is None and stop_loss is None and trail_stop is None else False, good_till_date = good_till_date)
                self.con.placeOrder(orderId, self.contract, order)       
                #print("\n Parent order entry price = '{}'".format(main_order_entry))
                self.parent_id = orderId
                self.parent_order = order
                
                if profit_taker is not None:
                    # create a profit taker, if no sl, ts, then transmit
                    order = self.create_order(counter_action, qty, None, profit_taker, None, None,True if (stop_loss is None and trail_stop is None) else False, orderId, pt_sl_good_till_date)
                    self.con.placeOrder(orderId+1, self.contract, order)  
                    #print("   Profit taker price = '{}'".format(order.m_lmtPrice))
                    self.pt_id = orderId + 1
                    self.pt_order = order
                # create a stop loss or trail stop, set transmit = True
                if stop_loss is not None:
                    order = self.create_order(counter_action, qty, None, None, stop_loss, None,True, orderId, pt_sl_good_till_date)
                    self.con.placeOrder(orderId+2, self.contract, order)
                    #print("   Stop loss price = '{}'".format(order.m_auxPrice))
                    self.sl_id = orderId + 2
                    self.sl_order = order
                elif trail_stop is not None:
                    order = self.create_order(counter_action, qty, None, None, None, trail_stop, True, orderId, '')
                    self.con.placeOrder(orderId+2, self.contract, order)
                    #print("   Trail stop = '{}'".format(order.m_auxPrice))
                    self.sl_id = orderId + 2
                    self.sl_order = order
            
            #place single stop or trail stop order
            else:
                if stop_loss is not None:
                    order = self.create_order(action, qty, None, None, stop_loss, None,True, None, good_till_date)
                    self.con.placeOrder(orderId, self.contract, order)
                    #print("Single stop order, action = {}, price = '{}'".format(action,order.m_auxPrice))
                elif trail_stop is not None:
                    order = self.create_order(action, qty, None, None, None, trail_stop,True, None, good_till_date)
                    self.con.placeOrder(orderId, self.contract, order)
                    #print("Single trailing stop order, action = {}, stop gap = '{}'".format(action,order.m_auxPrice))
            self.next_order_id = orderId + 3
        self.con.reqIds(1)
        
    
    def modify_order(self, order_type = 'sl', price = None, orderType = None, qty = None, action = None):
        if order_type == 'parent':
            order_id = self.parent_id
            order = self.parent_order
        elif order_type == 'pt':
            order_id = self.pt_id
            order = self.pt_order
        else:
            order_id = self.sl_id
            order = self.sl_order

        # LMT, STP, TRAIL, MKT
        if orderType is not None:
            order.m_orderType = orderType
        if qty is not None:
            order.m_totalQuantity = qty
        if action is not None:
            order.m_action = action
        if price is not None and order.m_orderType == 'LMT':
            order.m_lmtPrice = price
        if price is not None and order.m_orderType != 'LMT':
            order.m_auxPrice = price
        order.m_transmit = True
        if order_type == 'parent':
            self.parent_id = order_id
            self.parent_order = order
        elif order_type == 'pt':
            self.pt_id = order_id
            self.pt_order = order
        else:
            self.sl_id = order_id
            self.st_order = order
        self.con.placeOrder(order_id, self.contract, order)
        self.con.reqIds(1)
        
    def close_all(self):
        self.con.reqGlobalCancel()
        print('All open orders closed')
        symbols = set([self.symbol])
        if self.position != 0:
            print(self.position, 'contracts for', self.symbol, 'closed.' )
            temp_contract = self.create_contract(self.symbol, self.secType, self.exchange, self.currency, self.last_position_expiry)
            self.place_order(temp_contract, 'BUY' if self.position < 0 else 'SELL', abs(self.position))
            self.nq_mid_position = 0
        for coworker in self.coworkers:
            if coworker.position != 0 and coworker.symbol not in symbols:
                print(coworker.position, 'contracts for', coworker.symbol, 'closed.' )
                temp_contract = self.create_contract(self.symbol, self.secType, self.exchange, self.currency, self.last_position_expiry)
                coworker.place_order(temp_contract, 'BUY' if coworker.position < 0 else 'SELL', abs(coworker.position))
            symbols.add(coworker.symbol)
            coworker.nq_mid_position = 0
    def update_tables(self, time, open, high, low, close, volume):
        T = time if type(time) == str else time.strftime('%Y%m%d  %H:%M:%S')
        # update time, open, high, low, close, change, gry
        self.table.loc[-1] = [T,open,high,low,close,volume,close - open,self.gry(close - open)]
        self.table = self.table.sort_values('time',ascending = False)
        self.table = self.table.reset_index(drop=True)
            


            
    def fill_historical_data(self, Id = 9, duration = '3 D', barsize = None, enddate = None, contract = None, rth = 0):
        self.con.reqHistoricalData(Id, contract if contract != None else self.contract,(self.time + timedelta(seconds = 21000)).strftime('%Y%m%d %H:%M:%S') if enddate == None else enddate,duration, barsize if barsize != None else self.bar_size,self.whatToShow,rth,1)
    
    # register all callback functions 
    def register_callback_functions(self):
        # Assign server messages handling function.
        self.con.registerAll(self.server_handler)
        # Register market data events.
        self.con.register(self.realtime_handler, message.realtimeBar)
        # Register historical data events.
        self.con.register(self.historical_data_handler, message.historicalData)

    # start and stop functions of the app
    def start(self, coworkers = []):
        self.refresh_attributes()
        if self.coworkers == []:
            self.coworkers = coworkers
        self.con = Connection.create(port=self.port, clientId=self.clientId)
        self.con.connect()
        self.con.reqIds(1)
        self.register_callback_functions()
        self.contract = self.create_contract(self.symbol, self.secType, self.exchange, self.currency, None if self.secType == 'CONTFUT' else self.expiry)
        self.con.reqContractDetails(3, self.contract)
        dur = self.n_days
        if self.tf == 15: dur = self.n_days
        if self.tf == 5: dur = self.n_days
        self.fill_historical_data(Id = 9, duration = dur)
        self.con.reqAccountUpdates(True, self.account)

        
    def stop(self):
        self.con.cancelRealTimeBars(self.reqBarsId)
        self.reqBarsId = self.reqBarsId + 1
        self.con.reqAccountUpdates(False, self.account)
        self.active = False
        print('Disconnect:', self.con.disconnect())
    
    def gry(self, x, thresh=0):
        if x>thresh:
            return 'g'
        elif x<-thresh:
            return 'r'
        else:
            return 'y'

    def find_consec(self, lst, color):
        consec = 0
        lst = list(lst)
        while consec < len(lst) and lst[consec] == color:
            consec += 1
        return(consec)
    
    def rd(self, x, direc, p=4):
        if direc == 'u':
            return(ceil(x*p)/p)
        elif direc == 'd':
            return(floor(x*p)/p)
    
    def RSI(self, series, period):
        delta = series.diff().dropna()
        u = delta * 0
        d = u.copy()
        u[delta > 0] = delta[delta > 0]
        d[delta < 0] = -delta[delta < 0]
        u[u.index[period-1]] = np.mean( u[:period] ) #first value is sum of avg gains
        u = u.drop(u.index[:(period-1)])
        d[d.index[period-1]] = np.mean( d[:period] ) #first value is sum of avg losses
        d = d.drop(d.index[:(period-1)])
        rs = u.ewm(com=period-1, adjust=False).mean()/d.ewm(com=period-1, adjust=False).mean()
        return 100 - 100 / (1 + rs)
    
            
if __name__ == "__main__":
    
    n_days = '100 D'
    app1 = APP('GC', 'CONTFUT', 'NYMEX', 'USD', None, 1, [], 60, port=4001, clientId=1, nd = n_days)
    app2 = APP('TSLA', 'STK', 'SMART', 'USD', None, 1, [], 5, port=4001, clientId=2, nd = n_days)
    app3 = APP('CAD', 'CONTFUT', 'GLOBEX', 'USD', None, 1, [], 60, port=4001, clientId=3, nd = n_days)

    applist = [app1, app2, app3]
    app1.all_start(applist)
    

start_time = '20220101 00:00:00'
end_time = '20220131 00:00:00'
for app in applist:
    app.table = app.table.sort_values('time',ascending = True)
    app.table = app.table[(app.table.time > start_time) & (app.table.time < end_time)]
    app.table = app.table.reset_index(drop=True)
    app.table = app.table.loc[:,'time':'volume']
    app.table.to_csv('data.csv', index = False, header = False)