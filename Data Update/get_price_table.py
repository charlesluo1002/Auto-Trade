from time import strftime, time, sleep
from ib.ext.Contract import Contract
from ib.opt import ibConnection, message
import pandas as pd
import numpy as np

def get_price_table(symbol = "NQ", secType = "FUT", currency = "USD", exchange = "GLOBEX", expiry = '201906', includeExpired = True, duration = '1 D', bar_size = '5 mins', whatToShow = 'TRADES', useRTH = 0, Endtime = '20170101 00:00:00', port=4001, clientId = 5):
    hist = []
    con = ibConnection(port=port,clientId=clientId)
    def inner():
        qqqq = Contract()
        qqqq.m_secType = secType 
        qqqq.m_symbol = symbol
        qqqq.m_currency = currency
        qqqq.m_exchange = exchange
        qqqq.m_expiry = expiry
        qqqq.m_includeExpired = includeExpired
        #endtime = strftime('%Y%m%d %H:%M:%S')
        if Endtime == None:
            endtime = strftime('%Y%m%d %H:%M:%S')
        else:
            endtime = Endtime
        # 8 arguments: 'tickerId', 'contract', 'endDateTime', 'durationStr', 'barSizeSetting', 'whatToShow', 'useRTH', and 'formatDate'
        con.reqHistoricalData(1,qqqq,endtime,duration,bar_size,whatToShow,useRTH,1)
    
    def nextValidId_handler(msg):
        print(msg)
        inner()

    def my_hist_data_handler(msg):
        global lasttime
        if "finished" in msg.date:
            print('Disconnected:', con.disconnect())
            df = pd.DataFrame(index=np.arange(0, len(hist)), columns=('date', 'open','high','low','close', 'volume'))
            for index, msg in enumerate(hist):
                df.loc[index,'date':'volume'] = msg.date, float(msg.open),float(msg.high),float(msg.low),float(msg.close), float(msg.volume)
            global price_table
            price_table = df
        else:
            hist.append(msg)
            if msg.date[4:8] != lasttime:
                print('now at time:', msg.date)
                lasttime = msg.date[4:8]

    def error_handler(msg):
        if msg.typeName == "error" and msg.id != -1:
            print ("Error:", msg)
    
    con.register(error_handler, message.Error)
    con.register(nextValidId_handler, message.nextValidId)
    con.register(my_hist_data_handler, message.historicalData)
    con.connect()



# Data Fetching
if __name__ == '__main__':
    price_table = 1
    get_price_table(symbol = "ZB",
                    secType = "CONTFUT",
                    currency = "USD",
                    exchange = "ECBOT",
                    expiry = '',
                    includeExpired = True,
                    duration = '50 D',
                    bar_size = '1 hour',
                    whatToShow = 'TRADES',
                    useRTH = 0,
                    Endtime = '20191231 23:59:59',
                    port = 4001,
                    clientId = 11)

price_table.to_csv('')    
    
    
    
    