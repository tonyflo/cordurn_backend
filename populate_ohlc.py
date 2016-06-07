# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 18:45:39 2016

@author: tony
"""

from yahoo_finance import Share
import MySQLdb

def pack_historical(historical):
    d, o, h, l, c, a, v = [[], [], [], [], [], [], []]
    for data in historical:
        d.append(data['Date'])
        o.append(data['Open'])
        h.append(data['High'])
        l.append(data['Low'])
        c.append(data['Close'])
        a.append(data['Adj_Close'])
        v.append(data['Volume'])
    return zip(d, o, h, l, c, a, v)

def insert_ohlc(stock_id, z, historical_len, cursor, symbol):
    into_command = 'INSERT INTO ohlc(stock_id, date, open, high, low, close, adj_close, volume)'
    values_command = ' VALUES(' + str(stock_id) + ', %s, %s, %s, %s, %s, %s, %s)'
    sql_command = into_command + values_command
    print('\tInserting historical data')
    num_inserted = 0
    try:
        num_inserted = cursor.executemany(sql_command, list(z))
    except Exception as e:
        if e.args[0] == 1062:
            print("\tMySQL warning [%d]: %s" % (e.args[0], e.args[1]))
            return
        else:
           raise e
    if historical_len != num_inserted:
        print('\t--- Tried to insert %d rows but only %d were inserted for %s' % (historical_len, num_inserted, symbol))

def populate_ohlc():
    START = '2001-01-01'
    END = '2016-06-06'
    try:
        conn=MySQLdb.connect(host="mysql.cordurn.com", user="vptfitm", passwd="K0naBrewingC0", db="cordurn")
        print('Connected to MySQL db')
        cursor = conn.cursor()
        conn.autocommit('on')

        print('Getting list of stock symbols')
        cursor.execute('select stock_id, symbol from stock') 
        stocks = cursor.fetchall()
        for stock in stocks:
            stock_id = stock[0]
            symbol = stock[1]
            company = Share(symbol)
            print(symbol)
            print('\tGetting historical data')
            historical = company.get_historical(START, END)
            if not historical:
                str_msg='--- No price data found for %s' % symbol
                print('\t' + str_msg)
                with open("/home/tony/Development/cordurn/no_price_data.txt", "a") as myfile:
                    myfile.write(str_msg + '\n')
                continue

            historical_len = len(historical)
            z = pack_historical(historical)
            insert_ohlc(stock_id, z, historical_len, cursor, symbol)
            
    except MySQLdb.Error as e:
        print("\tMySQL Error [%d]: %s" % (e.args[0], e.args[1]))
    finally:
        cursor.close()
        conn.close()
        
if __name__ == '__main__':
    populate_ohlc()