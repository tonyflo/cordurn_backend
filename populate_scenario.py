# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 19:46:02 2016

@author: tony
"""

import MySQLdb

def populate_scenario():
    try:
        conn=MySQLdb.connect(host="mysql.cordurn.com", user="vptfitm", passwd="K0naBrewingC0", db="cordurn")
        conn.autocommit('on')
        print('Connected to MySQL db')

        print('Getting list of stock symbols with price data')
        cursor = conn.cursor()
        cursor.callproc('get_symbols_with_ohlc')  
        stocks = cursor.fetchall()
        cursor.close()
        print(str(len(stocks)) + ' stocks with price data found' )
        for stock in stocks:
            stock_id = stock[0]
            symbol = stock[1]
            print('%s %s' % (symbol, stock_id))
            cursor = conn.cursor()
            cursor.callproc('insert_earnings_scenario', [stock_id]) 
            cursor.close()
            
    except MySQLdb.Error as e:
        print("\tMySQL Error [%d]: %s" % (e.args[0], e.args[1]))
    finally:
        cursor.close()
        conn.close()
        
if __name__ == '__main__':
    populate_scenario()