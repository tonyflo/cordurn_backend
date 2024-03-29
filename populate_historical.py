# -*- coding: utf-8 -*-
"""

"""

from earcal import get_earning_data
from datetime import date, timedelta
import MySQLdb
        
def print_db_error(e):
    try:
        print("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
    except IndexError:
        print("MySQL Error: %s" % str(e))

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

def insert_earnings(qe, d_str):
    for stock in qe:
        symbol = stock['symbol']
        name = stock['name']
        when = stock['time']
        if not is_ascii(name):
            print('NOASCII - name:%s, symbol:%s, time:%s' % (name, symbol, when))
            continue
        if not symbol or not name or not when or '.' in symbol:
            print('SKIPING - name:%s, symbol:%s, time:%s' % (name, symbol, when))
            continue
        if not is_ascii(when):
            when = 'Time Not Supplied'
        if '\n' in name:
            name = name.replace('\n', ' ')

        args = [d_str, when, symbol, name]
        try:
            conn=MySQLdb.connect(host="mysql.cordurn.com", user="vptfitm", passwd="K0naBrewingC0", db="cordurn")
            conn.autocommit('on')
            cursor = conn.cursor()
            cursor.callproc('insert_earnings_release', args) 
        except MySQLdb.Error as e:
            print_db_error(e)
        finally:
            try:
                cursor.close()
                conn.close()
            except MySQLdb.ProgrammingError as pe:
                print_db_error(pe)
 
def populate_historical():
    d = date.today()
    oldest = date(2001, 1, 1)
    
    while d >= oldest:
        d_str = d.strftime("%Y%m%d")
        d = d - timedelta(days=1)
        qe = get_earning_data(d_str)   
        if not qe: 
            print('NO_DATA - %s' % d_str)
            continue
        print('PULLING - %s' % d_str)
        insert_earnings(qe, d_str)
        
if __name__ == '__main__':
    populate_historical()
