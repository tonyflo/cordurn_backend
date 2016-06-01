# -*- coding: utf-8 -*-
"""
Created on Wed Jun  1 17:40:07 2016

@author: tony
"""

from earcal import get_earning_data

def main():
    qe = get_earning_data("20160601")
    print(qe)

if __name__ == "__main__":
    main()

