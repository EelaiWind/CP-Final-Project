#coding=utf-8

from mydynamodb.utils import add_weather_item, add_product_price_item, get_poduct_price_record
from datetime import datetime, timedelta
from math import sin, cos, pi
from random import randint

my_date = datetime.strptime('2016-01-01','%Y-%m-%d').date()
count = 0
while my_date <= datetime.now().date():
    trading_datas = [
        {
            'region':'TAICHUNG',
            'price': sin(pi*count/10)+1,
            'turnover': cos(pi*count/10)+1
        },
    ]
    add_product_price_item('PONKAN',str(my_date), trading_datas)
    trading_datas = [
        {
            'region':'KAOHSIUNG',
            'price': randint(1,100),
            'turnover': randint(1,100)
        },
    ]
    add_product_price_item('CABBAGE',str(my_date), trading_datas)
    my_date += timedelta(1)
    count += 1
