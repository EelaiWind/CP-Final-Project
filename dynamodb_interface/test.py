#coding=utf-8

from mydynamodb.utils import add_weather_item, add_product_price_item, get_poduct_price_record
from mydynamodb.attribute_checker import valid_products, valid_regions
from mydynamodb.setting import product_price_table

for product in valid_products:
    for region in valid_regions:
        data = get_poduct_price_record(product, region)
        print('{} @ {}'.format(data['product'],data['region']))
        print('{} ~ {}'.format(data['starting_date'],data['ending_date']))
        print(data['price'])            # list of price
