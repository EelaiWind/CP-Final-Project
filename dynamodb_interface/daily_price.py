# -*- coding: utf-8 -*-

from mydynamodb.utils import add_product_price_item
import json
import urllib.request, urllib.parse, urllib.error

product_code = ['LP2', 'SG5', 'LB12', 'FY7', 'S01', 'SH5', 'T1', 'R3', 'FB11', 'LD1', 'SE1', 'FT1', 'SD1',
                'A1', '45', 'SC1', 'LA1', 'FK4', 'C1', 'FJ3', 'LH1', 'S1', 'FV1', 'B2', 'X69', 'SA32']

product_code_mapping = {'LP2':'BASIL', 'SG5':'GARLIC', 'LB12':'BOK_CHOY', 'FY7':'CORN', 'S01':'SWEET_POTATO',
                    'SH5':'BAMBOO_SHOOT', 'T1':'WATERMELON', 'R3':'MANGO', 'FB11':'BROCCOLI', 'LD1':'SPOON_CABBAGE',
                    'SE1':'SHALLOT', 'FT1':'PUMPKIN', 'SD1':'ONION', 'A1':'BANANA', '45':'STRAWBERRY', 'SC1':'POTATO',
                    'LA1':'CABBAGE', 'FK4':'SWEET_PEPPER', 'C1':'PONKAN', 'FJ3':'TOMATO', 'LH1':'SPINACH', 'S1':'GRAPE',
                    'FV1':'CHILI', 'B2':'PINEAPPLE', 'X69':'APPLE', 'SA32':'RADISH'}



def market_filter(market):

    flag = False
    if market == '桃園縣':
        flag = True
        market = 'TAOYUAN'
    elif market == '宜蘭市':
        flag = True
        market = 'YILAN'
    elif market == '台中市':
        flag = True
        market = 'TAICHUNG'
    elif market == '高雄市':
        flag = True
        market = 'KAOHSIUNG'
    elif market == '台東市':
        flag = True
        market = 'TAITUNG'

    return flag, market

#農產品交易行情(每日更新)
url = "http://m.coa.gov.tw/OpenData/FarmTransData.aspx"

#type : string
data = urllib.request.urlopen(url).read()
#print "Retrieved", len(data), "characters"

info = json.loads(data)
for item in info:
    code = item['作物代號']
    product = item['作物名稱']
    date = item['交易日期']
    region = item['市場名稱']
    price = item['平均價']
    turnover = item['交易量']

    for c in product_code:
        if c == code:
            required_market, region = market_filter(region)
            if required_market:
                # change date format from yy.mm.dd to yy-mm-dd
                date = date.encode('utf-8').replace('.', '-')
		ad_year = int(date[:3]) + 1911
		ad_date = str(ad_year) + date[3:]
		trading_datas = [
		    {
			'region': region,
			'price': price,
			'turnover': turnover
		    }
		]
		add_product_price_item(product_code_mapping[code], ad_date, trading_datas)
		break;

