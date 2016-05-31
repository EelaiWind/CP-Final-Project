# -*- coding: utf-8 -*-
from mydynamodb.utils import add_weather_item
import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET


def collect_data(location, region):

	date = location.find('d:time', ns).find('d:obsTime', ns).text
	date = date[:10]
	elements = location.findall('d:weatherElement', ns)
	for element in elements:
		if element.find('d:elementName', ns).text == 'TEMP':
			temperature = element.find('d:elementValue', ns).find('d:value', ns).text
		if element.find('d:elementName', ns).text == 'H_24R':
			rainfall = element.find('d:elementValue', ns).find('d:value', ns).text
		if element.find('d:elementName', ns).text == 'HUMD':
			humidity = float(element.find('d:elementValue', ns).find('d:value', ns).text) * 100
			if humidity == -99: humidity = 50

	add_weather_item(region, date, temperature, rainfall, humidity)


# 自動氣象站-氣象觀測資料
url = 'http://opendata.cwb.gov.tw/opendataapi?dataid=O-A0001-001&authorizationkey=CWB-9A63F68D-76D1-4678-9514-8C5D82B7283B'
ns = {'d': 'urn:cwb:gov:tw:cwbcommon:0.1'}

root = ET.parse(urllib.request.urlopen(url)).getroot()

locations = root.findall('d:location', ns)

for location in locations:
	name = location.find('d:locationName', ns).text
	if name == '桃園': collect_data(location, 'TAOYUAN')
	if name == '礁溪': collect_data(location, 'YILAN')
	if name == '大甲': collect_data(location, 'TAICHUNG')
	if name == '新興': collect_data(location, 'KAOHSIUNG')
	if name == '池上': collect_data(location, 'TAITUNG')

