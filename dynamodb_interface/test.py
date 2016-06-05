#coding=utf-8

from mydynamodb.utils import *
import csv

datas = retieve_training_data('WATERMELON','KAOHSIUNG', '2007-01-23', '2007-02-22')[0]['price']

print(len(datas))
print(datas)
