#coding=utf-8

from mydynamodb.utils import *
import csv

datas = retieve_training_data('WATERMELON','KAOHSIUNG', '2014-01-16', '2014-02-15')[0]

#print(len(datas))
print(datas)
