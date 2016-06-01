 # Python 2/3 compatibility
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta, date
from .chinese_name import *
from .attribute_checker import *
from .attribute_key import *
from .setting import weather_table, product_price_table

__one_day_timedelta = timedelta(1)

def add_weather_item(region, date, temperature, rainfall, humidity, overwrite = True):
    temperature =  Decimal(str(temperature))
    rainfall =  Decimal(str(rainfall))
    humidity =  Decimal(str(humidity))

    check_region(region)
    check_date(date)
    check_rainfall(rainfall)
    check_humidity(humidity)

    item_context = {
        key_date : date,
        key_region : region,
        key_temperature : temperature,
        key_rainfall : rainfall,
        key_humidity : humidity
    }
    if overwrite:
        weather_table.put_item(
            Item=item_context,
        )
    else:
        weather_table.put_item(
            Item=item_context,
            ConditionExpression='attribute_not_exists(#r) and attribute_not_exists(#d)',
            ExpressionAttributeNames={
                '#r':key_region,
                '#d':key_date
            }
        )

def add_product_price_item(product, date, trading_datas, overwrite = True):

    check_date(date)
    check_product(product)

    response = product_price_table.get_item(
        Key={
            key_product: product,
            key_date: date
        }
    )

    if 'Item' in response and key_trading_data in response['Item']:
        trading_datas_context = response['Item'][key_trading_data]
    else:
        trading_datas_context = {}

    for trading_data in trading_datas:
        region = trading_data[key_region]
        price = Decimal(str(trading_data[key_price]))
        turnover = Decimal(str(trading_data[key_turnover]))

        check_region(region)
        check_price(price)
        check_turnover(turnover)

        trading_datas_context[get_region_key(region)] = {
            key_price:price,
            key_turnover:turnover,
        }

    item_context = {
        key_product:product,
        key_date: date,
        key_trading_data: trading_datas_context
    }

    if overwrite:
        product_price_table.put_item(
            Item=item_context,
        )
    else:
        product_price_table.put_item(
            Item=item_context,
            ConditionExpression='attribute_not_exists(#p) and attribute_not_exists(#d)',
            ExpressionAttributeNames={
                '#p':key_product,
                '#d':key_date
            }
        )

def get_poduct_price_record(product, region, delta_starting_days=-90):
    check_product(product)
    check_region(region)

    response = product_price_table.query(
        ProjectionExpression='product, #d, trading_data.{}.price'.format(get_region_key(region)),
        ExpressionAttributeNames={'#d' : key_date },
        KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).gte(get_starting_date(delta_starting_days))
    )

    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = product_price_table.query(
            ProjectionExpression='product, #d, trading_data.{}.price'.format(get_region_key(region)),
            ExpressionAttributeNames={'#d' : key_date },
            KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).gte(get_starting_date(delta_starting_days)),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items += response['Items']
    return parse_product_prict_query_output(items, product, region)

def parse_product_prict_query_output(responseItem, product, region):
    starting_date = ''
    previous_date = None
    if len(responseItem) > 0:
        ending_date = responseItem[-1][key_date]
    else:
        ending_date = ''
    price=[]
    region_key = get_region_key(region)
    for data in responseItem:
        # fix missing date price
        while previous_date and parse_string_to_date(data[key_date]) - previous_date > __one_day_timedelta:
            price.append(0.0)
            previous_date += __one_day_timedelta

        # fix missing region price
        if not key_trading_data in data:
            if starting_date == '':
                continue
            else:
                price.append(0.0)
                previous_date += __one_day_timedelta
        else:
            if starting_date == '':
                starting_date = data[key_date]
                previous_date = parse_string_to_date(starting_date)
            else:
                previous_date += __one_day_timedelta
            price.append(data[key_trading_data][region_key][key_price])

    return {
        'product':product,
        'region':region,
        'starting_date':starting_date,
        'ending_date':ending_date,
        'price':price,
    }

def parse_string_to_date(date_input):
    if type(date_input) is date:
        return date
    else:
        return datetime.strptime(date_input, '%Y-%m-%d').date()

def get_region_key(region):
    if not region in valid_regions:
        raise Exception('{} is not a valid "region" attribute'.format(region))
    if region == TAOYUAN:
        return key_taoyuan
    elif region == YILAN:
        return key_yilan
    elif region == TAICHUNG:
        return key_taichung
    elif region == KAOHSIUNG:
        return key_kaohsiung
    elif region == TAITUNG:
        return key_taitung

def get_starting_date(delta_days):
    return str(datetime.now().date() + timedelta(delta_days))

def retieve_training_data(product, region, starting_date='2007-01-01', ending_date=datetime.now().date(), weather_history_size=31, trading_data_hostory_size=5):
    check_date(starting_date)
    check_date(ending_date)
    ending_date = parse_string_to_date(ending_date)
    starting_date = parse_string_to_date(starting_date)
    now_date = starting_date + (max(weather_history_size, trading_data_hostory_size)-1)*__one_day_timedelta

    ground_truth = retrieve_ground_truth(product, region, now_date, ending_date);

    training_datas = []
    index = 0
    while now_date <= ending_date:
        print(now_date)
        row_data = { 'month': now_date.month, 'date': now_date, 'ground_truth':ground_truth[index] }
        index += 1
        # weather data include today's data
        weather_data = get_batch_weather(region, now_date-(weather_history_size-1)*__one_day_timedelta, now_date)
        # tading data does NOT include today's data, the latest one is yesterdays
        trading_data = get_batch_trading_data(product, region, now_date-(trading_data_hostory_size)*__one_day_timedelta, now_date-__one_day_timedelta)
        row_data = { **row_data, **weather_data, **trading_data }
        training_datas.append(row_data)
        now_date += __one_day_timedelta

    return training_datas

def retrieve_ground_truth(product, region, starting_date, ending_date):
    check_product(product)
    check_region(region)

    starting_date = str(starting_date)
    ending_date = str(ending_date)

    if starting_date > ending_date:
        return []

    regin_shortcut = get_region_key(region)
    response = product_price_table.query(
        ProjectionExpression='#d, trading_data.{}.price'.format(regin_shortcut),
        ExpressionAttributeNames={'#d' : key_date },
        KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).between(starting_date, ending_date)
    )

    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = product_price_table.query(
            ProjectionExpression='#d, trading_data.{}.price'.format(regin_shortcut),
            ExpressionAttributeNames={'#d' : key_date },
            KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).between(starting_date, ending_date),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items += response['Items']

    origin_data = []
    for data in items:
        if key_trading_data in data:
            origin_data.append({
                key_date: data[key_date],
                key_price: data[key_trading_data][regin_shortcut][key_price]
            })

    return fix_missing_data(origin_data, [key_price], starting_date, ending_date)[key_price]

def get_batch_weather(region, starting_date, ending_date):
    starting_date = str(starting_date)
    ending_date = str(ending_date)
    check_region(region)
    check_date(starting_date)
    check_date(ending_date)

    response = weather_table.query(
        ProjectionExpression='#d, {}, {}, {}'.format(key_temperature, key_rainfall, key_humidity),
        ExpressionAttributeNames = {'#d':'date'},
        KeyConditionExpression=Key(key_region).eq(region) & Key(key_date).between(starting_date, ending_date)
    )
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = weather_table.query(
            ProjectionExpression='#d, {}, {}, {}'.format(key_temperature, key_rainfall, key_humidity),
            ExpressionAttributeNames = {'#d':'date'},
            KeyConditionExpression=Key(key_region).eq(region) & Key(key_date).between(starting_date, ending_date),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items += response['Items']

    origin_data = []
    for data in items:
        origin_data.append({
            key_date:data[key_date],
            key_temperature:data[key_temperature],
            key_rainfall:data[key_rainfall],
            key_humidity:data[key_humidity]
        })

    return fix_missing_data(origin_data, [key_temperature, key_rainfall, key_humidity], starting_date, ending_date)

def get_batch_trading_data(product, region, starting_date, ending_date):
    starting_date = str(starting_date)
    ending_date = str(ending_date)
    check_product(product)
    check_region(region)
    check_date(starting_date)
    check_date(ending_date)

    region_shortcut = get_region_key(region)
    response = product_price_table.query(
        ProjectionExpression='#d, trading_data.{}'.format(region_shortcut),
        ExpressionAttributeNames = { '#d' : 'date' },
        KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).between(starting_date, ending_date)
    )
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = product_price_table.query(
            ProjectionExpression='#d, trading_data.{}'.format(region_shortcut),
            ExpressionAttributeNames = { '#d' : 'date' },
            KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).between(starting_date, ending_date),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items += response['Items']

    if len(items) > 0:
        origin_data = []
        for data in items:
            if key_trading_data in data:
                origin_data.append({
                    key_date:data[key_date],
                    key_price:data[key_trading_data][region_shortcut][key_price],
                    key_turnover:data[key_trading_data][region_shortcut][key_turnover]
                })
        if len(origin_data) > 0:
            return fix_missing_data(origin_data, [key_price, key_turnover], starting_date, ending_date)

    historical_month_average = get_month_history_average(product, region, ending_date)
    elapsed_days = (parse_string_to_date(ending_date) - parse_string_to_date(starting_date)).days
    return {
        key_price: [historical_month_average[key_price]]*elapsed_days,
        key_turnover: [historical_month_average[key_turnover]]*elapsed_days,
    }

def get_month_history_average(product, region, date):
    check_product(product)
    check_region(region)
    check_date(date)
    region_shortcut = get_region_key(region)
    #ex: "-04-"
    date_query_string = str(date)[0:8]
    response = product_price_table.query(
        ProjectionExpression='trading_data.{}.price, trading_data.{}.turnover'.format(region_shortcut, region_shortcut),
        KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).begins_with(date_query_string)
    )

    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = product_price_table.query(
            ProjectionExpression='trading_data.{}.price, trading_data.{}.turnover'.format(region_shortcut, region_shortcut),
            KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).begins_with(date_query_string),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items += response['Items']

    total={
        key_price:0,
        key_turnover:0,
    }

    count = 0
    for data in items:
        if key_trading_data in data:
            total[key_price] += data[key_trading_data][region_shortcut][key_price]
            total[key_turnover] += data[key_trading_data][region_shortcut][key_turnover]
            count += 1

    if count == 0:
        raise Exception("trading data of {} at {} is insufficient")

    return {
        key_price: total[key_price]/count,
        key_turnover: total[key_turnover]/count
    }

def fix_missing_data(oringin_datas, key_list, starting_date, ending_date):
    if not key_date in oringin_datas[0]:
        raise Exception('Fix missing data: Oringin_data must contain "date" information!')

    __MISSING_VALUE = -1
    value_sum = {}
    value_count = {}
    value_mean = {}
    value_record = {}

    ending_date = str(ending_date)
    expected_date = parse_string_to_date(starting_date)
    for key in key_list:
        value_sum[key] = 0
        value_count[key] = 0
        value_record[key] = []

    no_data_index = []
    index = 0
    for data in oringin_datas:
        while str(expected_date) < data['date']:
            for key in key_list:
                value_record[key].append(__MISSING_VALUE)

            no_data_index.append(index)
            index += 1
            expected_date += __one_day_timedelta

        for key in key_list:
            value = data[key]
            value_sum[key] += value
            value_count[key] += 1
            value_record[key].append(value)

        expected_date += __one_day_timedelta
        index += 1

    while str(expected_date) <= ending_date:
        no_data_index.append(index)
        for key in key_list:
            value_record[key].append(__MISSING_VALUE)

        index += 1
        expected_date += __one_day_timedelta

    for key in key_list:
        if value_count[key] == 0:
            value_mean[key] = 0
        else:
            value_mean[key] = value_sum[key]/value_count[key]

    for i in no_data_index:
        for key in key_list:
            value_record[key][i] = value_mean[key]

    context = {}
    for key in key_list:
        context[key] = value_record[key]
    return context

