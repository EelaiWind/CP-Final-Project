 # Python 2/3 compatibility
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta, date
from .chinese_name import *
from .attribute_checker import *
from .attribute_key import *
from .setting import weather_table, product_price_table

__one_day_timedelta = timedelta(1)
__MISSING_VALUE = None
__month_time_delta = timedelta(30)

def add_weather_item(region, date, temperature, rainfall, humidity, overwrite = True):

    check_region(region)
    check_date(date)

    item_context = {
        key_date : date,
        key_region : region,
    }
    if temperature:
        temperature =  Decimal(str(temperature))
        check_temperature(temperature)
        item_context[key_temperature] = temperature
    if rainfall:
        rainfall =  Decimal(str(rainfall))
        check_rainfall(rainfall)
        item_context[key_rainfall] = rainfall
    if humidity:
        humidity =  Decimal(str(humidity))
        check_humidity(humidity)
        item_context[key_humidity] = humidity

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

def add_product_price_item(product, date, region, price, turnover):

    check_date(date)
    check_product(product)
    check_region(region)
    price = Decimal(str(price))
    turnover = Decimal(str(turnover))
    check_price(price)
    check_turnover(turnover)

    product_price_table.update_item(
        #Item=item_context,
        Key={
            key_product: product,
            key_date: date,
        },
        UpdateExpression= 'set {} = :tr'.format(region),
        ExpressionAttributeValues = {
            ':tr' : {
                key_price:price,
                key_turnover:turnover
            }
        }
    )


def get_poduct_price_record(product, region, delta_starting_days=-90):
    check_product(product)
    check_region(region)

    ending_date = datetime.now().date();
    starting_date = ending_date + timedelta(delta_starting_days);

    return {
        'product':product,
        'region':region,
        'starting_date':str(starting_date),
        'ending_date':str(ending_date),
        'price':get_poduct_price_record(product, region, starting_date, ending_date)[key_price],
    }

def parse_string_to_date(date_input):
    if type(date_input) is date:
        return date
    else:
        return datetime.strptime(date_input, '%Y-%m-%d').date()

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
    return get_batch_trading_data(product, region, starting_date, ending_date)[key_price]

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
    while key_last_evaluated in response:
        response = weather_table.query(
            ProjectionExpression='#d, {}, {}, {}'.format(key_temperature, key_rainfall, key_humidity),
            ExpressionAttributeNames = {'#d':'date'},
            KeyConditionExpression=Key(key_region).eq(region) & Key(key_date).between(starting_date, ending_date),
            ExclusiveStartKey=response[key_last_evaluated]
        )
        items.append(response['Items'])

    origin_data = []
    if len(items) == 0:
        if elapsed_days < __month_time_delta:
            # Try to get month average
            ending_date = parse_string_to_date(ending_date)
            starting_date =  ending_date - __month_time_delta
            return get_batch_trading_data(product, region, starting_date, ending_date)
        else:
            raise Exception('Get Bath Weather: There is no sufficient matching data!')
    else:
        keys = [key_temperature, key_rainfall, key_humidity]
        for data in items:
            context = { key_date:data[key_date] }
            for key in keys:
                if key in data:
                    context[key] = data[key]
            origin_data.append(context)

        return fix_missing_data(origin_data, keys, starting_date, ending_date)

def get_batch_trading_data(product, region, starting_date, ending_date):
    starting_date = str(starting_date)
    ending_date = str(ending_date)
    check_product(product)
    check_region(region)
    check_date(starting_date)
    check_date(ending_date)

    response = product_price_table.query(
        ProjectionExpression='#d, {}'.format(region),
        FilterExpression='attribute_exists(#r)',
        ExpressionAttributeNames={'#d' : key_date, '#r': region },
        KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).between(starting_date, ending_date)
    )
    items = response['Items']
    while key_last_evaluated in response:
        response = product_price_table.query(
            ProjectionExpression='#d, trading_data.{}'.format(region),
            FilterExpression='attribute_exists(#r)',
            ExpressionAttributeNames={'#d' : key_date, '#r': region },
            KeyConditionExpression=Key(key_product).eq(product) & Key(key_date).between(starting_date, ending_date),
            ExclusiveStartKey=response[key_last_evaluated]
        )
        items.appen(response['Items'])

    elapsed_days = parse_string_to_date(ending_date) - parse_string_to_date(starting_date)
    if len(items) == 0:
        if elapsed_days < __month_time_delta:
            # Try to get month average
            ending_date = parse_string_to_date(ending_date)
            starting_date =  ending_date - __month_time_delta
            return get_batch_trading_data(product, region, starting_date, ending_date)
        else:
            raise Exception('Get Bath Trading Data: There is no sufficient matching data!')
    else:
        origin_data = []
        for data in items:
            origin_data.append({
                key_date:data[key_date],
                key_price:data[region][key_price],
                key_turnover:data[region][key_turnover]
            })
        return fix_missing_data(origin_data, [key_price, key_turnover], starting_date, ending_date)

def fix_missing_data(oringin_datas, keys, starting_date, ending_date):
    if len(oringin_datas) == 0:
        raise Exception('Fix missing data: Oringin_data is empty!')
    if not key_date in oringin_datas[0]:
        raise Exception('Fix missing data: Oringin_data must contain "date" information!')

    value_sum = {}
    value_count = {}
    value_mean = {}
    value_record = {}
    no_data_index = {}

    ending_date = str(ending_date)
    expected_date = parse_string_to_date(starting_date)
    for key in keys:
        value_sum[key] = 0
        value_count[key] = 0
        value_record[key] = []
        no_data_index[key] = []

    index = 0
    for data in oringin_datas:
        while str(expected_date) < data[key_date]:
            for key in keys:
                value_record[key].append(__MISSING_VALUE)
                no_data_index[key].append(index)
            index += 1
            expected_date += __one_day_timedelta

        for key in keys:
            if key in data:
                value = data[key]
                value_sum[key] += value
                value_count[key] += 1
                value_record[key].append(value)
            else:
                value_record[key].append(__MISSING_VALUE)
                no_data_index[key].append(index)

        expected_date += __one_day_timedelta
        index += 1

    while str(expected_date) <= ending_date:
        for key in keys:
            value_record[key].append(__MISSING_VALUE)
            no_data_index[key].append(index)
        index += 1
        expected_date += __one_day_timedelta

    for key in keys:
        if value_count[key] == 0:
            raise Exception('Fix missing data: There is no "{}" data in the origin_data!'.format(key))
            value_mean[key] = 0
        else:
            value_mean[key] = value_sum[key]/value_count[key]

    for key in keys:
        for i in no_data_index[key]:
            value_record[key][i] = value_mean[key]

    context = {}
    for key in keys:
        context[key] = value_record[key]
    return context

