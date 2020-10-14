import time
from sqlalchemy import create_engine, text
import requests
from requests import Request
import json
import datetime
import sys
import signal
import argparse
import os
import dateutil.parser
from dateutil.parser import parse
from math import ceil
import logging 

def init_db(engine):
    print("init_db()")
    create_candle_db_statement = text("""
        CREATE TABLE IF NOT EXISTS public.bitmex_trades_2 (
            load_timestamp timestamp with time zone default (now() at time zone 'utc'),
            timestamp timestamp with time zone,
            symbol TEXT,
            side TEXT,
            size NUMERIC,
            price NUMERIC,
            tickDirection TEXT,
            trdMatchID TEXT,
            grossValue NUMERIC,
            homeNotional NUMERIC,
            foreignNotional NUMERIC)
    """)
    engine.execute(create_candle_db_statement)

logging.basicConfig(
    filename='/Users/user/Documents/PYTHON_PROJECTS/bitmex_trade_grab/log.txt', 
    format='%(levelname)s: %(asctime)s %(message)s', 
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.DEBUG)

def get_load_data_statement(api_resp_bucketd, meta):
    """
    Args:
        api_resp_buckets:   a list of bucketd response items like below.
        meta:               

    {
        "timestamp": "2015-09-25T12:34:25.706Z",
        "symbol": "XBTUSD",
        "side": "Buy",
        "size": 1,
        "price": 239.99,
        "tickDirection": "ZeroPlusTick",
        "trdMatchID": "7ff37f6c-c4f6-4226-20f8-460ec68d4b50",
        "grossValue": 239990,
        "homeNotional": 0.0023999,
        "foreignNotional": 0.575952001
    },
    """
    # for loop: 
    # insert_statement = []
    # for resp_data in api_resp_bucketd:
    #     # processed_insert_data = dict()
    #     # for k, v in resp_data.items():
    #     #     processed_insert_data[k] = v if v else "NULL"
    #     # resp_data = processed_insert_data
    #     query_insert_values = """
    #     (
    #         '{load_timestamp}',
    #         '{timestamp}',
    #         '{symbol}',
    #         '{side}',
    #         {size},
    #         {price},
    #         '{tickDirection}',
    #         '{trdMatchID}',
    #         {grossValue},
    #         {homeNotional},
    #         {foreignNotional}
    #     )
    #     """.format(
    #         load_timestamp = meta['load_timestamp'],
    #         timestamp = parse(str(resp_data['timestamp'])),
    #         symbol = resp_data['symbol'],
    #         side = resp_data['side'],
    #         size = resp_data['size'],
    #         price = resp_data['price'],
    #         tickDirection = resp_data['tickDirection'],
    #         trdMatchID = resp_data['trdMatchID'],
    #         grossValue = resp_data['grossValue'],
    #         homeNotional = resp_data['homeNotional'],
    #         foreignNotional = resp_data['foreignNotional']
    #     )
    #         insert_statement.append(query_insert_values)

    # List comprehension alternative: 
    insert_statement = ["""
    (
        '{load_timestamp}',
        '{timestamp}',
        '{symbol}',
        '{side}',
        {size},
        {price},
        '{tickDirection}',
        '{trdMatchID}',
        {grossValue},
        {homeNotional},
        {foreignNotional}
    )
    """.format(
        load_timestamp = meta['load_timestamp'],
        timestamp = parse(str(resp_data['timestamp'])),
        symbol = resp_data['symbol'],
        side = resp_data['side'],
        size = resp_data['size'],
        price = resp_data['price'],
        tickDirection = resp_data['tickDirection'],
        trdMatchID = resp_data['trdMatchID'],
        grossValue = resp_data['grossValue'],
        homeNotional = resp_data['homeNotional'],
        foreignNotional = resp_data['foreignNotional']
    ) for resp_data in api_resp_bucketd]

    insert_statement = text(
        """
            INSERT INTO public.bitmex_trades_2 (
                load_timestamp,
                timestamp,
                symbol,
                side,
                size,
                price,
                tickDirection,
                trdMatchID,
                grossValue,
                homeNotional,
                foreignNotional) 
            VALUES {} 
            """.format(','.join(insert_statement)) + ';'
    )
    return insert_statement


def BitMEX_ETL_fn(engine, meta):
    print("BitMEX_ETL_fn()")
    rate_limit = meta['rate_limit']
    start_time = datetime.datetime.utcnow()
    total_inserted = 0

    first_run = True
    most_recent_timestamp_dt = None
    start_from_timestamp = None
    api_call_times = []

    i=0
    buffer=[]
    last_insertion_time=0
    average_insertion=0
    last_trade_inserted='None'

    start_param=0
    while True:
        iteration_start_time = datetime.datetime.utcnow()
        if first_run is True:
            start_from_timestamp = engine.execute(
                text(
                    """
                        SELECT
                            MAX(timestamp)
                        FROM
                            public.bitmex_trades_2
                        WHERE
                            symbol = '{symbol}'
                    """.format(
                        symbol=meta['symbol'],
                    )
                )
            ).fetchall()[0][0]
            last_trade_inserted = start_from_timestamp
            if start_from_timestamp is None:
                start_from_timestamp = '2010-01-01 00:00:00'
            else:
                # use the appropriate timedelta offset.
                start_from_timestamp = str(start_from_timestamp)
            first_run = False
            start_time = datetime.datetime.utcnow()
        elif first_run is False and most_recent_timestamp_dt is not None:
            start_from_timestamp = str(most_recent_timestamp_dt)
            # print('nth run - using {}'.format(start_from_timestamp))
        s = requests.Session()

        loop_start_time = datetime.datetime.utcnow()
        for j in range(0, rate_limit):
            api_params = dict(
                symbol=meta['symbol'],
                count=1000,
                start=start_param,
                reverse='false',
                startTime=start_from_timestamp
            )
            prelim_request = Request(
                'GET',
                url=api_base_url,
                params=api_params,
            )
            prepped_request = prelim_request.prepare()
            try:
                response = s.send(prepped_request)

                api_call_times.append(datetime.datetime.utcnow())
                i+=1

            except Exception as e:
                logging.critical('EXCEPTION')
                logging.critical(e)
                print(e)
                return
            if response.status_code != 200 or len(response.json()) == 0:
                logging.error('UNEXPECTED status CODE: {}'.format(response.status_code))
                time.sleep(10)
                continue
            
            most_recent_timestamp_str = response.json()[-1]['timestamp']
            most_recent_timestamp_dt = dateutil.parser.parse(most_recent_timestamp_str)
            start_from_timestamp = str(most_recent_timestamp_dt)

            # set the correct start_param
            total_entries_with_recent_timestamp = len(list(filter(lambda x: x['timestamp']==most_recent_timestamp_str, response.json())))
            if total_entries_with_recent_timestamp == 1000:
                start_param = start_param + total_entries_with_recent_timestamp
            else: 
                start_param = total_entries_with_recent_timestamp

            # add response to buffer
            buffer = buffer+response.json()            

            end_time = datetime.datetime.utcnow()
            # remove all api calls older than 60 seconds
            api_call_times = list(filter(lambda x: (end_time-x).total_seconds()<=60, api_call_times))

            # print statistics
            os.system("printf '\033c'")
            session_time = end_time - start_time
            print('------------------------------------------------------------')          
            print('session length:            {}'.format(session_time))        
            print('last insertion time:       {}'.format(last_insertion_time))
            print('total inserted:            {}'.format(total_inserted))
            print('last trade inserted:       {}'.format(last_trade_inserted))
            print('average insertion rate:    {}/minute'.format(round(average_insertion)))
            print('')
            print('API calls - last minute:   {}'.format(len(api_call_times)))
            print('API calls - total:         {}'.format(i))
            print('API calls - average:       {}/minute'.format(round(i/(session_time.total_seconds()/60),1)))
            print('')
            print('buffer size:               {}'.format(len(buffer)))
            print('grabbed up to:             {}'.format(most_recent_timestamp_dt))
            print('------------------------------------------------------------')        
        
        loop_time = datetime.datetime.utcnow() - loop_start_time 
   
        insert_length = len(buffer)      
        print("INSERTING {} entries".format(insert_length))           
        print('------------------------------------------------------------')                     
        insertion_start_time = datetime.datetime.utcnow()
        engine.execute(
            get_load_data_statement(
                api_resp_bucketd=buffer,
                meta=meta
            )
        )
        last_trade_inserted = 'time: {}, side: {}, price: {}, size: {}'.format(dateutil.parser.parse(buffer[-1]['timestamp']), buffer[-1]['side'],buffer[-1]['price'],buffer[-1]['size'])

        last_insertion_time = datetime.datetime.utcnow() - insertion_start_time
        total_inserted += insert_length

        end_time = datetime.datetime.utcnow()
        api_call_times = list(filter(lambda x: (end_time-x).total_seconds()<=60, api_call_times))
        os.system("printf '\033c'")
        session_time = end_time - start_time
        average_insertion = total_inserted/(session_time.total_seconds()/60)
        print('------------------------------------------------------------')          
        print('session length:            {}'.format(session_time))        
        print('last insertion time:       {}'.format(last_insertion_time))
        print('total inserted:            {}'.format(total_inserted))
        print('last trade inserted:       {}'.format(last_trade_inserted))
        print('average insertion rate:    {}/minute'.format(round(average_insertion)))
        print('')
        print('API calls - last minute:   {}'.format(len(api_call_times)))
        print('API calls - total:         {}'.format(i))
        print('API calls - average:       {}/minute'.format(round(i/(session_time.total_seconds()/60),1)))
        print('')
        print('buffer size:               {}'.format(len(buffer)))
        print('grabbed up to:             {}'.format(most_recent_timestamp_dt))
        print('------------------------------------------------------------')   
        print("INSERTED {} entries".format(insert_length))           
        print('------------------------------------------------------------')      
        buffer = []
        
        print('insertion + loop time:     {}'.format(loop_time+last_insertion_time))
        end_time = datetime.datetime.utcnow()
        api_call_times = list(filter(lambda x: (end_time-x).total_seconds()<=60, api_call_times))
        while len(api_call_times)>=rate_limit:
            sleep_len = ceil((60 - (end_time-min(api_call_times)).total_seconds()))
            print('RATE LIMIT REACHED - sleeping for {} seconds'.format(sleep_len))
            print('------------------------------------------------------------') 
            time.sleep(sleep_len)
            end_time = datetime.datetime.utcnow()
            api_call_times = list(filter(lambda x: (end_time-x).total_seconds()<=60, api_call_times))

if __name__ == '__main__':
    logging.info('STARTING UP')
    print(datetime.datetime.now())

    api_base_url = 'https://www.bitmex.com/api/v1/trade'
    db_string = 'postgresql://postgres:docker@localhost:5433/postgres'
    engine = create_engine(db_string)
    
    init_db(engine)
    BitMEX_ETL_fn(
        engine=engine,
        meta={
            'symbol': 'XBTUSD',
            'load_timestamp': datetime.datetime.utcnow(),
            'rate_limit': 30
        }
    )
