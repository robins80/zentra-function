import ast
from azure.storage.blob import BlockBlobService
import azure.functions as func
import datetime
from dateutil.tz import tzlocal
import json
import logging
from multiweatherapi import multiweatherapi
import os
import pprint
import psycopg2
from requests import Session, Request
import time

import requests

logger = logging.getLogger(__name__)
parms = None
readings = None

class Timer:    
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start

def connect_database():    # Connect to the postgres database.
    server = os.environ['SERVER']
    database = os.environ['DATABASE']
    username = os.environ['USER']
    password = os.environ['PASSWORD']
    sslmode = os.environ['SSLMODE']

    logger.info('connect_database: Connecting to the database...')

    # Build the connection string.
    connection_string = 'host=%s user=%s dbname=%s password=%s sslmode=%s' % (server, username, database, password, sslmode)
    connection = psycopg2.connect(connection_string)
    logger.info('connect_database: Connection established...')

    return connection

def check_parms(**parms):
    # logger.info('DEBUG: The parms to be checked are: ' + str(parms))
    # Check to see if we have a vendor.  This is to catch if one is not listed in the station_info table.
    if (parms['vendor'] is None or len(parms['vendor']) == 0):
        raise Exception('ERROR: Vendor was not specified.  Check entry for station with id ' + parms['sn'] + ' in the station_info table.')

    # print('start date = ' + str(parms['start_date']) + ', end date = ' + str(parms['end_date']))

    # Check to see if we have an identifier.
    if (parms['sn'] is None):
        raise Exception('ERROR: Station identifier is None.  Check the station_info table for null sn entries.')

    if(len(parms['sn']) == 0):
        raise Exception('ERROR: Station identifier is empty.  Check the station_info table for empty string sn entries.')

    # Check to make sure that the start date is not after the end date.  If they are, switch them.
    if parms['start_date'] > parms['end_date']:
        raise Exception('Start date is after the end date.')

def get_range(sn, db, vendor):
    # Get the last time this station was polled.

    local = tzlocal()
    # Use this when deploying the function.
    query = 'Select poll_date from raw_data poll_date where sn = \'' + sn + '\' order by poll_date desc fetch first row only'
    cursor = db.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()

    # Set the end date of the range for now.
    end_date = datetime.datetime.now(datetime.timezone.utc)
    
    # If there is no data in the raw_data table, then set the start_date to be 24 hours ago.  Make sure to make it time zone aware.
    if results:
        start_date = results[0][0]
    else:
        start_date = end_date - datetime.timedelta(seconds = 86400)
    

    logger.info('start_date: ' + str(start_date) + ', end_date: ' + str(end_date))

    # If this is a Davis API, we need to make the date range 24 hours as they do not support date ranges larger than that.
    if (vendor == 'davis') and (end_date - start_date > datetime.timedelta(hours = 24)):
        start_date = end_date - datetime.timedelta(hours = 24)

    # Use this for local testing as needed.
    # start_date = end_date - datetime.timedelta(hours = 1)

    logger.info('Exiting date_range...')
    return start_date, end_date

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Send Logging to stdout.
    streamer = logging.StreamHandler()
    streamer.setLevel(logging.DEBUG)
    logger.addHandler(streamer)

    logger.info('main: Starting...')
    logger.info('the body is ' + str(req.get_body()))
    # logger.info('DEBUG: The local time zone is ' + str(datetime.datetime.utcnow().astimezone().tzinfo))

    parms = req.get_json()
    # logger.info('The time is ' + str(datetime.datetime.now()))
    # logger.info('DEBUG: parms is of type ' + str(type(parms)))
    # logger.info(parms)

    # Connect to the blob service
    account = os.environ.get('AZURE_ACCOUNT')
    key = os.environ.get('AZURE_KEY')
    blobservice = BlockBlobService(account_name = account, account_key = key)

    # Delete the raw_data.json file if it exists.
    if blobservice.exists(container_name = 'zentra', blob_name = 'raw_data.json'):
        blobservice.delete_blob(container_name = 'zentra', blob_name = 'raw_data.json')

    # Delete the parsed.data file if it exists.
    if blobservice.exists(container_name = 'zentra', blob_name = 'parsed.data'):
        blobservice.delete_blob(container_name = 'zentra', blob_name = 'parsed.data')

    # Connect to the database
    logger.info('main: Connecting to the database...')
    db = connect_database()

    # Get the date range and add it to the parms.
    logger.info('Setting date range...')
    start_date, end_date = get_range(parms['sn'], db, parms.get('vendor'))
    parms['start_date'] = start_date
    parms['end_date'] = end_date
    # logger.info('DEBUG: start date = ' + str(parms['start_date']) + ', end date = ' + str(parms['end_date']))
    # logger.info('DEBUG: Types are: start_date - ' + str(type(start_date)) + ', end_date - ' + str(type(end_date)))
    # logger.info('DEBUG: start_date is: ' + str(start_date.tzinfo))
    # logger.info('DEBUG: end_date is: ' + str(end_date.tzinfo))

    # If the vendor is rainwise, we need to add in mac, username (same as mac), sid, and pid.
    if (parms['vendor'] == 'rainwise'):
        logger.info('Setting rainwise parms...')
        parms['username'] = parms['user_id']
        parms['mac'] = parms['user_id']
        parms['pid'] = parms['apisec']
        parms['sid'] = parms['apikey']
    
    # If the vendor is Campbell, we need to use sn for station_id, password for user_passwd and client_id for station_lid.
    # Call the multiweatherapi library to poll the API.
    if (parms['vendor'] == 'campbell'):
        logger.info('Setting campbell parms...')
        parms['station_id'] = parms['sn']
        parms['station_lid'] = parms['client_id']
        parms['user_passwd'] = parms['password']

    # Check the parms.
    check_parms(**parms)

    logger.info ('Calling multiweather version ' + multiweatherapi.get_version())

    # logger.info('DEBUG: The parms being sent are: ')
    # for key, value in parms.items():
    #     logger.info('DEBUG: ' + str(key) + " = " + str(value))

    success = True

    try:
        logger.info('main: Calling the ' + parms.get('vendor') + ' API...')
        
        with Timer() as timer: 
            readings = None
            readings = multiweatherapi.get_reading(**parms)

        logger.info('main: The call to ' + parms.get('vendor') + ' took %.03f seconds.' % timer.interval)

        # To Do: We will need to check readings to see if there is truly data here and set success to false if there isn't.  That way,
        # we don't create a raw_data.json file that has no real info in it, and therefore the part of the ddata factory that stores data will
        # not be called.
    except Exception as error:
        logger.info('====================================')
        logger.info('Error is')
        logger.info(error)
        logger.info('====================================')
        success = False
        if error is None:
            output = "No error message"
        else:
            output = str(error)
        logger.error('Encountered this error calling the API:\n=====>' + output)
    
    # If we didn't encounter an error calling the API, create our "raw data" JSON, which will have the following information:
    # The identifier that the vendor used to uniquely ID a station.
    # The name of the api provider.
    # The date this station was polled.
    # The raw JSON file returned by the multiweatherapi call (readings)

    if success:
        raw_data = {
            'sn'         : parms.get('sn'),
            'vendor'     : parms.get('vendor'),
            'poll_date'  : datetime.datetime.now().strftime('%m/%d/%Y %H:%M'),
            'data'       : readings.resp_raw
        }

        logger.info('main: Writing the raw_data table entry to blob storage...')
        # logger.info('*************************************************')
        # logger.info('DEBUG: main, multiweatherusa debug output is ' + str(readings.resp_debug))
        # logger.info('*************************************************')
        # logger.info('DEBUG: main,  readings.resp_raw = ' + str(readings.resp_raw))
        # logger.info('*************************************************')

        try:
            blobservice.create_blob_from_text(container_name = 'zentra', blob_name = 'raw_data.json', text = json.dumps(raw_data), encoding='utf-8')
        except Exception as error:
            raise Exception('main: %s' % str(error))    

        logger.info('main: Writing the parsed.data file to blob storage...')
        print('parsed data is ' + str(readings.resp_parsed))
        try:
            blobservice.create_blob_from_text(container_name = 'zentra', blob_name = 'parsed.data', text = json.dumps(readings.resp_parsed), encoding='utf-8')
        except Exception as error:
            raise Exception('main: %s' % str(error))          

    logger.info('main: Disconnecting from the database...')
    db.close()

    logger.info('main: Returning data and ending...')
    return func.HttpResponse(str(readings))