#
# VPC Flow Logs with Geo Data (ETL)
# Usage: works as a triggered Lambda function + locally by specifying an S3 source
#
# pylint: disable=line-too-long,global-statement,bare-except,missing-function-docstring,broad-except
# pylint: disable=no-else-return,missing-module-docstring,too-many-statements,too-many-locals,too-many-branches

import os
import gzip
import shutil
import csv
import json
import time
from datetime import datetime, date
import ipaddress
import traceback
import boto3
from boto3.dynamodb.conditions import Key
import requests

#
# LOCAL TESTING
#
LOCAL_RUN = False
BUCKET_NAME = ''
BUCKET_KEY_PATH = ''
CW_LG_STREAM_NAME = BUCKET_KEY_PATH.split('/')[-1]

#
# ENV VARS
#
IPSTACK_ACCESS_KEY = os.environ.get('IPSTACK_ACCESS_KEY', '')
DDB_TABLE_NAME = os.environ.get('DDB_TABLE_NAME', '')
DDB_TABLE_PK_ATTRIB = os.environ.get('DDB_TABLE_PK_ATTRIB', 'ip_address')
DDB_TABLE_GEO_ATTRIB = os.environ.get('DDB_TABLE_GEO_ATTRIB', 'geo_info')
DDB_TABLE_ING_ATTRIB = os.environ.get('DDB_TABLE_TTL_ATTRIB', 'ingestion_date')
DDB_TABLE_TTL_ATTRIB = os.environ.get('DDB_TABLE_TTL_ATTRIB', 'expiry_in_sec')
DDB_TTL_EXPIRY_SEC = os.environ.get('DDB_TTL_EXPIRY_SEC', 2592000) # 30 days
DDB_REQ_SLEEP_SEC = os.environ.get('DDB_REQ_SLEEP_SEC', 0)
CW_LOG_GROUP_NAME = os.environ.get('CW_LOG_GROUP_NAME', '')

#
# CONSTANTS
#
CWD = '/tmp/'
LOGS_DT_FORMAT = '%Y-%m-%d %H:%M:%S'
LOGS_COUNT = 0
CACHE_LOCAL_HIT_COUNT = 0
CACHE_DDB_HIT_COUNT = 0

IPSTACK_API_URL = 'http://api.ipstack.com/'
IPSTACK_PARAMS = {
    'access_key': IPSTACK_ACCESS_KEY,
    'fields': 'continent_code,continent_name,country_code,country_name,city,latitude,longitude'
}

# http://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
IANA_PROTOCOLS = {
    "1": "ICMP",
    "4": "IPv4",
    "6": "TCP",
    "17": "UDP",
    "27": "RDP",
    "41": "IPv6",
    "115": "L2TP"
}

TCP_FLAGS = {
    "1": "FIN",
    "2": "SYN",
    "3": "SYN+FIN",
    "4": "RST",
    "18": "SYN-ACK",
    "19": "SYN-ACK+FIN"
}

ips_geo_local_cache = {} # pylint: disable=invalid-name

#
# DEFINITIONS
#
def epoch_to_datetime(epoch: int, dt_format: str) -> str:
    return datetime.fromtimestamp(epoch).strftime(dt_format)

def decode(value: str):
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return value
    else:
        return value

def is_ip_private(ip_addr: str) -> bool:
    if ip_addr is None:
        return False
    else:
        return ipaddress.ip_address(ip_addr).is_private

def get_geo_details(ipaddr: str, key_prefix: str, ddb_client, ddb_table) -> dict:
    geo_dict = None
    global CACHE_LOCAL_HIT_COUNT
    global CACHE_DDB_HIT_COUNT

    print('[INFO] Requesting geo info from local cache for: {}'.format(ipaddr))
    if ipaddr in ips_geo_local_cache:
        print('[INFO] IPs local cache hit.')
        CACHE_LOCAL_HIT_COUNT += 1
        geo_dict = ips_geo_local_cache[ipaddr]
    else:
        print('[INFO] Requesting geo info from DDB cache for: {}'.format(ipaddr))
        ddb_cache_response = ddb_table.query(
            KeyConditionExpression=Key(DDB_TABLE_PK_ATTRIB).eq(ipaddr),
            ReturnConsumedCapacity='TOTAL',
        )
        # print('[DEBUG] DDB cache response: {}'.format(ddb_cache_response))
        time.sleep(DDB_REQ_SLEEP_SEC)

        if ddb_cache_response['Count'] != 0:
            ddb_cache_item = ddb_cache_response['Items']
            print('[INFO] IPs DDB cache hit.')
            CACHE_DDB_HIT_COUNT += 1
            geo_dict = json.loads(ddb_cache_item[0][DDB_TABLE_GEO_ATTRIB].replace("'", "\"").replace("None", "null"))
            # print('[DEBUG] DDB item: {}'.format(geo_dict))
            print('[INFO] Updating IPs local cache with geo data for: {}'.format(ipaddr))
            ips_geo_local_cache[ipaddr] = geo_dict
        else:
            print('[INFO] Requesting geo info from IPStack for: {}'.format(ipaddr))
            geo_response = requests.get(url=IPSTACK_API_URL + ipaddr, params=IPSTACK_PARAMS)
            geo_dict = geo_response.json()
            # print('[DEBUG] IPStack response ({}): {}'.format(geo_response.status_code, geo_dict))
            print('[INFO] Updating IPs local cache with geo data for: {}'.format(ipaddr))
            ips_geo_local_cache[ipaddr] = geo_dict
            print('[INFO] Updating IPs DDB cache with geo data for: {}'.format(ipaddr))
            ddb_client.put_item(
                TableName=DDB_TABLE_NAME,
                Item={
                    DDB_TABLE_PK_ATTRIB: {'S': ipaddr},
                    DDB_TABLE_GEO_ATTRIB: {'S': str(geo_dict)},
                    DDB_TABLE_ING_ATTRIB: {'S': str(date.today().strftime('%Y-%m-%d'))},
                    DDB_TABLE_TTL_ATTRIB: {'N': str(round(time.time())+DDB_TTL_EXPIRY_SEC)}
                },
                ReturnConsumedCapacity='TOTAL',
            )

        # print('[DEBUG] Local cache: {}'.format(ips_geo_local_cache))

    geo_keys_map = {
        'continent_code': key_prefix + '-continent-code',
        'continent_name': key_prefix + '-continent-name',
        'country_code': key_prefix + '-country-code',
        'country_name': key_prefix + '-country-name',
        'city': key_prefix + '-city',
    }
    geo_dict_final = {geo_keys_map.get(k, k):v for k, v in geo_dict.items()}

    geo_dict_final[key_prefix + '-geopoint'] = '{},{}'.format(
        geo_dict_final['latitude'], geo_dict_final['longitude']
    )
    for item in ['latitude', 'longitude']:
        geo_dict_final.pop(item)

    return geo_dict_final

def put_logs_in_cw(cw_lg: str, cw_lg_stream: str, logs: dict):
    print("[INFO] Creating CW log stream '{}' in '{}' log group...".format(cw_lg_stream, cw_lg))
    cwlogs_client = boto3.client('logs')
    cwlogs_client.create_log_stream(
        logGroupName=cw_lg,
        logStreamName=cw_lg_stream,
    )
    log_event_items = []
    for log in logs:
        log_event_item = {
            'timestamp': round(time.time())*1000,
            'message': json.dumps(log)
        }
        log_event_items.append(log_event_item)
    print('[INFO] Putting log events to CW...')
    cw_log_event = {
        'logGroupName': cw_lg,
        'logStreamName': cw_lg_stream,
        'logEvents': log_event_items
    }
    cwlogs_client.put_log_events(**cw_log_event)

#
# FIRE
#
def handler(event, context):
    logs_json_data = [] # pylint: disable=invalid-name
    global BUCKET_NAME
    global BUCKET_KEY_PATH
    global LOGS_COUNT
    global CW_LG_STREAM_NAME

    if event:
        # print('[DEBUG] Event payload: {}'.format(event))
        s3_file_object = event['Records'][0]

        BUCKET_NAME = str(s3_file_object['s3']['bucket']['name']) # pylint: disable=redefined-outer-name,invalid-name
        print('[INFO] Event received from S3: {}'.format(BUCKET_NAME))

        BUCKET_KEY_PATH = str(s3_file_object['s3']['object']['key']) # pylint: disable=redefined-outer-name,invalid-name
        print('[INFO] Log file to be downloaded: {}'.format(BUCKET_KEY_PATH))
        CW_LG_STREAM_NAME = BUCKET_KEY_PATH.split('/')[-1]

    print('[INFO] Downloading GZ log file...')
    s3_client = boto3.client('s3')
    s3_client.download_file(BUCKET_NAME, BUCKET_KEY_PATH, CWD + 'vpcflowlogs.gz')

    print('[INFO] Extracting CSV log file from GZ...')
    with gzip.open(CWD + 'vpcflowlogs.gz', 'rb') as log_gz:
        with open(CWD + 'vpcflowlogs.csv', 'wb') as log_csv:
            shutil.copyfileobj(log_gz, log_csv)

    print('[INFO] Converting CSV logs into JSON...')
    logs_csv = open(CWD + 'vpcflowlogs.csv', 'r')
    reader = csv.DictReader(logs_csv, delimiter=' ')
    for log in reader:
        LOGS_COUNT += 1
        logs_json_data.append(log)
    print('[INFO] Log entries found: {}'.format(LOGS_COUNT))

    print('[INFO] Converting, enriching, cleaning and decoding log values...')
    ddb_client = boto3.client('dynamodb')
    ddb_table = boto3.resource('dynamodb').Table(DDB_TABLE_NAME)

    for log in logs_json_data:
        # print('[DEBUG] BEFORE Log item: {}'.format(json.dumps(log)))
        for key, value in log.items():
            if value == '-':
                log[key] = None

        log['start_in_sec'] = log['start']
        log['start'] = epoch_to_datetime(int(log['start']), LOGS_DT_FORMAT)
        log['end_in_sec'] = log['end']
        log['end'] = epoch_to_datetime(int(log['end']), LOGS_DT_FORMAT)
        log['protocol'] = IANA_PROTOCOLS.get(log['protocol'], log['protocol'])
        log['tcp-flags'] = TCP_FLAGS.get(log['tcp-flags'], log['tcp-flags'])
        log['src-location'] = 'private' if is_ip_private(log['pkt-srcaddr']) else 'global'
        log['dst-location'] = 'private' if is_ip_private(log['pkt-dstaddr']) else 'global'

        for key, value in log.items():
            if key != 'account-id':
                log[key] = decode(value)

        try:
            if log['src-location'] == 'global':
                geo_data = get_geo_details(log['pkt-srcaddr'], 'src', ddb_client, ddb_table)
                log.update(geo_data)

            if log['dst-location'] == 'global':
                geo_data = get_geo_details(log['pkt-dstaddr'], 'dst', ddb_client, ddb_table)
                log.update(geo_data)
        except RuntimeError:
            pass # log.update() -> dict mutated during iteration
        except Exception:
            print('[ERROR] Fetching geo data failed: {}'.format(traceback.print_exc()))

        # print('[DEBUG] AFTER Log item: {}'.format(json.dumps(log)))

    if LOCAL_RUN is False:
        put_logs_in_cw(CW_LOG_GROUP_NAME, CW_LG_STREAM_NAME, logs_json_data)
    else:
        print('[INFO] Dumping logs to {} file...'.format(CWD + 'vpcflowlogs.json'))
        logs_json = open(CWD + 'vpcflowlogs.json', 'w')
        json.dump(logs_json_data, sort_keys=False, fp=logs_json)

    print('[STAT] Local cache hit count: {}'.format(CACHE_LOCAL_HIT_COUNT))
    print('[STAT] DDB cache hit count: {}'.format(CACHE_DDB_HIT_COUNT))
    print('[STAT] Cache saves count: {}'.format(LOGS_COUNT-CACHE_LOCAL_HIT_COUNT-CACHE_DDB_HIT_COUNT))
    print('[STAT] Cache hit ratio: {:.2f}'.format((CACHE_LOCAL_HIT_COUNT+CACHE_DDB_HIT_COUNT)/LOGS_COUNT))

#
# MAIN
#
if __name__ == '__main__':
    CWD = './'
    handler('', '')
