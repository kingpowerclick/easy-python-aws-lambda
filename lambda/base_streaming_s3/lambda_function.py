from __future__ import print_function
from datetime import datetime

import logging
import os
import re
import time
import zlib
import json
import base64
import string
import random

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_S3BUCKET = os.getenv('DEFAULT_S3BUCKET')
if not DEFAULT_S3BUCKET:
    raise Exception('Default s3 bucket in lambda environment id needed.')


def lambda_handler(event, context):
    logger.info('Trigger event...')

    streaming_event = event.get('StreamingS3Event', None)
    if not streaming_event:
        raise Exception('No streaming event defined.')

    data_type = streaming_event.get('data_type', None)
    if not data_type:
        raise Exception('No data_type defined.')

    s3_key = streaming_event.get('s3_key', None)
    if not s3_key:
        raise Exception('No s3_key defined.')

    data_type_ucase = data_type.upper()

    if data_type_ucase == 'JSON':
        _op_json(_get_compress_or_raw_data(streaming_event), streaming_event)

    elif data_type_ucase == 'CSV':
        _op_csv(_get_compress_or_raw_data(streaming_event), streaming_event)

    else:
        raise Exception('Specify data type is not support')


def _get_compress_or_raw_data(streaming_event):
    comp_data = streaming_event.get('compress_data', None)
    if comp_data:
        return zlib.decompress(base64.b64decode(comp_data.encode())).decode('utf-8')
    else:
        raw_data = streaming_event.get('raw_data', None)
        if not raw_data:
            raise Exception('No data stream for processing')

        try:
            return json.loads(raw_data)
        except Exception as e:
            try:
                json.loads(raw_data, strict=False)
                return raw_data
            except Exception as inner_e:
                raise Exception('Stream data is not a valid json format. Should be escaped string or raw format')


def _id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def _get_timestamp_microsec():
    return int(round(time.time() * 1000))


def _reg_sub_slash(phrase):
    return re.sub('^\/|\/$', '', phrase)


def _get_s3_key(ts, streaming_event):
    s3_key = streaming_event.get('s3_key', None)
    s3_key = s3_key if s3_key else _id_generator()

    u_def_pref_key = streaming_event.get('s3_prefix_key', None)
    u_def_pref_key = u_def_pref_key if not u_def_pref_key else _reg_sub_slash(u_def_pref_key)

    is_time_flag = streaming_event.get('is_time_flag', True)

    if is_time_flag:
        ts_str = str(ts)

        u_def_time_pref_key = streaming_event.get('s3_time_prefix_key', None)
        u_def_time_pref_key = _reg_sub_slash(u_def_time_pref_key) if u_def_time_pref_key else '%Y/%m/%d/%H'

        dt_str = datetime.utcfromtimestamp(ts / 1000).strftime(u_def_time_pref_key)

        return ('{}/{}_{}'.format(dt_str, s3_key, ts_str)
                if not u_def_pref_key
                else '{}/{}/{}_{}'.format(u_def_pref_key, dt_str, s3_key, ts_str))

    else:
        return '{}'.format(s3_key) if not u_def_pref_key else '{}/{}'.format(u_def_pref_key, s3_key)


def _get_s3_bucket_name(streaming_event):
    u_def_name = streaming_event.get('s3_bucket', None)
    return _reg_sub_slash(u_def_name if u_def_name else DEFAULT_S3BUCKET)


def _put_json_object(b_data, streaming_event):
    ts = _get_timestamp_microsec()
    s3_client = boto3.client('s3')
    s3_client.put_object(ACL='authenticated-read',
                         Body=json.loads(b_data.read()),
                         Bucket=_get_s3_bucket_name(streaming_event),
                         ContentEncoding='utf-8',
                         Key='{}.json'.format(_get_s3_key(ts, streaming_event)))


def _op_json(data, streaming_event):
    with open("/tmp/temp_file.json", "w+") as temp_file:
        json.dump(data, temp_file, separators=(',', ': '), indent=4, sort_keys=True, ensure_ascii=False)

    with open("/tmp/temp_file.json", "rb") as temp_file:
        _put_json_object(temp_file, streaming_event)


def _op_csv(data, streaming_event):
    pass
