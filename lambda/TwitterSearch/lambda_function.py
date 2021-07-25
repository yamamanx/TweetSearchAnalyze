import json
import os
import boto3
import base64
import logging
import traceback
from botocore.exceptions import ClientError
from requests_oauthlib import OAuth1Session
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

queries = [
    'クエリー1',
    'クエリー2',
    'クエリー3',
    'クエリー4'
]
secret_name = os.environ.get('SECRET_NAME')
level = os.environ.get('LOG_LEVEL', 'ERROR')


def logger_level():
    if level == 'CRITICAL':
        return 50
    elif level == 'ERROR':
        return 40
    elif level == 'WARNING':
        return 30
    elif level == 'INFO':
        return 20
    elif level == 'DEBUG':
        return 10
    else:
        return 0


logger = logging.getLogger()
logger.setLevel(logger_level())
ssm_client = boto3.client('ssm')


def get_secret():
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secret


def get_tweets(secret, since_id):
    try:
        tweets_array = []
        for query in queries:
            twitter = OAuth1Session(
                secret['api_key'],
                secret['api_secret_key'],
                secret['access_token'],
                secret['access_token_secret']
            )
            url = "https://api.twitter.com/1.1/search/tweets.json"
            params = {
                'q': query,
                'count': 100,
                'since_id': int(since_id)
            }

            response = twitter.get(
                url,
                params=params
            )
            logger.info(response.status_code)
            logger.debug(response.text)
            tweets = json.loads(response.text)
            tweets_array.append(tweets)

        return tweets_array

    except Exception as e:
        raise e


def get_since_id():
    try:
        return ssm_client.get_parameter(
            Name='twitter-since-id'
        )['Parameter']['Value']

    except Exception as e:
        raise e


def update_since_id(since_id):
    try:

        ssm_client.put_parameter(
            Name='twitter-since-id',
            Value=since_id,
            Overwrite=True
        )

    except Exception as e:
        raise e


def kinesis_put_records(tweets_array):
    try:
        records = []
        for tweets in tweets_array:
            for tweet in tweets['statuses']:
                payload = {
                    'id': tweet['id_str'],
                    'text': tweet['text'],
                    'source': tweet['source'],
                    'user_name': tweet['user']['name'],
                    'user_id': tweet['user']['id'],
                    'user_screen_name': tweet['user']['screen_name'],
                    'user_followers_count': tweet['user']['followers_count'],
                    'geo': tweet['geo']
                }
                logger.debug(payload)
                record = {
                    'Data': json.dumps(payload),
                    'PartitionKey': tweet['id_str']
                }
                records.append(record)

        logger.info(len(records))
        if len(records) > 0:
            kinesis_client = boto3.client('kinesis')
            response = kinesis_client.put_records(
                Records=records,
                StreamName='Tweet'
            )
            logger.debug(response)

    except Exception as e:
        raise e


def lambda_handler(event, context):
    logger.debug(event)

    try:
        secret = json.loads(get_secret())
        logger.debug(secret)

        since_id = get_since_id()
        logger.debug(since_id)

        tweets_array = get_tweets(secret, since_id)
        logger.debug(tweets_array)

        kinesis_put_records(tweets_array)

        if since_id != tweets_array[0]['search_metadata']['max_id_str']:
            update_since_id(
                tweets_array[0]['search_metadata']['max_id_str']
            )

        return {
            'statusCode': '200'
        }

    except Exception as e:
        logger.error(traceback.format_exc())
        raise e
