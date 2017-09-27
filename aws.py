from __future__ import print_function

import boto3
import json
import logging

print('Loading aws functions')

s3 = boto3.client('s3')
db = boto3.client('dynamodb')
sns = boto3.client('sns')
kms = boto3.client('kms')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def send(topicArn, subject, message):
    response = sns.publish(
        TargetArn=topicArn,
        Subject=subject,
        Message=json.dumps(message)
    )
    logger.info("sns sent: " + message)


def read_function_config(table, function):
    config = db.get_item(TableName=table, Key={'function': {'S': function}})
    print(config)

    def value(name, type='S'):
        if name in config['Item']:
            return config['Item'][name][type]
        else:
            return None

    return value


def read_app_config(table, app):
    config = db.get_item(TableName=table, Key={'application': {'S': app}})
    print(config)

    def value(name, type='S'):
        if name in config['Item']:
            return config['Item'][name][type]
        else:
            return None

    return value


def get_s3_content(bucket, key):
    cs = s3.get_object(Bucket=bucket, Key=key)
    return cs['Body'].read()


def get_s3_object(bucket, key):
    cs = s3.get_object(Bucket=bucket, Key=key)
    return cs['Body']


def put_s3_object(bucket, key, bytes):
    response = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=bytes
    )
    return response


def list_s3_objects(bucket, prefix):
    resp = s3.list_objects(Bucket=bucket, Prefix=prefix)
    if 'Contents' in resp:
        return resp['Contents']
    else:
        return []


def encrypt(key, data):
    encrypted = kms.encrypt(KeyId=key, Plaintext=data)
    Encrpyted_Data = encrypted['CiphertextBlob']
    return Encrpyted_Data.encode('base64')


def decrypt(encrypted_base64):
    encrypted_data = encrypted_base64.decode('base64')
    decrypted = kms.decrypt(CiphertextBlob=encrypted_data)
    return decrypted['Plaintext']


def write_success(jobStatsBucket, successFile):
    print("creating", successFile)
    response = s3.put_object(
        Bucket=jobStatsBucket,
        ContentEncoding='string',
        ContentLength=0,
        ContentType='string',
        Key=successFile
    )
    print(successFile, "file created")


def write_failed(jobStatsBucket, failedFile, content):
    print("creating", failedFile)
    response = s3.put_object(
        Bucket=jobStatsBucket,
        Body=content,
        ContentEncoding='string',
        ContentLength=len(content),
        ContentType='string',
        Key=failedFile
    )
    print(failedFile, "file created")


def write_stats(jobStatsBucket, statsFile, content):
    content = ""
    bytes_of_content = content.encode('utf-8')

    response = s3.put_object(
        Bucket=jobStatsBucket,
        Body=bytes_of_content,
        ContentEncoding='string',
        ContentLength=len(bytes_of_content),
        ContentType='string',
        Key=statsFile
    )
    print(statsFile, "file created")


def object_exists(bucket, key):
    # print(bucket)
    # print(key)
    results = s3.list_objects(Bucket=bucket, Prefix=key)
    # print(str(results))
    return 'Contents' in results


def decrypt(encrypted):
    tmp = encrypted.decode('base64')
    decrypted = kms.decrypt(CiphertextBlob=tmp)
    return decrypted['Plaintext']

