from __future__ import print_function

import json
import logging
import psycopg2
import util
import aws
from datetime import datetime, timedelta
import time

print('Loading redshift')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load(config, date):
    logging.info("date: " + date)
    y, m, d = util.datepart(date)

    db_host = config('db_host')
    db_port = config('db_port')
    db_name = config('db_name')
    db_user = config('db_user')
    db_password = config('db_password')
    access_key = config('access_key')
    secret_key = config('secret_key')
    bucket = config('bucket')
    parsed_key = config('parsed_key')
    table = config('table')
    success_file = config('success_file')
    print(db_host, db_port, db_name, db_user, bucket, parsed_key, table)

    access_key = aws.decrypt(access_key)
    logger.info("access key descrypted")
    secret_key = aws.decrypt(secret_key)
    logger.info("secret key descrypted")
    db_password = aws.decrypt(db_password)
    logger.info("password descrypted")
    conn_string = "host='" + db_host + \
                  "' port='" + db_port + \
                  "' dbname='" + db_name + \
                  "' user='" + db_user + \
                  "' password='" + db_password + "'"
    logger.info("The connection string is: " + conn_string.replace(db_password, '*'))

    tries = 0;
    while True:
        tries += 1
        logger.info("Number of tries:" + str(tries))
        try:
            conn = psycopg2.connect(conn_string)
            logger.info("Connection successful.")
            break
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            if tries > 5:
                raise

    logger.info("Connection successful, execute sql command")

    cur = conn.cursor()

    # delete first
    delete_statement = config('delete_statement').format(table=table, year=y, month=m, day=d)
    logger.info(delete_statement)

    try:
        cur.execute(delete_statement)
        conn.commit()
        logger.info("delete completed successfully")
    except Exception as e:
        logger.error(e)
        raise

    s3_url = "s3://" + bucket + "/" + parsed_key.format(bucket=bucket, year=y, month=m, day=d)
    copy_statement = config('copy_statement')
    stmt = copy_statement.format(table=table, s3_url=s3_url, access_key=access_key, secret_key=secret_key)
    logger.info(stmt.replace(access_key, access_key[:3] + '*****' + access_key[-3:]).replace(secret_key, secret_key[:3] + '*****' + secret_key[-3:]))

    try:
        cur.execute(stmt)
        conn.commit()
        logger.info("copy completed successfully")
    except Exception as e:
        logger.error(e)
        raise

    # Close communication with the database
    conn.close()
    aws.write_success(bucket, success_file.format(year=y, month=m, day=d))
    logger.info("load completed successfully")
