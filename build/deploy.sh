pwd

mkdir -p ../deploy

cd ../deploy/
rm *

cd ..

zip -r deploy/redis.zip *.py psycopg2 redis

aws s3 cp deploy/redis.zip s3://tmxwebdev/deploy/redis/redis.zip