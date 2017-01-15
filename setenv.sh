#Set initial configuration
heroku config:set CHUNK_SIZE=20000 \
    LOG_QUEUE_NAME=copydb-logq \
    QUEUE_NAME=copydb-q \
    SCHEDULE_CRON="0 0/10 * * * ?" \
    SOURCE_VAR=JDBC_DATABASE_URLL \
    TARGET_VAR=HEROKU_POSTGRESQL_YELLOW_JDBC_URL \
    TRANSFER_FROM_SCHEMA=public \
    TRANSFER_TO_SCHEMA=public \
 -a dell-transfer 
 
 #recreate schema
 heroku pg:psql --app dell-transfer < src/main/resources/db.sql 
 