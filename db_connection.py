import boto3
import psycopg2
import os
from logging import *

LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'a',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')

logger.info("-------------------------------------------------------Job Started---------------------------------------------------------------------")

#-----------------------------------------------------------------------------------------------------------------------
# DB connection
class DBConnection:
    def __init__(self): ## These values should be read from AWS Secret Manager - In Secret Manager Password and userid should be encrypted form.
        self.host = "xxxx-ap-south-1-db.cckebez2onwv.ap-south-1.rds.amazonaws.com"
        self.port = "5432"
        self.dbname = "webappdb"
        self.user = "postgres"
        self.password = "master123"

    def get_db_connection(self):
        try:
            db_conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user,password=self.password)
            logger.info("DB Connection Successful")
            db_conn.autocommit = True
            return db_conn
        except Exception as e:
            #print("Exception in DB connection", e)
            logger.critical("Exception in DB Connection")
            logger.error(e)

def get_db_conn():
    logger.info("Inside get_db_conn")
    postgres_db = DBConnection()
    db_conn = postgres_db.get_db_connection()

    return db_conn