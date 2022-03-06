import boto3
import psycopg2
import redshift_connector
import base64
from botocore.exceptions import ClientError
import os
from logging import *

LOG_FORMAT = '{lineno}  : {name}: {asctime}: {message}'
basicConfig(filename='logfile.log',level=DEBUG, filemode = 'a',style='{',format=LOG_FORMAT)
logger = getLogger('SFHTC')

logger.info("-------------------------------------------------------Job Started---------------------------------------------------------------------")

#-----------------------------------------------------------------------------------------------------------------------
# DB connection

def get_secret():

    secret_name = "arn:aws:secretsmanager:ap-south-1:143580737085:secret:migrationsecret-yJlTOZ"
    region_name = "ap-south-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            print(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            print(decoded_binary_secret)

class DBConnection:
    def __init__(self): ## These values should be read from AWS Secret Manager - In Secret Manager Password and userid should be encrypted form.
        self.host = "redshift-cluster-1.c04kzwicvscs.ap-south-1.redshift.amazonaws.com"
        self.port = "5439"
        self.dbname = "dev"
        self.user = "awsuser"
        self.password = "MNTHfyget1-."

    def get_db_connection(self):
        try:
            # db_conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user,password=self.password)
            db_conn = redshift_connector.connect(
                        host='redshift-cluster-1.c04kzwicvscs.ap-south-1.redshift.amazonaws.com',
                        database='dev',
                        user='awsuser',
                        password='MNTHfyget1-.'
            )
            logger.info("DB Connection Successful")
            print("Db connection established")
            db_conn.autocommit = True
            return db_conn
        except Exception as e:
            #print("Exception in DB connection", e)
            logger.critical("Exception in DB Connection")
            print(e)
            # print("Db connection failed")
            logger.error(e)

def get_db_conn():
    logger.info("Inside get_db_conn")
    postgres_db = DBConnection()
    db_conn = postgres_db.get_db_connection()
    return db_conn


#-----------------------------------------------------------------------------------------------------------------------
# S3 connectivity

if __name__ == '__main__':
    try:
        logger.info("-----AWS S3 Connectivity Intiated-----")
        logger.info("Setting Up S3 client")
        s3_client = boto3.client("s3", region_name='ap-south-1', aws_access_key_id='AKIASC3QUFY6SICETLNP', aws_secret_access_key='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')
        logger.info("Setting Up Os.environ")
        os.environ['aws_access_key_id'] = 'AKIASC3QUFY6SICETLNP'
        os.environ['aws_secret_access_key'] = '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'
        s3 = boto3.resource('s3')

        def upload_log(bucket_name):
            logger.info("Inside upload_log function")
            folder = 'logfile/' + 'logfile.log'
            try:
                s3_client.upload_file('logfile.log', bucket_name, folder)
                logger.info("Log File Uploaded Successfully!!.")
            except Exception as e:
                logger.error("LogFile Uploaded Failed!!.")
                logger.error(e)
        
        s3_bucket = []
        # appending bucket names
        try:
            logger.info('checking for all the buckets')
            for bucket in s3.buckets.all():
                s3_bucket.append(bucket.name)
                logger.info('Appended all the buckets to s3_bucket list')
            #print(s3_bucket)
        except Exception as e:
            logger.error("Unable to fetch S3 bucket")
            logger.error(e)

        try:
            if 'index-bucket-sfs' and 'parquet-bucket-sfs' in s3_bucket:
                logger.info("index-bucket and parquet-bucket found in the s3_bucket list")
                # index_bucket = s3.Bucket('index-bucket-sfs')
                # parquet_bucket1 = s3.Bucket('parquet-bucket-sfs')
                #code for accessing tablename in bucket goes here.
                #copy_command to copy to db, can be modified according to use.
                copy_command = ("COPY table_1 FROM 's3://parquet-bucket-sfs/table_1/userdata8.parquet' IAM_ROLE 'arn:aws:iam::143580737085:role/migrationrole' FORMAT AS PARQUET;")
                print("Db connection started")
                # logger.log("Creating Database Connection")
                con = get_db_conn()
                cur = con.cursor()
                print("Db connection established")
                # logger.log("Copying started for parquet file to redshift")
                print("copy started")
                cur.execute(copy_command)
                con.commit()
                # logger.log("Copying completed to redshift for file")
            else:
                logger.error("Index File Not Present-----!!")

        except Exception as e:
            logger.error("Something went wrong while processing index and parquet bucket----->")

    except Exception as e:
        logger.critical("Main Execution Stopped----->")

    finally:
        # upload_log('testbucketsuraj')
        get_secret()
        logger.info("Job Executed------------------------------------------------------------------------------------------------------------------")