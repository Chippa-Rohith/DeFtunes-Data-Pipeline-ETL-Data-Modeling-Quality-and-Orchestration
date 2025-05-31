import json
import logging
import os

import boto3
import traceback
import cfnresponse
import sqlalchemy as sa
import sqlparse
from botocore.exceptions import ClientError


DBHOST = os.getenv("DBHOST", "")
DBPORT = int(os.getenv("DBPORT", ""))
DBNAME = os.getenv("DBDATABASE", "postgres")
DBUSER = os.getenv("DBUSER", "")
DBPASSWORD = os.getenv("DBPASSWORD", "")
BUCKET_NAME = os.getenv("BUCKET_NAME", "")
OBJECT_KEY = os.getenv("OBJECT_KEY", "")
IAM_ROLE_ARN = os.getenv("IAM_ROLE_ARN", "")
BUCKET_PATH = os.getenv("BUCKET_PATH", "labs/cfn_dependencies/c4w4a1/csv")
ACCOUNT_ID = os.getenv("ACCOUNT_ID", "")
PROJECT = os.getenv("PROJECT", "de-c4w4a1")
REGION = os.getenv("REGION", "us-east-1")


CREATE = "Create"
DELETE = "Delete"
response_data = {}

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sts_client = boto3.client("sts")
glue_client = boto3.client("glue", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)
iam_client = boto3.client('iam', region_name=REGION)

glue_jobs = [f"{PROJECT}-api-users-extract-job",
             f"{PROJECT}-api-sessions-extract-job",
             f"{PROJECT}-rds-extract-job",
             f"{PROJECT}-json-transform-job",
             f"{PROJECT}-songs-transform-job"]

s3_buckets = [f"{PROJECT}-{ACCOUNT_ID}-{REGION}-scripts",
             f"{PROJECT}-{ACCOUNT_ID}-{REGION}-data-lake",
             f"{PROJECT}-{ACCOUNT_ID}-{REGION}-dags",
             f"{PROJECT}-{ACCOUNT_ID}-{REGION}-dbt"]

def sts_assume_role(role_arn):
    session_creds = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="CreatePostgresSession"
    )
    return session_creds['Credentials']


def empty_bucket(bucket_name: str):
    try:
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(bucket_name)
        bucket.object_versions.delete()
    except Exception as e:
        traceback_error = traceback.format_exc()
        logger.warning(f"Error emptying bucket:{bucket_name} {e} \n")


def delete_table(database_name: str, table_name: str):
    try:
        response = glue_client.delete_table(
            DatabaseName=database_name,
            Name=table_name
            )
        logger.info(f"Deleted table: {database_name}.{table_name}\n")
    except Exception as e:
        traceback_error = traceback.format_exc()
        logger.warning(f"Error details: . {traceback_error} {e} \n")
        logger.info(f"Couldn't delete table: {database_name}.{table_name}\n")


def delete_database(database_name: str):
    try:
        response_delete_database = glue_client.delete_database(
            Name=database_name
        )
        logger.info(f"Deleted database: {database_name}\n")
    except Exception as e:
        traceback_error = traceback.format_exc()
        logger.warning(f"Error details: . {traceback_error} {e} \n")
        logger.info(f"Couldn't delete database: {database_name}\n")


def terraform_cleanup():
    # Delete Glue DQ rulesets
    try:
        glue_rulesets = glue_client.list_data_quality_rulesets()
        glue_rulesets_list = glue_rulesets['Rulesets']
        for glue_ruleset in glue_rulesets_list:
            glue_ruleset_name = glue_ruleset['Name']
            response = glue_rulesets.delete_data_quality_ruleset(
                Name=glue_ruleset_name
            )
    except Exception as e:
        logger.info(f"Failed to delete glue rulesets. Exception: {e}")
    # Delete Glue jobs and databases
    for job_name in glue_jobs:
        try:
            glue_client.delete_job(JobName=job_name)
            logger.info(f"Deleted Glue job: {job_name}")
        except Exception as e:
            logger.info(
                f"Glue job: {job_name} does not exist. Exception: {e}")
    try:
        glue_databases = glue_client.get_databases()
        glue_database_list = glue_databases['DatabaseList']
        for glue_database in glue_database_list:
            glue_database_name = glue_database['Name']
            if 'default' == glue_database_name:
                continue
            glue_tables = glue_client.get_tables(DatabaseName=glue_database_name)
            glue_table_list = glue_tables['TableList']
            for table in glue_table_list:
                glue_table_name = table['Name']
                delete_table(glue_database_name, glue_table_name)
            delete_database(glue_database_name)
    except Exception as e:
        logger.info(
            f"Failed to delete glue databases. Exception: {e}")
    # Delete Glue connection
    try:
        connection_name = f'{PROJECT}-connection-rds'
        glue_client.delete_connection(ConnectionName=connection_name)
        logger.info(f"Deleted Glue connection: {connection_name}")
    except Exception as e:
        logger.info(
            f"Glue connection: {connection_name} does not exist. Exception: {e}")
    # Delete IAM Role and Policy
    iam_role = f"{PROJECT}-glue-role"
    iam_policy = f"{PROJECT}-glue-role-policy"
    try:
        # Delete policies
        try:
            iam_client.delete_role_policy(RoleName=iam_role,
                                          PolicyName=iam_policy)
            logger.info(
                f"Deleted policy: {iam_policy} from role: {iam_role}")
        except Exception as e:
            logger.info(
                f"Cannot delete policy {iam_policy} from {iam_role}. Exception: {e}")
        # Delete role
        try:
            iam_client.delete_role(RoleName=iam_role)
            logger.info(f"Deleted IAM role: {iam_role}")
        except Exception as e:
            logger.info(
                f"IAM role {iam_role} not found. Exception: {e}")
    except ClientError as e:
        logger.info(f"IAM role or policy do not exist. Exception: {e}")


format_load = """SELECT aws_s3.table_import_from_s3(
   'deftunes.songs', '', '(format csv, HEADER true)',
   aws_commons.create_s3_uri('{bucket_name}','{bucket_path}/songs.csv','us-east-1'),
   aws_commons.create_aws_credentials('{access_key}', '{secret_key}', '{session_token}')
);"""


def lambda_handler(event, context):
    logger.warning(f"Event: {event}")
    try:
        if event["RequestType"] == CREATE:
            terraform_cleanup()
            logger.info(f"SQLAlchemy version {sa.__version__}")
            # Reading file from S3
            s3 = boto3.client("s3")

            response = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
            logger.info(f"response:  {response}")
            sql_statement = response["Body"].read().decode("utf-8")
            sql_list = sqlparse.split(sql_statement)

            # Population of Database
            engine = sa.create_engine(
                f"postgresql+psycopg2://{DBUSER}:{DBPASSWORD}@{DBHOST}/{DBNAME}"
            )
            with engine.connect() as conn:
                for statement in sql_list:
                    db_response = conn.execute(
                        sa.text(statement).execution_options(autocommit=True)
                    )
                session_token = sts_assume_role(IAM_ROLE_ARN)
                statement = format_load.format(
                    bucket_name=BUCKET_NAME,
                    bucket_path=BUCKET_PATH,
                    access_key=session_token['AccessKeyId'],
                    secret_key=session_token['SecretAccessKey'],
                    session_token=session_token['SessionToken']
                )
                db_response = conn.execute(
                    sa.text(statement).execution_options(autocommit=True)
                )

        if event["RequestType"] == DELETE:
            # Delete elements in s3 buckets
            for s3_bucket in s3_buckets:
                empty_bucket(s3_bucket)
            curl_data = {
                "Status": "SUCCESS",
                "PhysicalResourceId": event["PhysicalResourceId"],
                "StackId": event["StackId"],
                "RequestId": event["RequestId"],
                "LogicalResourceId": event["LogicalResourceId"],
            }

            curl_command = [
                "curl",
                "-H",
                "Content-Type: application/json",
                "-X",
                "PUT",
                "-d",
                json.dumps(curl_data),
                event["ResponseURL"],
            ]
            logger.info(f"curl_command: {curl_command}")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
    except Exception as exc:
        logger.error(f"Error: {str(exc)}")
        cfnresponse.send(event, context, cfnresponse.FAILED, response_data)
