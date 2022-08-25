import os
import logging
import argparse
import configparser
import json
import time

import pandas as pd
import boto3
from botocore.exceptions import ClientError


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = os.environ["AWS_ACCESS_KEY"]
SECRET = os.environ["AWS_SECRET_KEY"]
SESSION_TOKEN = os.environ["AWS_SESSION_TOKEN"]

DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DB","DB_NAME")
DWH_DB_USER = config.get("DB","DB_USER")
DWH_DB_PASSWORD = config.get("DB","DB_PASSWORD")
DWH_PORT = config.get("DB","DB_PORT")
DWH_IAM_ROLE_NAME= config.get("DWH", "DWH_IAM_ROLE_NAME")


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()   # by default writes to STDERR when stream is None
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def create_resources():
    """
        Creates some common resources from AWS in order to be able to access them 
    """
    ec2 = boto3.resource(
        'ec2', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET,aws_session_token=SESSION_TOKEN)

    s3 = boto3.resource(
        's3', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET,aws_session_token=SESSION_TOKEN)

    iam = boto3.client(
        'iam',aws_access_key_id=KEY, aws_secret_access_key=SECRET, aws_session_token=SESSION_TOKEN, region_name='us-west-2')

    redshift = boto3.client(
        'redshift', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET,aws_session_token=SESSION_TOKEN)

    return ec2, s3, iam, redshift


def create_iam_role(iam) -> str:
    """
    Creates an IAM Role if does not exists
    """

    try:
        logger.info("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'
            })
        )
        logger.info("Role Created")
        status_code = iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']
        
        if status_code != 200:
            raise ValueError("Error trying to attach policy to the IAM Role")
    except ClientError as e:
        logger.warning(e)

    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logger.info(role_arn)
    return role_arn


def create_redshift_cluster(redshift, role_arn: str):
    """
    Creates a Redshift Cluster

    :param redshift: The redshift client object
    : role_arn: IAM Role used for S3 access
    """
    try:
        response = redshift.create_cluster(        
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            # Roles (for s3 access)
            IamRoles=[role_arn]  
        )
        logger.info(f"Creating cluster {DWH_CLUSTER_IDENTIFIER}... Please wait until it is available")
    except ClientError as e:
        logger.warning(e)


def wait_for_cluster_status(redshift, status: str) -> None:
    """
    Wait until the Redshift cluster reaches some status

    :param redshift: Redshift client object that allows us to manage the cluster
    :param status: The cluster status we need to wait for (e.g. "available")
    """
    cluster = None
    total_waiting_period = 900
    waiting_increment = 30

    try:
        for _ in range(int(total_waiting_period / waiting_increment)):
            cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            if cluster['ClusterStatus'] == status:
                break
            
            logger.info(f"Cluster status: {cluster['ClusterStatus']}. Retrying in {waiting_increment} seconds")
            time.sleep(waiting_increment)
    except redshift.exceptions.ClusterNotFoundFault:
        logger.warning(f"Cluster {DWH_CLUSTER_IDENTIFIER} already deleted")

    return cluster


def open_tcp_port(ec2, vpc_id: str) -> None:
    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        logger.info(default_sg)
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except ClientError as e:
        logger.warning(e)


def delete_redshift_cluster(redshift) -> None:
    """
    Delete the Redshift Cluster

    :params redshift: Redshift client object that allows us to manage the cluster
    """
    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        logger.info(f"Deleting the Redshift cluster {DWH_CLUSTER_IDENTIFIER}...")
    except ClientError as e:
        logger.error(e)


def delete_iam_role(iam) -> None:
    """
    Delete the IAM Role and dettach its policy from it

    :param iam: IAM client
    """
    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        logger.info(f"Role deleted {DWH_IAM_ROLE_NAME}")
    except ClientError as e:
        logger.error(e)


def main(args):
    ec2, s3, iam, redshift = create_resources()

    if args.delete:
        delete_redshift_cluster(redshift)
        _ = wait_for_cluster_status(redshift, status="deleted")
        delete_iam_role(iam)
    else:
        role_arn = create_iam_role(iam)

        create_redshift_cluster(redshift, role_arn)
        redshift_cluster = wait_for_cluster_status(redshift, status="available")

        if redshift_cluster:
            logger.info(f"Cluster created. Endpoint: {redshift_cluster['Endpoint']}")
            open_tcp_port(ec2, redshift_cluster['VpcId'])
        else:
            logger.error('Could not connect to cluster')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', dest='delete', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
