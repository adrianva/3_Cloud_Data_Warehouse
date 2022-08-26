# 3_Cloud_Data_Warehouse

This repository contains all the files for the Cloud Data Warehouse project of the Data Engineer Nanodegree Program by Udacity.

## Introduction
"A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results."

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the dependecies (it is recommended to create a virtual environment before doing that).

```bash
pip install -r requirements.txt
```

## Configuration

We need to create 3 environment variables in order to access to AWS:
```
export AWS_ACCESS_KEY=<access_key>
export AWS_SECRET_KEY=<secret_key>
export AWS_SESSION_TOKEN=<session_token>
```

All these 3 variables are provided to us when we launch the cloud gateway.

We also need to choose the database name, user and password, and update the **dwh.cfg** file.

```
[DB]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=5439
```

## Usage

Once we have everything ready, we need to create the Redshift cluster. There is a script that does this:
```
python create_cluster.py
```

This is script basically creates an IAM Role with permissions for reading S3, and then assign it to the cluster. It also allows to access the cluster from the outer world (although we should be a little bit more carefull in a production environment).

We will need to wait a little bit until the cluster is ready. If everything goes fine, this script should end with our cluster up and running. It is possible that we encounter some warnings telling us that some resources are already created (such as the IAM role) but this is fine and expected.

```
root@12c44dc81ffe:/home/workspace# python create_cluster.py 
Creating a new IAM Role
Role Created
arn:aws:iam::712737122592:role/dwhRole
Creating cluster dwhCluster... Please wait until it is available
Cluster status: creating. Retrying in 30 seconds
Cluster status: creating. Retrying in 30 seconds
Cluster status: creating. Retrying in 30 seconds
Cluster status: creating. Retrying in 30 seconds
Cluster created. Endpoint: {'Address': 'dwhcluster.c8w1fd3esnxj.us-west-2.redshift.amazonaws.com', 'Port': 5439}
ec2.SecurityGroup(id='sg-09bb048707049586d')
An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule "peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW" already exists

```

We need to write down both the IAM Role ARN and the cluster endpoint (arn:aws:iam::712737122592:role/dwhRole and dwhcluster.c8w1fd3esnxj.us-west-2.redshift.amazonaws.com in this case) and update the **dwh.cfg** file accordingly.

* `DB/HOST`
* `IAM_ROLE/ARN`

Now, we are ready to create the tables:
```
python create_tables.py
```

If everything goes well we shouldn't see any warning nor errors.

Finally, we are ready to run the etl.py script.

```
python etl.py
```

Again, this script should finish without warning nor errors. Now we are ready to query our Redshift cluster.


### Delete the cluster
Once we finish working with our cluster, we need to delete it

```
python create_cluster.py --delete
```

Output:
```
Deleting the Redshift cluster dwhCluster...
Cluster status: deleting. Retrying in 30 seconds
Cluster status: deleting. Retrying in 30 seconds
Cluster dwhCluster already deleted
Role deleted dwhRole
```

TThe script waits until the cluster is deleted and then it also deletes the role previously created. 
