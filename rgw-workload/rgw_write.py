#!/usr/bin/env python3
# -*- coding: utf8 -*-

import argparse
import boto3
import kubernetes
import random
import string
import time
import base64
import uuid
import logging

from datetime import datetime
from kubernetes import client, config

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# Load the Kubernetes configuration
config.load_incluster_config()

# Create a Kubernetes API client
v1 = client.CoreV1Api()

# Set up the argument parser
parser = argparse.ArgumentParser(description='Generate a random object and write it to an MCG bucket every N seconds')
parser.add_argument('delay', type=int, help='Number of seconds to wait between writes')

# Parse the command-line arguments
args = parser.parse_args()

# Fetch the MCG endpoint URL, access key ID, and secret access key from Kubernetes secrets
mcg_endpoint_url = "https://ocs-storagecluster-cephobjectstore-secure-openshift-storage.apps.mashetty-s2.qe.rh-ocs.com/"

# Specify the name of the secret and the namespace
secret_name = "rook-ceph-object-user-ocs-storagecluster-cephobjectstore-my-user"
namespace = "openshift-storage"

# Fetch the secret
secret = v1.read_namespaced_secret(name=secret_name, namespace=namespace)

# Extract the value of the AWS_ACCESS_KEY_ID key and decode it from base64
aws_access_key_id = base64.b64decode(secret.data["AccessKey"]).decode("utf-8")
logging.info(aws_access_key_id)
aws_secret_access_key = base64.b64decode(secret.data["SecretKey"]).decode("utf-8")
logging.info(aws_secret_access_key)
# Set up the MCG client
s3 = boto3.client('s3',
                  endpoint_url=mcg_endpoint_url,
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  verify=False)

# Create a bucket
bucket_name = str(uuid.uuid4())
s3.create_bucket(Bucket=bucket_name)
logging.info(f"Bucket created: {bucket_name}")

# Write a random object to the bucket every N seconds
try:
    while True:
        # Generate a random object name
        object_name = "Random-object-"+str((datetime.now()).isoformat())

        # Generate a 10KB file with the same name as the object
        file_content = bytes(''.join(random.choices(string.ascii_uppercase + string.digits, k=10240)), 'utf-8')

        # Write the file to disk with the same name as the object
        file_name = object_name
        with open(file_name, 'wb') as f:
            f.write(file_content)

        # Put the object in the bucket with the same name as the file
        s3.put_object(Bucket=bucket_name,
                      Key=object_name,
                      Body=file_content)

        logging.info(f'Object {object_name} written to bucket {bucket_name} and saved to local file {file_name}')

        # Wait for the specified delay before writing the next object
        time.sleep(args.delay)

except KeyboardInterrupt:
    logging.info('\nInterrupted by user. Exiting...')
