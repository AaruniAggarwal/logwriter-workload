#!/usr/bin/env python3
# -*- coding: utf8 -*-

import os
import hashlib
import argparse
import time
import boto3
import logging
import base64

from kubernetes import client, config


# Load the Kubernetes configuration
config.load_incluster_config()

# Create a Kubernetes API client
v1 = client.CoreV1Api()

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

# Fetch the MCG endpoint URL, access key ID, and secret access key from Kubernetes secrets
mcg_endpoint_url = "https://ocs-storagecluster-cephobjectstore-secure-openshift-storage.apps.mashetty-s2.qe.rh-ocs.com/"

# Specify the name of the secret and the namespace
secret_name = "rook-ceph-object-user-ocs-storagecluster-cephobjectstore-my-user"
namespace = "openshift-storage"

# Fetch the secret
secret = v1.read_namespaced_secret(name=secret_name, namespace=namespace)

# Extract the value of the AWS_ACCESS_KEY_ID key and decode it from base64
aws_access_key_id = base64.b64decode(secret.data["AccessKey"]).decode("utf-8")
aws_secret_access_key = base64.b64decode(secret.data["SecretKey"]).decode("utf-8")

# Set up the MCG client
s3 = boto3.resource('s3',
                  endpoint_url=mcg_endpoint_url,
                  aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  verify=False)

def verify_object(bucket, obj):
    # Check if the file with the same name exists locally
    if os.path.isfile(obj.key):
        # Compare the MD5 hash of the object with the local file
        obj_hash = obj.e_tag.strip('"')
        local_hash = hashlib.md5(open(obj.key, 'rb').read()).hexdigest()
        if obj_hash == local_hash:
            logging.info(f"{obj.key} is ok")
        else:
            logging.info(f"{obj.key} is corrupted [{obj_hash} != {local_hash}]")
    else:
        logging.info(f"Local file {obj.key} does not exist")

def list_objects_in_buckets():
    for bucket in s3.buckets.all():
        logging.info(f"Bucket: {bucket.name}")
        for obj in bucket.objects.all():
            verify_object(bucket, obj)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Verify objects in S3 buckets")
    parser.add_argument('-d', '--duration', type=int, help='Duration in minutes to verify objects')
    args = parser.parse_args()

    if not args.duration:
        parser.print_help()
        exit()

    end_time = time.time() + args.duration * 60
    while time.time() < end_time:
        list_objects_in_buckets()
        time.sleep(10)

    logging.info("Done verifying!")
