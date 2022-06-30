# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose
Show how to use AWS SDK for Python (Boto3) with Amazon Simple Storage Service
(Amazon S3) to perform basic object operations
"""

import json
import logging
import random
import uuid

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class ObjectWrapper:
    def __init__(self, s3_object):
        self.object = s3_object

    def put(self, data):
        """
        Upload data to the object.

        :param data: The data to upload. This can either be bytes or a string. When this
                     argument is a string, it is interpreted as a file name, which is
                     opened in read bytes mode.
        """
        put_data = data
        if isinstance(data, str):
            try:
                put_data = open(data, 'rb')
            except IOError:
                logger.exception("Expected file name or binary data, got '%s'.", data)
                raise

        try:
            self.object.put(Body=put_data)
            self.object.wait_until_exists()
            logger.info(
                "Put object '%s' to bucket '%s'.", self.object.key,
                self.object.bucket_name)
        except ClientError:
            logger.exception(
                "Couldn't put object '%s' to bucket '%s'.", self.object.key,
                self.object.bucket_name)
            raise
        finally:
            if getattr(put_data, 'close', None):
                put_data.close()

    def get(self):
        """
        Gets the object.

        :return: The object data in bytes.
        """
        try:
            body = self.object.get()['Body'].read()
            logger.info(
                "Got object '%s' from bucket '%s'.",
                self.object.key, self.object.bucket_name)
        except ClientError:
            logger.exception(
                "Couldn't get object '%s' from bucket '%s'.",
                self.object.key, self.object.bucket_name)
            raise
        else:
            return body

    @staticmethod
    def list(bucket, prefix=None):
        """
        Lists the objects in a bucket, optionally filtered by a prefix.

        :param bucket: The bucket to query.
        :param prefix: When specified, only objects that start with this prefix are listed.
        :return: The list of objects.
        """
        try:
            if not prefix:
                objects = list(bucket.objects.all())
            else:
                objects = list(bucket.objects.filter(Prefix=prefix))
            logger.info("Got objects %s from bucket '%s'",
                        [o.key for o in objects], bucket.name)
        except ClientError:
            logger.exception("Couldn't get objects for bucket '%s'.", bucket.name)
            raise
        else:
            return objects


    def delete(self):
        """
        Deletes the object.
        """
        try:
            self.object.delete()
            self.object.wait_until_not_exists()
            logger.info(
                "Deleted object '%s' from bucket '%s'.",
                self.object.key, self.object.bucket_name)
        except ClientError:
            logger.exception(
                "Couldn't delete object '%s' from bucket '%s'.",
                self.object.key, self.object.bucket_name)
            raise

    @staticmethod
    def delete_objects(bucket, object_keys):
        """
        Removes a list of objects from a bucket.
        This operation is done as a batch in a single request.

        :param bucket: The bucket that contains the objects.
        :param object_keys: The list of keys that identify the objects to remove.
        :return: The response that contains data about which objects were deleted
                 and any that could not be deleted.
        """
        try:
            response = bucket.delete_objects(Delete={
                'Objects': [{
                    'Key': key
                } for key in object_keys]
            })
            if 'Deleted' in response:
                logger.info(
                    "Deleted objects '%s' from bucket '%s'.",
                    [del_obj['Key'] for del_obj in response['Deleted']], bucket.name)
            if 'Errors' in response:
                logger.warning(
                    "Could not delete objects '%s' from bucket '%s'.", [
                        f"{del_obj['Key']}: {del_obj['Code']}"
                        for del_obj in response['Errors']],
                    bucket.name)
        except ClientError:
            logger.exception("Couldn't delete any objects from bucket %s.", bucket.name)
            raise
        else:
            return response


    @staticmethod
    def empty_bucket(bucket):
        """
        Remove all objects from a bucket.

        :param bucket: The bucket to empty.
        """
        try:
            bucket.objects.delete()
            logger.info("Emptied bucket '%s'.", bucket.name)
        except ClientError:
            logger.exception("Couldn't empty bucket '%s'.", bucket.name)
            raise


def usage_demo():
    print('-'*88)
    print("Welcome to the Amazon S3 object demo!")
    print('-'*88)

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('mindfire-s3-demo')

    object = bucket.Object('demo-object')
    obj_wrapper = ObjectWrapper(object)
    
    # obj_wrapper.put(__file__)
    # print(f"Put file object with key {object.key} in bucket {bucket.name}.")

    # listed_lines = obj_wrapper.list(bucket)
    # print(f"Their keys are: {', '.join(l.key for l in listed_lines)}")

    # object.delete()
    # print(f"Deleted object with key {object.key}.")

    obj_wrapper.empty_bucket(bucket)
    print(f"Emptied bucket {bucket.name}.")

    print("Thanks for watching!")
    print('-'*88)


if __name__ == '__main__':
    usage_demo()
