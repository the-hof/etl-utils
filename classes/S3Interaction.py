import boto
import sys, os, errno
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class S3Interaction:
    def __init__(self, aws_access_key, aws_secret_key):
        self.conn = S3Connection(aws_access_key, aws_secret_key)

    def get_bucket(self, bucket_name):
        return self.conn.get_bucket(bucket_name)

    def save_file_locally(self, key, local_filename):
        if not os.path.exists(local_filename):
            try:
                os.makedirs(os.path.dirname(local_filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
            key.get_contents_to_filename(local_filename)

    def get_s3_key(self, bucket, key, create_key=False):
        s3_key = bucket.get_key(key)
        if (s3_key is None) and create_key:
            s3_key = bucket.new_key(key)
        return s3_key

    def put_file_to_s3(self, bucket, key, local_filename, s3_filename):
        object_key_name = "/".join((key, s3_filename))
        s3_key = self.get_s3_key(bucket, object_key_name, True)
        s3_key.set_contents_from_filename(local_filename)
