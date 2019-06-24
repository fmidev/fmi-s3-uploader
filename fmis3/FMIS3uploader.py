import os,sys, threading, boto3
from boto3.s3.transfer import TransferConfig

def upload_cb(complete, total):
    sys.stdout.write(".")
    sys.stdout.flush()

def _standard_transfer(bucket_name, s3_key_name, transfer_file, use_rr):
    print(" Upload with standard transfer, not multipart")
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, s3_key_name).put(Body=open(transfer_file, 'rb'))


def _multipart_upload(bucket_name, s3_key_name, src_file):
    """Upload large files using Amazon's multipart upload functionality.
    """
    print("Uploading in multipart...")

    session = boto3.Session()
    s3 = session.client('s3')

    try:
        tc = boto3.s3.transfer.TransferConfig()
        t = boto3.s3.transfer.S3Transfer(client=s3,
                                         config=tc )
        t.upload_file( src_file, bucket_name, s3_key_name)

    except Exception as e:
        print("Error uploading: {}".format(e))


def multi_part_upload_with_s3(src_file, bucket_name, dst_file):
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    s3 = boto3.resource('s3')
    cb = ProgressPercentage(src_file)
    s3.meta.client.upload_file(src_file, bucket_name, dst_file,
                               Config=config#,
                               #Callback=cb
                            )

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()
