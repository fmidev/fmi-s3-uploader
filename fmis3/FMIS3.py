import os,sys,string,argparse,requests,re,datetime,math,glob,datetime,dateutil.parser
import boto3
import threading
import subprocess
import fmis3.FMIS3uploader as uploader
import copyreg, types
from filechunkio import FileChunkIO
from os import listdir
from os.path import isfile, join


class FMIS3Handler:
    def __init__(self, name):
        self.existing_entries = []
        self.bucket_name = name
        self.verbose = False
        self.bucket = None # <-- REMOVE
        self.s3 = None

    def set_verbose(self, value=True):
        self.verbose = value

    def _get_bucket(self):
        """ Return bucket """
        if not self.bucket:
            s3 = boto3.resource('s3')
            self.bucket = s3.Bucket(self.bucket_name)
            print(self.bucket)
        return self.bucket

    def get_existing_entries(self, force = False):
        """ Get existing entries """
        if not self.existing_entries or force:
            bucket = self._get_bucket()
            ex_list = []
            for e in bucket.objects.all():
                ex_list.append(str(e.key))
            self.existing_entries = ex_list

        return self.existing_entries

    def archive(self, s3_folder, s3_archive, numbers_to_keep, postfix):
        """
        Archive S3 bucket to S3 archive bucket.
        TODO: NOT coverted to work with python3 and boto3
        """
        if self.verbose:
            print("Archiving S3 bucket...")

        entries = self.get_existing_entries(True)
        files = []
        for e in entries:
            ## If s3_folder is not none and not recursive, drop all
            ## entries which don't start with the folder
            if s3_folder is not None and s3_folder is not 'recursive':
                if not e.startswith(s3_folder):
                    continue
            files.append(e)

        files.sort()

        if len(files) > int(numbers_to_keep):
            conn = boto.connect_s3()
            ab = conn.get_bucket(s3_archive)

        while len(files) > int(numbers_to_keep):
            filename = files.pop(0)

            if postfix is not None and not filename.endswith(postfix):
                continue

            path = self._get_folder(os.path.basename(filename))
            if path is None:
                continue

            filepath = path+"/"+os.path.basename(filename)

            if self.verbose:
                print("Archiving file " + filename + " from "+self.bucket_name+" to " + s3_archive + " with name " + filepath)

            self._move(self._get_bucket(), filename, ab, filepath)
            #ab.copy_key(filepath, self.bucket_name, filename)
            #self._get_bucket().delete_key(filename)

    def _move(self, source_bucket, source_key, dest_bucket, dest_key):
        """Move file from bucket to another. Note that source and dest buckets
        need to be connected

        """
        chunk_size = 209715200 # 200 Mb
        key = source_bucket.lookup(source_key)

        # Single part
        if key.size < chunk_size:
            if self.verbose:
                print("Copying file as single part...")
            dest_bucket.copy_key(dest_key, source_bucket, source_key)
        # Multipart
        else:
            if self.verbose:
                print("Copying file as multipart...")

            mp = dest_bucket.initiate_multipart_upload(dest_key)
            chunk_count = int(math.ceil(key.size / float(chunk_size)))
            self._print_progress(0, chunk_count)

            for i in range(chunk_count):
                start = chunk_size * i
                end = start + chunk_size
                if end > key.size:
                    end = key.size-1

                mp.copy_part_from_key(source_bucket.name, source_key, part_num=i + 1, start=start, end=end)
                self._print_progress(i+1, chunk_count)

            mp.complete_upload()

        source_bucket.delete_key(source_key)

    def _print_progress(self, iteration, total, prefix = '', suffix = '', decimals = 1, bar_length = 100):
        """
        Call in a loop to create terminal progress bar
        @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        """
        str_format = "{0:." + str(decimals) + "f}"
        percents = str_format.format(100 * (iteration / float(total)))
        filled_length = int(round(bar_length * iteration / float(total)))
        bar = '.' * filled_length + '-' * (bar_length - filled_length)

        sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

        if iteration == total:
            sys.stdout.write('\n')
        sys.stdout.flush()

    def _get_folder(self, file):

        """ Get folder for the file """
        try:
            stamp = self._get_stamp(file)
            folder = str(stamp.year)+"/"+stamp.strftime("%m")+"/"+stamp.strftime("%d")
        except:
            folder = None
        return folder

    def _get_stamp(self, file):
        """ Get time stamp from filename """
        regex = r'\d\d\d\d\d\d\d\dT\d\d\d\d\d\dZ'
        d = re.search(regex, file).group()
        stamp = dateutil.parser.parse(d)
        return stamp

    def clean_s3(self, numbers_to_keep):
        """ Clean S3 bucket """

        if self.verbose:
            print("Cleaning S3 bucket...")

    def remove_existing_files(self, files, s3_folder):
        """ Remove existing entries from given list """

        existing_entries = self.get_existing_entries()

        new_files = []
        for name in files:

            entry = self._s3_path_from_file(name, s3_folder)

            if entry not in existing_entries:
                new_files.append((name, entry))
            elif self.verbose:
                print(entry + ' already exists in the bucket')

        return new_files

    def handle_file(self, src_file, s3_folder):
        """ Upload file to S3 bucket if it not there already """

        if os.stat(src_file).st_size == 0:
            print('    File {} is empty. Skipping...'.format(src_file))
            return

        if self.verbose:
            print(" ..."+src_file)

        existing_entries = self.get_existing_entries()

        dst_file = self._s3_path_from_file(src_file, s3_folder)

        if dst_file not in existing_entries:
            self.put_file(src_file, dst_file)
        else:
            print('File exists in the bucket, not overwriting...')

    def put_file(self, src_file, dst_file):
        """ Upload file to S3 Bucket """

        file_size = os.stat(src_file).st_size

        if self.verbose:
            print("    Uploading file {} ({}) to {}".format(src_file, file_size, dst_file))

        file = open(src_file)

        if self.verbose:
            print("    Connecting to S3 and getting bucket " + self.bucket_name + "...")

        #bucket = self._get_bucket()
        mb_size = os.path.getsize(src_file) / 1e6

        if mb_size < 10:
            print('Uploading...')
            uploader._standard_transfer(self.bucket_name, dst_file, src_file, True)
        else:
            print('Uploading using multipart...')
            uploader.multi_part_upload_with_s3(src_file, self.bucket_name, dst_file)
            #uploader._multipart_upload(self.bucket_name, dst_file, src_file)


    def _s3_path_from_file(self, name, s3_folder=None):
        """ Generate S3 path from file """

        if s3_folder is not None and s3_folder == 'recursive':
            filename = os.path.basename(name)
            regex = r"(?<=_)\d{10}(?=_)"
            stamp = re.search(regex, filename).group(0)
            entry = '{}/{}/{}/{}'.format(stamp[0:4], stamp[4:6], stamp[6:8], filename)
        elif s3_folder is not None:
            entry = s3_folder+"/"+name
        else:
            entry = name

        return entry
#
# def _pickle_method(method):
#     func_name = method.im_func.__name__
#     obj = method.im_self
#     cls = method.im_class
#     return _unpickle_method, (func_name, obj, cls)
#
# def _unpickle_method(func_name, obj, cls):
#     for cls in cls.mro():
#         try:
#             func = cls.__dict__[func_name]
#         except KeyError:
#             pass
#         else:
#             break
#     return func.__get__(obj, cls)
#
# copyreg.pickle(types.MethodType, _pickle_method, _unpickle_method)
