import os,sys,string,argparse,requests,re,datetime
import fmis3.FMIS3 as fmis3
from os import listdir
import os, fnmatch
from os.path import isfile, join

class FileHandler:

    def __init__(self, verbose=False):
        self.verbose = verbose

    def set_verbose(self, value=True):
        self.verbose = value

    def list_files(self, directory, pattern):
        """
        List all files in the given directory matching given pattern (recursive)
        """
        matches = []
        for root, dirnames, filenames in os.walk(directory):
            for filename in fnmatch.filter(filenames, pattern):
                matches.append(os.path.join(root, filename))

        return matches

    def print_files(self, files):
        """ Print files to command line """
        print("Found following files:")
        for key in files:
            print('    -' + key + ': ')

    def clean_dir(self, dir, numbers_to_keep):
        """ Clean directory """
        existing_files = [f for f in listdir(dir) if isfile(join(dir, f))]
        while len(existing_files) > int(numbers_to_keep):
            f = existing_files.pop(0)
            os.remove(dir+'/'+f)

    def archive(self, s3_archive, numbers_to_keep):
        """ Archive S3 bucket to S3 archive bucket. """
        if self.verbose:
            print("Archiving S3 bucket...")

        fs3.archive(s3_archive, numbers_to_keep)


    def clean_s3(self, numbers_to_keep):
        """ Clean S3 bucket """

        if self.verbose:
            print("Cleaning S3 bucket...")

        fs3.clean_s3(numbers_to_keep)

    def set_fs3(self, fs3):
        """ Set S3 Information """
        self.fs3 = fs3
