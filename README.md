Scripts to upload files from file system to AWS S3.

Required environment can be seen from Dockerfile. Required proxies and AWS keys has to be set in environment following:
```
https_proxy=
http_proxy=
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
```

Usage:
```
python -u uploader --dir test-data --file_suffix *.nc --s3 fmi-opendata-silam-surface-netcdf --s3_folder recursive --verbose 1

python -u uploader -h
```

When recursive is used for s3_folder, date is searched from the file. The stamp is assumed to be in file name in format yyyymmddhh (10 digits). One may change this behaviour from file fmis3/FMIS3.py funcion `_s3_path_from_file`.
