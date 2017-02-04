# backup-pg-to-s3

Script to generate dump from all data of postgres and store it on a s3 bucket

## Install

Create a virtualenv

```
virtualenv env-name
```

Clone the repo

```
git clone git@github.com:diegorocha/backup-pg-to-s3.git
```

Install packages

```
pip install -r requirements.txt
```

## Configuration

You will need to create a .env file on set the following variables:
 * PGHOST: Host to connect to postgres
 * PGUSER: User name to connec to postgres
 * PGPASSWORD: Password to connect to postgres
 * BACKUP_DESTINATION: s3 bucket and path to store. Ex: s3://bucket-name/path/to/

## Usage

```
python backup.py
```

You can use a cron schedule to run the script automatically
