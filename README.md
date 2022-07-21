# icat-data-generator

Icat-data-generator is a tool for generating synthetic data for an iRODS
iCAT database. The main intended use case is to quickly generate iCAT database
contents for performance testing.

Icat-data-generator supports Postgres databases. It should work on any Linux
system with Python 3.6 or higher.

It has been tested with Postgres MD5 authentication.

## Usage

```
usage: icat-data-gen [-h] [-v] [-d DB_NAME] [-u DB_USER] [-p DB_PASSWORD]
                     [--db-port DB_PORT] [--db-host DB_HOST] [-b BATCH_SIZE]
                     [--nc NC] [--nd ND] [--nm NM] [--nmm NMM] [--na NA]
                     [--nu NU] [--nr NR] [-s {8}]

Generates synthetic iCAT data for performance testing

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Verbose mode
  -d DB_NAME, --db-name DB_NAME
                        Database name
  -u DB_USER, --db-user DB_USER
                        Database user
  -p DB_PASSWORD, --db-password DB_PASSWORD
                        Database password
  --db-port DB_PORT     Database (TCP) port
  --db-host DB_HOST     Database host
  -b BATCH_SIZE, --batch-size BATCH_SIZE
                        Batch size for database inserts and reference queries
  --nc NC, --num-collections NC
                        Number of collections to create
  --nd ND, --num-dataobjects ND
                        Number of data objects to create
  --nm NM, --num-metadata NM
                        Number of metadata AVUs to create
  --nmm NMM, --num-metadata-map NMM
                        Number of metadata map entries to create
  --na NA, --num-access NA
                        Number of object access entries to create
  --nu NU, --num-users NU
                        Number of users to create
  --nr NR, --num-resc NR
                        Number of resources to create
  -s {8}, --schema {8}  Schema version
```

