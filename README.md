# icat-data-generator

Icat-data-generator is a tool for generating synthetic data for an iRODS
iCAT database. The main intended use case is to quickly generate iCAT database
contents for performance testing.

## System requirements

Icat-data-generator supports Postgres databases. It should work on any Linux
system with Python 3.6 or higher.

It has been tested with Postgres MD5 authentication.

## Installation

### CentOS 7

First install Postgres, Python 3 and development dependencies of psycopg2

```
sudo yum -y install postgresql-server postgresql-devel python3 python3-devel python-virtualenv gcc git
```

Next initialize Postgres:

```
sudo postgresql-setup initdb
```

Adapt your pg\_hba.conf file in /var/lib/pgsql/data so that MD5 authentication is available for local
connections. Example configuration lines for local connections:

```
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
```

Start Postgres:

```
sudo systemctl start postgresql
```

Set a password on the Postgres database account, or create another account
you would like to use for the data generator. Example:


```
sudo -iu postgres psql
psql (9.2.24)
Type "help" for help.

postgres=# \password postgres
Enter new password:
Enter it again:
postgres=# \q
```

Install the data generator in a virtualenv. Example:

```
git clone https://github.com/UtrechtUniversity/icat-data-generator.git
virtualenv --python /usr/bin/python3 venv
source venv/bin/activate
pip3 install --upgrade ./icat-data-generator
```

Now you can use the data generator to generate a test iCAT database, like
so (adjust the credentials in the -u and -p parameters so that they
match the Postgres account you will be using):

```
icat-data-gen --nc 20000 -d icat_test -u postgres -p postgres
```

### Ubuntu 20.04 LTS

First install PostgreSQL and development dependencies of psycopg2. These example
commands are based on PostgreSQL 11, but it should work with other PostgreSQL
versions as well.

```
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
sudo apt update
sudo apt install postgresql-11
sudo apt install python3-dev python3-virtualenv gcc
```

Start Postgres:

```
sudo pg_ctlcluster 11 main start
```

Set a password on the Postgres database account, or create another account
you would like to use for the data generator. Example:


```
sudo -iu postgres psql
psql (11.16 (Ubuntu 11.16-1.pgdg20.04+1))
Type "help" for help.

postgres=# \password postgres
Enter new password:
Enter it again:
postgres=# \q
```

Install the data generator in a virtualenv. Example:

```
git clone https://github.com/UtrechtUniversity/icat-data-generator.git
virtualenv --python /usr/bin/python3 venv
source venv/bin/activate
pip3 install --upgrade ./icat-data-generator
```

Now you can use the data generator to generate a test iCAT database, like
so (adjust the credentials in the -u and -p parameters so that they
match the Postgres account you will be using):

```
icat-data-gen --nc 20000 -d icat_test -u postgres -p postgres
```

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

The batch size and number arguments support abbreviated numbers,
e.g. "100k" for 100,000 and "10m" for 10,000,000.
