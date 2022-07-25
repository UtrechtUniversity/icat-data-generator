import re
import sys
from argparse import ArgumentParser, ArgumentTypeError

from icat_data_generate import icat_schemas
from icat_data_generate.db import DataGenerator


def get_arguments():
    desc = 'Generates synthetic iCAT data for performance testing'
    parser = ArgumentParser(description=desc)
    parser.add_argument(
        '-v', '--verbose',
        action='store_const',
        const=True,
        help='Verbose mode')
    parser.add_argument(
        '-d', "--db-name",
        default='icat_test',
        help='Database name')
    parser.add_argument(
        '-u', "--db-user",
        default='postgres',
        help='Database user')
    parser.add_argument(
        '-p', "--db-password",
        default='postgres',
        help='Database password')
    parser.add_argument(
        "--db-port",
        default=5432,
        type=int,
        help='Database (TCP) port')
    parser.add_argument(
        "--db-host",
        default="localhost",
        help='Database host')
    parser.add_argument(
        '-b', "--batch-size",
        default=1000,
        help='Default batch size for database inserts and sample queries',
        type=arg_convenientnumber_type)
    parser.add_argument(
        "--nc", "--num-collections",
        default=20,
        help='Number of collections to create',
        type=arg_convenientnumber_type)
    parser.add_argument(
        '--nd', "--num-dataobjects",
        default=20,
        help='Number of data objects to create',
        type=arg_convenientnumber_type)
    parser.add_argument(
        '--nm', "--num-metadata",
        default=20,
        help='Number of metadata AVUs to create',
        type=arg_convenientnumber_type)
    parser.add_argument(
        '--nmm', "--num-metadata-map",
        default=20,
        help='Number of metadata map entries to create',
        type=arg_convenientnumber_type)
    parser.add_argument(
        '--na', "--num-access",
        default=20,
        help='Number of object access entries to create',
        type=arg_convenientnumber_type)
    parser.add_argument(
        '--nu', "--num-users",
        default=20,
        help='Number of users to create',
        type=arg_convenientnumber_type)
    parser.add_argument(
        '--nr', "--num-resc",
        default=20,
        help='Number of resources to create',
        type=arg_convenientnumber_type)
    parser.add_argument(
        '-s', "--schema",
        default=8,
        type=int,
        help='Schema version',
        choices=icat_schemas.get_available_schemas())
    args = parser.parse_args()
    return args


def arg_convenientnumber_type(arg):
    """Custom argparse type for numbers. It supports the following formats:
       - regular numbers, e.g. "123"
       - thousands, e.g. "456k" means: 456.000
       - millions, e.g. "789m" means: 789.000.000

       :param arg:  argument (string) to parse
       :returns:  an int (parsed argument)

       :raises ArgumentTypeError: if the argument can't be parsed.
    """
    if not re.match("^[0-9]+[km]?$", arg.lower()):
        message = "Argument {} is not a valid number. ".format(arg) + \
                  "Expecting 123/456k/789m."
        raise ArgumentTypeError(message)

    if arg.lower()[-1] == "k":
        return int(arg.lower()[:-1]) * 1000
    elif arg.lower()[-1] == "m":
        return int(arg.lower()[:-1]) * 1000000
    else:
        return int(arg)


def entry():
    try:
        main()
    except KeyboardInterrupt:
        print("Script interrupted by user.", file=sys.stderr)


def main():
    args = get_arguments()
    data = DataGenerator(args)
    if data.db_exists(args.db_name):
        print("Error: database already exists.")
        sys.exit(1)
    data.create_db()
    data.connect()
    data.initialize_schema(icat_schemas.get_schema(args.schema))
    data.add_seed_data()
    data.populate_data()
