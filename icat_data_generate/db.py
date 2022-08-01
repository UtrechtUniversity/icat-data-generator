import itertools
import random

from icat_data_generate.iterators.accessmap_iterator import AccessMapIterator
from icat_data_generate.iterators.coll_iterator import CollectionIterator
from icat_data_generate.iterators.dataobject_iterator import DataObjectIterator
from icat_data_generate.iterators.metadata_iterator import MetadataIterator
from icat_data_generate.iterators.metadatamap_iterator import MetadataMapIterator
from icat_data_generate.iterators.resc_iterator import RescIterator
from icat_data_generate.iterators.user_iterator import UserIterator
from icat_data_generate.msp import compute_msp

import psycopg2
from psycopg2 import extras


class DataGenerator:

    def __init__(self, args):
        self.args = args
        self.pg_conn = self._get_connection(True)
        self.user_iterator = None
        self.resc_iterator = None
        self.coll_iterator = None
        self.data_iterator = None
        self.meta_iterator = None
        self.metamap_iterator = None
        self.acl_iterator = None

    def _get_connection(self, initial_connection):
        a = self.args
        dbname = "postgres" if initial_connection else a.db_name
        conn = psycopg2.connect(f"user='{a.db_user}' host='{a.db_host}' " +
                                f"password='{a.db_password}' " +
                                f"port='{str(a.db_port)}'"
                                f"dbname='{dbname}'")
        conn.autocommit = True
        return conn

    def _db_supports_tablesample(self, conn):
        # PostgreSQL 10.x and up support TABLESAMPLE
        return conn.server_version > 100000

    def connect(self):
        self.conn = self._get_connection(False)

    def db_exists(self, dbname):
        """Checks whether a Postgres database exists

           :param dbname: Name of database to look for
           :returns: boolean value - whether database exists
        """
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database;")
            dblist = cur.fetchall()
        return dbname in dblist

    def create_db(self):
        """Creates a new database."""
        with self.pg_conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {self.args.db_name}")

    def initialize_schema(self, schema):
        with self.conn.cursor() as cur:
            cur.execute(schema)

    def populate_data(self):
        # Initialize tsm_system_rows if needed
        if self._db_supports_tablesample(self.conn):
            with self.conn.cursor() as cur:
                if self.args.verbose:
                    print("Installing tsm_system_rows extension ...")
                cur.execute("CREATE EXTENSION tsm_system_rows;")
        # Users
        user_query = "INSERT INTO r_user_main (user_id, user_name, user_type_name, zone_name) VALUES %s"
        user_template = "(NEXTVAL('R_ObjectID'), %s, %s, %s)"
        self.insert_table_data(user_query,
                               user_template,
                               "r_user_main",
                               self.args.nu,
                               self.gen_data_user)
        # Resources
        resc_query = "INSERT INTO r_resc_main (resc_id, resc_name, zone_name, resc_type_name, resc_class_name, resc_net, resc_def_path) VALUES %s"
        resc_template = "(NEXTVAL('R_ObjectID'), %s, %s, %s, %s, %s, %s)"
        self.insert_table_data(resc_query,
                               resc_template,
                               "r_resc_main",
                               self.args.nr,
                               self.gen_data_resc)
        # Collections
        coll_query = "INSERT INTO r_coll_main (coll_id, parent_coll_name, coll_name, coll_owner_name, coll_owner_zone, create_ts, modify_ts) VALUES %s"
        coll_template = "(NEXTVAL('R_ObjectID'), %s, %s, %s, %s, %s, %s)"
        self.insert_table_data(coll_query,
                               coll_template,
                               "r_coll_main",
                               self.args.nc,
                               self.gen_data_coll)
        # Data objects
        do_query = "INSERT INTO r_data_main (data_id, coll_id, data_name, data_repl_num, data_type_name, data_size, resc_name, data_path, data_owner_name, data_owner_zone, create_ts, modify_ts) VALUES %s"
        do_template = "(NEXTVAL('R_ObjectID'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.insert_table_data(do_query,
                               do_template,
                               "r_data_main",
                               self.args.nd,
                               self.gen_data_do)
        # Metadata AVUs
        meta_query = "INSERT INTO r_meta_main (meta_id, meta_attr_name, meta_attr_value, meta_attr_unit) VALUES %s"
        meta_template = "(NEXTVAL('R_ObjectID'), %s, %s, %s)"
        self.insert_table_data(meta_query,
                               meta_template,
                               "r_meta_main",
                               self.args.nm,
                               self.gen_data_metadata)
        # Metadata map
        metamap_query = "INSERT INTO r_objt_metamap (object_id, meta_id) VALUES %s"
        metamap_template = "(%s, %s)"
        self.insert_table_data(metamap_query,
                               metamap_template,
                               "r_objt_metamap",
                               self.args.nmm,
                               self.gen_data_metamap)
        # Object access map
        access_query = "INSERT INTO r_objt_access (object_id, user_id, access_type_id) VALUES %s"
        access_template = "(%s, %s, %s)"
        self.insert_table_data(access_query,
                               access_template,
                               "r_objt_access",
                               self.args.na,
                               self.gen_data_access)

    def insert_table_data(self, query, query_template, table, number, function):
        retries_left = number
        cur = self.conn.cursor()
        num_left = number
        batch_size = self.args.batch_size
        # We process data in batches here in order to be able to query random
        # referential external keys in the iterator functions in an efficient way.
        while num_left > 0:
            cur_batch_size = min([batch_size, num_left])
            if self.args.verbose:
                print(f"Processing batch of {str(cur_batch_size)} ... for table {table}.")
            with self.conn.cursor() as cur:
                try:
                    extras.execute_values(cur, query, function(cur_batch_size), template=query_template)
                except psycopg2.errors.UniqueViolation:
                    if retries_left > 0:
                        retries_left -= 1
                    else:
                        print(f"Out of uniqueness violation retries for table {table}")
                        exit(1)
                # We recalculate the number of records left to add based on row count after what would
                # be the last insert, or the one before that, so that we can adjust for inserts that
                # failed (if any).
                if num_left > cur_batch_size:
                    num_left -= cur_batch_size
                else:
                    num_left = max(0, number-self._count_rows(table))

    def add_seed_data(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO r_zone_main (zone_id, zone_name, zone_type_name, create_ts, modify_ts) " +
                "VALUES (9000, 'testZone','local', '1170000000', '1170000000')")
            cur.execute(
                "INSERT INTO r_coll_main (coll_id, coll_name, parent_coll_name, coll_owner_name, coll_owner_zone, create_ts, modify_ts) " +
                "VALUES (9020, '/', '/', 'rods', 'testZone', '1170000000', '1170000000')")
            cur.execute(
                "INSERT INTO r_user_main (user_id, user_name, user_type_name, zone_name, create_ts, modify_ts) " +
                "VALUES (9010, 'rods', 'rodsadmin', 'testZone', '1170000000', '1170000000')")

    def gen_data_user(self, number):
        if self.user_iterator is None:
            self.user_iterator = UserIterator(number, "testZone")
        else:
            self.user_iterator.extend(number)
        return self.user_iterator

    def gen_data_resc(self, number):
        if self.resc_iterator is None:
            self.resc_iterator = RescIterator(number, "testZone")
        else:
            self.resc_iterator.extend(number)
        return self.resc_iterator

    def gen_data_metadata(self, number):
        if self.meta_iterator is None:
            self.meta_iterator = MetadataIterator(number)
        else:
            self.meta_iterator.extend(number)
        return self.meta_iterator

    def gen_data_metamap(self, number):
        if self.metamap_iterator is None:
            num_meta = self.args.nm
            num_object = self.args.nc + self.args.nd
            (num_subset_meta, num_subset_object) = compute_msp(num_meta, num_object, self.args.nmm)
            subset_meta = self._get_id_subset("r_meta_main", "meta_id", num_subset_meta)
            subset_object = self._get_object_id_subset(num_subset_object)
            self.metamap_iterator = MetadataMapIterator(number, subset_object, subset_meta)
        else:
            self.metamap_iterator.extend(number)

        return self.metamap_iterator

    def gen_data_access(self, number):
        if self.acl_iterator is None:
            num_object = self.args.nc + self.args.nd
            num_user = self.args.nu
            (num_subset_user, num_subset_object) = compute_msp(num_user, num_object, self.args.na)
            subset_user = self._get_id_subset("r_user_main", "user_id", num_subset_user)
            subset_object = self._get_object_id_subset(num_subset_object)
            self.acl_iterator = AccessMapIterator(number, subset_object, subset_user)
        else:
            self.acl_iterator.extend(number)

        return self.acl_iterator

    def gen_objectid_sample(self, number):
        """Returns sample of data object and collection IDs. Should only be invoked
           once collection and data object tables have been initialized.

           :param number: size of sample
           :returns: list of collection / data object IDs
        """
        if number % 2 == 0:
            number_coll_meta = int(number / 2)
            number_do_meta = int(number / 2)
        else:
            # Add an extra element so that we can zip the coll / DO Ids
            number_coll_meta = int(number / 2) + 1
            number_do_meta = int(number / 2) + 1
        coll_id_sample = self._get_random_sample("r_coll_main",
                                                 ["coll_id"],
                                                 self.args.nc,
                                                 number_coll_meta)
        do_id_sample = self._get_random_sample("r_data_main",
                                               ["data_id"],
                                               self.args.nd,
                                               number_do_meta)
        return itertools.chain.from_iterable(zip(coll_id_sample, do_id_sample))

    def gen_data_coll(self, number):
        coll_name_sample = self._get_random_sample("r_coll_main",
                                                   ["coll_name"],
                                                   self._count_rows("r_coll_main"),
                                                   number)
        owner_sample = self._get_random_sample("r_user_main",
                                               ["user_name"],
                                               self.args.nu,
                                               number)
        if self.coll_iterator is None:
            self.coll_iterator = CollectionIterator(number,
                                                    coll_name_sample,
                                                    owner_sample)
        else:
            self.coll_iterator.extend(number, coll_name_sample, owner_sample)
        return self.coll_iterator

    def gen_data_do(self, number):
        coll_sample = self._get_random_sample("r_coll_main",
                                              ["coll_id", "coll_name"],
                                              self.args.nc,
                                              number)
        resc_sample = self._get_random_sample("r_resc_main",
                                              ["resc_name"],
                                              self.args.nr,
                                              number)
        user_sample = self._get_random_sample("r_user_main",
                                              ["user_name"],
                                              self.args.nu,
                                              number)
        if self.data_iterator is None:
            self.data_iterator = DataObjectIterator(number,
                                                    coll_sample,
                                                    resc_sample,
                                                    user_sample,
                                                    "testZone")
        else:
            self.data_iterator.extend(number, coll_sample, resc_sample, user_sample)
        return self.data_iterator

    def _count_rows(self, table):
        query = f"SELECT COUNT(*) FROM {table}"
        with self.conn.cursor() as cur:
            cur.execute(query)
            count = cur.fetchall()
            return count[0][0]

    def _get_object_id_subset(self, num_subset):
        num_do = self.args.nd
        num_co = self.args.nc
        if num_do + num_co < num_subset:
            raise ValueError("Too few data objects / collection for subset.")
        num_do_subset = min(num_do, num_subset)
        num_co_subset = num_subset - num_do
        subset = self._get_id_subset("r_data_main", "data_id", num_do_subset)
        if num_co_subset > 0:
            subset.extend(self._get_id_subset("r_coll_main", "coll_id", num_co_subset))
        return subset

    def _get_id_subset(self, table, column, num_subset):
        query = f"SELECT {column} FROM {table} ORDER BY {column} LIMIT {str(num_subset)}"
        with self.conn.cursor() as cur:
            cur.execute(query)
            return list(cur.fetchall())

    def _get_random_sample(self, table, columns, num_rows, num_sample):
        """ Gets a list of samples from a particular column of a
            particular table. Samples aren't necessarily unique.

            :param table: name of database table
            :param columns: list of database columns to return
            :param num_rows: number of rows in the table (so that we don't have to do
                             table count for every sample). This parameter is only used
                             on PostgreSQL versions that don't have tsm_system_rows.
            :param num_sample: number of samples.
            :returns: random list of result tuples
            :raises Exception: if unable to retrieve sample
        """
        column_list = str.join(",", columns)
        if self._db_supports_tablesample(self.conn):
            with self.conn.cursor() as cur:
                query = f"SELECT {column_list} FROM {table} TABLESAMPLE SYSTEM_ROWS ({str(num_sample)});"
                cur.execute(query)
                sample = cur.fetchall()
            return iter(list(sample))
        else:
            # Set random probability to select a row a bit higher than proportional
            # in order to reduce the chance we end up with fewer rows than
            # num_sample. Loop until we have selected at least 1 sample.
            sample_source_size = 0
            max_sample_retries = 20
            sample_retry = 0
            while sample_source_size == 0:
                if sample_retry > max_sample_retries:
                    raise Exception("Too many retries when getting random sample")

                query_factor = 1.5
                query_prob = min([1.0, (num_sample / num_rows) * query_factor])

                query = f"SELECT {column_list} FROM {table} WHERE RANDOM() < {str(query_prob)}"

                with self.conn.cursor() as cur:
                    cur.execute(query)
                    sample = cur.fetchall()
                    sample_source_size = len(sample)

                sample_retry += 1

            return iter(random.choices(sample, k=num_sample))
