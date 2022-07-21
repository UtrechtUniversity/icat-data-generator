import unittest

from icat_data_generate.iterators.accessmap_iterator import AccessMapIterator
from icat_data_generate.iterators.coll_iterator import CollectionIterator
from icat_data_generate.iterators.dataobject_iterator import DataObjectIterator
from icat_data_generate.iterators.irods_iterator import IrodsIterator
from icat_data_generate.iterators.metadata_iterator import MetadataIterator
from icat_data_generate.iterators.metadatamap_iterator import MetadataMapIterator
from icat_data_generate.iterators.resc_iterator import RescIterator
from icat_data_generate.iterators.user_iterator import UserIterator


class TestIterators(unittest.TestCase):
    def test_irods_iterator(self):
        length = 2
        extend_length = 3
        i = IrodsIterator(length)
        self.assertEqual(len(list(i)), length)
        i.extend(extend_length)
        self.assertEqual(len(list(i)), extend_length)

    def test_resc_iterator(self):
        length = 2
        extend_length = 3
        i = RescIterator(length, "testZone")
        self.assertEqual(len(list(i)), length)
        i.extend(extend_length)
        self.assertEqual(len(list(i)), extend_length)

    def test_user_iterator(self):
        length = 2
        extend_length = 3
        i = UserIterator(length, "testZone")
        self.assertEqual(len(list(i)), length)
        i.extend(extend_length)
        self.assertEqual(len(list(i)), extend_length)

    def test_metadata_iterator(self):
        length = 2
        extend_length = 3
        i = MetadataIterator(length)
        self.assertEqual(len(list(i)), length)
        i.extend(extend_length)
        self.assertEqual(len(list(i)), extend_length)

    def test_metadatamap_iterator(self):
        length = 2
        extend_length = 3
        oid_sample = iter([[1], [2]])
        oid_sample_extend = iter([[3], [4], [5]])
        mid_sample = iter([[1], [2]])
        mid_sample_extend = iter([[3], [4], [5]])
        i = MetadataMapIterator(length, oid_sample, mid_sample)
        self.assertEqual(len(list(i)), length)
        i.extend(extend_length, oid_sample_extend, mid_sample_extend)
        self.assertEqual(len(list(i)), extend_length)

    def test_accessmap_iterator(self):
        length = 2
        extend_length = 3
        oid_sample = iter([[1], [2]])
        oid_sample_extend = iter([[3], [4], [5]])
        uid_sample = iter([[1], [2]])
        uid_sample_extend = iter([[3], [4], [5]])
        i = AccessMapIterator(length, oid_sample, uid_sample)
        self.assertEqual(len(list(i)), length)
        i.extend(extend_length, oid_sample_extend, uid_sample_extend)
        self.assertEqual(len(list(i)), extend_length)

    def test_coll_iterator(self):
        length = 2
        extend_length = 3
        owner_sample = iter(["user1", "user2"])
        owner_sample_extend = iter(["user3", "user4", "user5"])
        coll_sample = iter(["/c1", "/c2"])
        coll_sample_extend = iter(["/c3", "/c4", "/c5"])
        i = CollectionIterator(length, coll_sample, owner_sample)
        self.assertEqual(len(list(i)), length)
        i.extend(extend_length, coll_sample_extend, owner_sample_extend)
        self.assertEqual(len(list(i)), extend_length)

    def test_data_iterator(self):
        length = 2
        extend_length = 3
        owner_sample = iter(["user1", "user2"])
        owner_sample_extend = iter(["user3", "user4", "user5"])
        resc_sample = iter(["resc1", "resc2"])
        resc_sample_extend = iter(["resc3", "resc4", "resc5"])
        coll_sample = iter(["/c1", "/c2"])
        coll_sample_extend = iter(["/c3", "/c4", "/c5"])
        i = DataObjectIterator(length,
                               coll_sample,
                               resc_sample,
                               owner_sample,
                               "testZone")
        self.assertEqual(len(list(i)), length)
        i.extend(extend_length,
                 coll_sample_extend,
                 resc_sample_extend,
                 owner_sample_extend)
        self.assertEqual(len(list(i)), extend_length)


if __name__ == '__main__':
    unittest.main()
