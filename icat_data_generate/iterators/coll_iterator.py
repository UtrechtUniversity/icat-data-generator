import os

from icat_data_generate.iterators.irods_iterator import IrodsIterator


class CollectionIterator(IrodsIterator):

    def __init__(self, max_number, collname_sample, owner_sample):
        self.owner_sample = owner_sample
        self.collname_sample = collname_sample
        super().__init__(max_number)

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            coll_parent_name = next(self.collname_sample)[0]
            while len(coll_parent_name) > 500:
                # Keep parent collection names to a manageable size so
                # we don't exceed maximum path length
                (coll_parent_name, _) = os.path.split(coll_parent_name)
            coll_base_name = "coll"+str(self.current)
            coll_full_name = os.path.join(coll_parent_name, coll_base_name)
            return (coll_parent_name, coll_full_name,
                    next(self.owner_sample)[0], "testZone",
                    "1170000000", "1170000000")
        else:
            raise StopIteration()

    def extend(self, add_number, collname_sample, owner_sample):
        self.owner_sample = owner_sample
        self.collname_sample = collname_sample
        super().extend(add_number)
