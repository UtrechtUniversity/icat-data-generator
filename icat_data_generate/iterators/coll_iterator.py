from icat_data_generate.iterators.irods_iterator import IrodsIterator


class CollectionIterator(IrodsIterator):

    def __init__(self, max_number, collname_sample, owner_sample):
        self.owner_sample = owner_sample
        self.collname_sample = collname_sample
        super().__init__(max_number)

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            return (next(self.collname_sample)[0], "/coll"+str(self.current),
                    next(self.owner_sample)[0], "testZone",
                    "1170000000", "1170000000")
        else:
            raise StopIteration()

    def extend(self, add_number, collname_sample, owner_sample):
        self.owner_sample = owner_sample
        self.collname_sample = collname_sample
        super().extend(add_number)
