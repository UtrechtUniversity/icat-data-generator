from itertools import product

from icat_data_generate.iterators.irods_iterator import IrodsIterator


class MetadataMapIterator(IrodsIterator):

    def __init__(self, max_number, object_id_subset, meta_id_subset):
        self.id_iterator = product(object_id_subset, meta_id_subset)
        super().__init__(max_number)

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            (object_id, meta_id) = next(self.id_iterator)
            return (object_id, meta_id)
        else:
            raise StopIteration()
