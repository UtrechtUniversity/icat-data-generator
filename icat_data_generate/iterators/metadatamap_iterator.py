from icat_data_generate.iterators.irods_iterator import IrodsIterator


class MetadataMapIterator(IrodsIterator):

    def __init__(self, max_number, object_id_sample, meta_id_sample):
        self.object_id_sample = object_id_sample
        self.meta_id_sample = meta_id_sample
        super().__init__(max_number)

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            object_id = next(self.object_id_sample)[0]
            meta_id = next(self.meta_id_sample)[0]
            return (object_id, meta_id)
        else:
            raise StopIteration()

    def extend(self, add_number, object_id_sample, meta_id_sample):
        self.object_id_sample = object_id_sample
        self.meta_id_sample = meta_id_sample
        super().extend(add_number)
