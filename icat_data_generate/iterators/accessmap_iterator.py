from icat_data_generate.iterators.irods_iterator import IrodsIterator


class AccessMapIterator(IrodsIterator):

    def __init__(self, max_number, object_id_sample, user_id_sample):
        self.object_id_sample = object_id_sample
        self.user_id_sample = user_id_sample
        super().__init__(max_number)

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            object_id = next(self.object_id_sample)[0]
            user_id = next(self.user_id_sample)[0]
            return (object_id, user_id, 1200)
        else:
            raise StopIteration()
