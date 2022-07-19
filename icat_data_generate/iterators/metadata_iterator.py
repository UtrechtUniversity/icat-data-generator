from icat_data_generate.iterators.irods_iterator import IrodsIterator


class MetadataIterator(IrodsIterator):

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            dummydata = "Dummy metadata " + str(self.current)
            return (dummydata, dummydata, dummydata)
        else:
            raise StopIteration()
