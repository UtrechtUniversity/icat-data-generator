from icat_data_generate.iterators.irods_iterator import IrodsIterator


class UserIterator(IrodsIterator):

    def __init__(self, max_number, zone_name):
        self.zone_name = zone_name
        super().__init__(max_number)

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            user_name = f"user{str(self.current)}"
            return (user_name, "rodsuser", self.zone_name)
        else:
            raise StopIteration()
