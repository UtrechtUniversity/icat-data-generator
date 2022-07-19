from icat_data_generate.iterators.irods_iterator import IrodsIterator


class RescIterator(IrodsIterator):

    def __init__(self, max_number, zone_name):
        self.zone_name = zone_name
        super().__init__(max_number)

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            resc_name = f"resc{str(self.current)}"
            resc_path = f"/resources/{str(self.current)}"
            return (resc_name,
                    self.zone_name,
                    "unixfilesystem",
                    "cache",
                    "provider.local",
                    resc_path)
        else:
            raise StopIteration()
