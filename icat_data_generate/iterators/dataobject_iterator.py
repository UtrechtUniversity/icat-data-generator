import random

from icat_data_generate.iterators.irods_iterator import IrodsIterator


class DataObjectIterator(IrodsIterator):
    def __init__(self, max_number, collections, resc_names, owner_names,
                 owner_zone):
        self.collections = collections
        self.resc_names = resc_names
        self.owner_names = owner_names
        self.owner_zone = owner_zone
        super().__init__(max_number)

    def __next__(self):
        if self.max_number == 0 or self.max_number > self.current:
            self.current += 1
            coll = next(self.collections)
            coll_id = coll[0]
            coll_name = coll[1]
            resc_name = next(self.resc_names)[0]
            owner = next(self.owner_names)[0]
            do_name = f"dataobject{str(self.current)}.dat"
            data_path = f"/data/{resc_name}{coll_name}/{do_name}"
            size = random.randint(1, 1024768)
            return (coll_id, do_name, 1, "generic", size, resc_name, data_path,
                    owner, self.owner_zone, "1170000000", "1170000000")
        else:
            raise StopIteration()

    def extend(self, add_number, coll_sample, resc_sample, user_sample):
        self.collections = coll_sample
        self.resc_names = resc_sample
        self.owner_names = user_sample
        super().extend(add_number)
