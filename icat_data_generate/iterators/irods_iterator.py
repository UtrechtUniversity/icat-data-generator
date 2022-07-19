class IrodsIterator:
    def __init__(self, max_number):
        self.current = 0
        self.max_number = max_number

    def __iter__(self):
        return self

    def __next__(self):
        if self.max_number == 0 or self.max_number >= self.current:
            self.current += 1
            return self.current
        else:
            raise StopIteration()

    # To be called after StopIteration to extend iterator with
    # more iterations
    def extend(self, extra_iterations):
        self.max_number += extra_iterations
