import math

#https://gist.github.com/alexalemi/2151722
class Welford(object):

    def __init__(self, k=0, M=0, S=0):
        self.k = k # Count
        self.M = M # Mean
        self.S = S # Sigma (xi-x)^2

    def merge(self, other, backup_flg=True):
        """Merge this accumulator with another one."""
        # backup for rollbacking
        if backup_flg:
            self.__backup_attrs()

        count = self.k + other.k
        delta = self.M - other.M
        delta2 = delta * delta
        m = (self.__count * self.__m + other.__count * other.__m) / count
        s = self.__s + other.__s + delta2 * (self.__count * other.__count) / count

        self.__count = count
        self.__m = m
        self.__s = s


    # ADD A SINGLE ELEMENT
    def update(self, x):
        if x is None:
            return
        self.k += 1
        newM = self.M + (x - self.M) * 1. / self.k
        newS = self.S + (x - self.M) * (x - newM)
        self.M, self.S = newM, newS

    # ADD A LIST
    def consume(self, lst):
        lst = iter(lst)
        for x in lst:
            self.update(x)

    # INGEST EITHER A SINGLE NUMBER OR A LIST
    def ingest(self, x):
        if hasattr(x, "__iter__"):
            self.consume(x)
        else:
            self.update(x)

    def mean(self):
        return self.M

    def meanfull(self):
        return self.mean, self.std() / math.sqrt(self.k)

    def std(self):
        if self.k == 1:
            return 0
        return math.sqrt(self.S / (self.k - 1))

    def __repr__(self):
        return "<Welford: {} +- {} +- {}>".format(self.mean(), self.std(), self.S)

if __name__ == "__main__":
    w = Welford()
    w.ingest([1,1,1])
    print(w)
    w.ingest([2,2,2])
    print(w)