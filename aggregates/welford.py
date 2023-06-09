#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This library is python(numpy) implementation of Welford's algorithm, 
which is online and parallel algorithm for calculating variances.

Welfords method is more numerically stable than the standard method as
described in the following blog,
    * Accurately computing running variance: www.johndcook.com/blog/standard_deviation

This library is inspired by the jvf's implementation, which is implemented
without using numpy library.
    * implementaion done by jvf: github.com/jvf/welford
"""

#https://github.com/a-mitani/welford
import numpy as np
import pickle


class Welford:
    """class Welford

     Accumulator object for Welfords online / parallel variance algorithm.

    Attributes:
        count (int): The number of accumulated samples.
        mean (array(D,)): Mean of the accumulated samples.
        var_s (array(D,)): Sample variance of the accumulated samples.
        var_p (array(D,)): Population variance of the accumulated samples.
    """

    def __init__(self, elements=None):
        """__init__

        Initialize with an optional data. 
        For the calculation efficiency, Welford's method is not used on the initialization process.

        Args:
            elements (array(S, D)): data samples.

        """

        # Initialize instance attributes
        if elements is None:
            self.__shape = None
            # current attribute values
            self.__count = 0
            self.__m = None
            self.__s = None
            # previous attribute values for rollbacking
            self.__count_old = None
            self.__m_old = None
            self.__s_old = None

        else:
            self.__shape = elements[0].shape
            # current attribute values
            self.__count = elements.shape[0]
            self.__m = np.mean(elements, axis=0)
            self.__s = np.var(elements, axis=0, ddof=0) * elements.shape[0]
            # previous attribute values for rollbacking
            self.__count_old = None
            self.__init_old_with_nan()

    def init_from_saved_attrs(self, count, mean, sum):
        self.__shape = ()
        # current attribute values
        self.__count = count

        self.__m = np.array(mean)
        self.__s = np.array(sum)

        self.__count_old = None
        self.__init_old_with_nan()


    def dump_state(self):
        return self.__count, self.__m.item(), self.__s.item()

    @property
    def count(self):
        return self.__count

    @property
    def mean(self):
        return self.__m

    @property
    def var_s(self):
        return self.__getvars(ddof=1)

    @property
    def var_p(self):
        return self.__getvars(ddof=0)

    def add(self, element, backup_flg=True):
        """ add

        add one data sample.

        Args:
            element (array(D, )): data sample.
            backup_flg (boolean): if True, backup previous state for rollbacking.

        """
        # Initialize if not yet.
        if self.__shape is None:
            self.__shape = element.shape
            self.__m = np.zeros(element.shape)
            self.__s = np.zeros(element.shape)
            self.__init_old_with_nan()
        # argument check if already initialized
        else:
            assert element.shape == self.__shape

        # backup for rollbacking
        if backup_flg:
            self.__backup_attrs()

        # Welford's algorithm
        self.__count += 1
        delta = element - self.__m
        self.__m += delta / self.__count
        self.__s += delta * (element - self.__m)

    def add_all(self, elements, backup_flg=True):
        """ add_all

        add multiple data samples.

        Args:
            elements (array(S, D)): data samples.
            backup_flg (boolean): if True, backup previous state for rollbacking.

        """
        # backup for rollbacking
        if backup_flg:
            self.__backup_attrs()

        for elem in elements:
            self.add(elem, backup_flg=False)

    def rollback(self):
        self.__count = self.__count_old
        self.__m[...] = self.__m_old[...]
        self.__s[...] = self.__s_old[...]

    def merge(self, other, backup_flg=True):
        """Merge this accumulator with another one."""
        # backup for rollbacking
        if backup_flg:
            self.__backup_attrs()

        count = self.__count + other.__count
        delta = self.__m - other.__m
        delta2 = delta * delta
        m = (self.__count * self.__m + other.__count * other.__m) / count
        s = self.__s + other.__s + delta2 * (self.__count * other.__count) / count

        self.__count = count
        self.__m = m
        self.__s = s

    def __getvars(self, ddof):
        if self.__count <= 0:
            return None
        min_count = ddof
        if self.__count <= min_count:
            return np.full(self.__shape, np.nan)
        else:
            return self.__s / (self.__count - ddof)

    def __backup_attrs(self):
        if self.__shape is None:
            pass
        else:
            self.__count_old = self.__count
            self.__m_old[...] = self.__m[...]
            self.__s_old[...] = self.__s[...]

    def __init_old_with_nan(self):
        self.__m_old = np.empty(self.__shape)
        self.__m_old[...] = np.nan
        self.__s_old = np.empty(self.__shape)
        self.__s_old[...] = np.nan
        self.__s_old[...] = np.nan

    def __repr__(self):
        return "<Welford: {} +- {} +- {}>".format(self.mean, self.var_s, self.var_p)

if __name__ == "__main__":
    w = Welford()
    w.add_all(np.array([1,5,3]))
    print(w)
    count, m, s = w.dump_state()

    w1 = Welford()
    w1.init_from_saved_attrs(count, m, s)

    #print(type(m.item()), m.item(), m.shape, m, s, s.shape)
    print(w1)

    w.add_all(np.array([5, 5, 5]))
    print(w)
    w1.add_all(np.array([5, 5, 5]))
    print(w1)

    '''w1.add_all(np.array([2, 8, 2]))
    print(w1)

    a1 = Welford()
    a1.add_all(np.array([1, 1, 1]))
    print(a1)

    a2 = Welford()
    a2.add_all(np.array([2, 8, 2]))
    a1.merge(a2)
    print(a1)

    a11 = Welford()
    a11.add_all(np.array([1, 1, 1]))
    print(a11)

    a11.add_all(np.array([2, 8, 2]))
    print(a11)'''