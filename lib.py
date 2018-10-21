#!/usr/bin/python2
# -*- coding: utf-8 -*-

import codecs

class PersistentSet:

    def __init__(self, filename, my_set = None):
        self._filename = filename
        self._my_set = set()
        try:
            with open(filename) as in_file:
                for line in in_file.readlines():
                    self._my_set.add(line.strip())
        except:
            if my_set is not None:
                self._my_set = my_set

    def __iter__(self):
        return self._my_set.__iter__()

    def __next__(self):
        return self._my_set.__next__()

    def add(self, item):
        self._my_set.add(item)

    def remove(self, item):
        self._my_set.remove(item)

    def add_from(self, other_set):
        self._my_set.update(other_set._my_set)

    def remove_from(self, other_set):
        self._my_set -= other_set._my_set

    def save(self):
        with codecs.open(self._filename, 'w', 'utf-8') as out_file:
            for item in self._my_set:
                if item is not '': out_file.write(item + '\n')

    def clear(self):
        self._my_set.clear()

    def is_empty(self):
        return len(self._my_set) == 0
