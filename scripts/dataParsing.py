#!/usr/bin/python
import os

def getfile(filename):
    lines = open(filename).read().splitlines()
    for line in lines:
        print line