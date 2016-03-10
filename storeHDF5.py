#!/usr/bin/env python

import h5py
import numpy as np
import matplotlib.pyplot as plt   #Don't think i need

#HD5 contain 3 layers
## Top level is a file, which contains groups and datasets
## Each group may contain other groups and datasets
## Each dataset contains the data objects

def openFile(HD5, )