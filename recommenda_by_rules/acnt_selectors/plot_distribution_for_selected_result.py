# !/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import matplotlib.pyplot as plt
import pandas as pd

__author__ = 'LH Liu'


def get_data_hist_distribution(data, col=None):
    if col is not None and isinstance(data, pd.DataFrame):
        plot_data = data[col]
    else:
        plot_data = data
    plt.hist(plot_data)
    plt.title('Distribution')
    plt.xlabel('Score')
    plt.ylabel('Numbers of accounts')
    plt.show()


def get_data_basic_distribution(x_value, y_value, drawer='o-'):
    plt.plot(x_value, y_value, drawer)
    plt.title('Distribution')
    plt.xlabel('Score')
    plt.ylabel('Numbers of accounts')
    plt.show()
