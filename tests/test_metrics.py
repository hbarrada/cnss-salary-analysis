# tests/test_metrics.py
import numpy as np
from yourmodule import calculate_gini, calculate_hoover_index, calculate_atkinson_index

def test_gini_ordering():
    equal = np.array([10,10,10,10])
    unequal = np.array([1,1,1,100])
    assert calculate_gini(equal) < calculate_gini(unequal)

def test_hoover_bounds():
    x = np.array([5,5,5,5])
    h = calculate_hoover_index(x)
    assert 0 <= h <= 1
