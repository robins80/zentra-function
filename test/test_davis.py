# This file is for testing anything specific to Davis weather stations.
from multiweatherapi.multiweatherapi import multiweatherapi as multiweather
import pytest

def test_invalid_ident():
    