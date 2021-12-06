# This file is for testing anything that is not vendor specific.
from multiweatherapi.multiweatherapi import multiweatherapi as multiweather
import pytest

def test_invalid_vendor():
    with pytest.raises(Exception) as ex_info:
        reading = multiweather.get_reading(vendor = 'bad_vendor')

    assert '"vendor" must be specified and in the approved vendor list' in str(ex_info.value), 'Invalid vendor exception was not raised for vendor "bad_vendor"'