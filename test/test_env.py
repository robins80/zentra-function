from APICall.main import check_parms, main
import azure.functions as func
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import logging
import os
import pytest

logger = logging.getLogger('zentra')

@pytest.fixture(scope='session', autouse=True)
def load_env():
    load_dotenv()

def test_zentra_call():
    body = bytes(os.environ.get('ZENTRA_GOOD'), 'utf-8')
    # print('body = ' + str(body))
    request = func.HttpRequest(method = 'POST', url = os.environ.get('url'), body = body)
    results = main(request)
    assert results is not None, 'Results are None.'

def test_start_date_after_end_date():
    body = bytes(os.environ.get('ZENTRA_GOOD'), 'utf-8')
    parms = json.loads(os.environ.get('ZENTRA_GOOD'))
    parms['end_date'] = datetime.now()
    parms['start_date'] = parms['end_date'] + timedelta(seconds = 86400)
    # print('parms = ' + str(parms))
    
    with pytest.raises(Exception) as error:
        check_parms(**parms)

    assert 'Start date is after the end date' in str(error.value), 'Didn''t get the expected error message, we got ' + str(error.value)

def test_empty_string_vendor():
    body = bytes(os.environ.get('ZENTRA_GOOD'), 'utf-8')
    parms = json.loads(os.environ.get('ZENTRA_GOOD'))
    parms['vendor'] = ''
    # print('parms = ' + str(parms))

    with pytest.raises(Exception) as error:
        check_parms(**parms)

    assert 'Vendor was not specified' in str(error.value), 'Didn''t get the expected error message, we got ' + str(error.value)

def test_None_string_vendor():
    body = bytes(os.environ.get('ZENTRA_GOOD'), 'utf-8')
    parms = json.loads(os.environ.get('ZENTRA_GOOD'))
    parms['vendor'] = None
    # print('parms = ' + str(parms))

    with pytest.raises(Exception) as error:
        check_parms(**parms)

    assert 'Vendor was not specified' in str(error.value), 'Didn''t get the expected error message, we got ' + str(error.value)

def test_empty_string_identifier():
    body = bytes(os.environ.get('ZENTRA_GOOD'), 'utf-8')
    parms = json.loads(os.environ.get('ZENTRA_GOOD'))
    parms['sn'] = ''
    # print('parms = ' + str(parms))

    with pytest.raises(Exception) as error:
        check_parms(**parms)

    assert 'identifier is empty' in str(error.value), 'Didn''t get the expected error message, we got ' + str(error.value)

def test_None_identifier_vendor():
    body = bytes(os.environ.get('ZENTRA_GOOD'), 'utf-8')
    parms = json.loads(os.environ.get('ZENTRA_GOOD'))
    parms['sn'] = None
    # print('parms = ' + str(parms))

    with pytest.raises(Exception) as error:
        check_parms(**parms)

    assert 'identifier is None' in str(error.value), 'Didn''t get the expected error message, we got ' + str(error.value)    