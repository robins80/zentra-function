from APICall.main import main
import azure.functions as func
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
    print('Body = ' + str(body))
    request = func.HttpRequest(method = 'POST', url = os.environ.get('url'), body = body)
    print('request is a ' + str(type(request)))
    results = main(request)
    assert results is not None, 'Results are None.'