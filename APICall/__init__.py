import logging
import azure.functions as func

import json
from requests import Session, Request

class ZentraReadings:
    """
    A class used to represent a device's readings

    Attributes
    ----------
    request : Request
        a Request object defining the request made to the Zentra server
    response : Response
        a json response from the Zentra server
    device_info : dict
        a dictionary providing the device info
    timeseries : list
        a list of ZentraTimeseriesRecord objects

    """

    def __init__(self, sn=None, token=None, start_date=None, end_date=None, start_mrid=None, end_mrid=None):
        """
        Gets a device readings using a GET request to the Zentra API.

        Parameters
        ----------
        sn : str
            The serial number of the device
        token : ZentraToken
            The user's access token
        start_date : int, optional
            Return readings with timestamps ≥ start_date. 
        end_date : int, optional
            Return readings with timestamps ≤ end_date. 
        start_mrid : int, optional
            Return readings with mrid ≥ start_mrid.
        end_mrid : int, optional
            Return readings with mrid ≤ start_mrid.
        """
        if sn and token:
            self.get(sn, token, start_date, end_date, start_mrid, end_mrid)
        elif sn or token:
            raise Exception(
                '"sn" and "token" parameters must both be included.')
        else:
            # build an empty ZentraToken
            self.request = None
            self.response = None
            self.device_info = None
            self.measurement_settings = None
            self.time_settings = None
            self.locations = None
            self.installation_metadata = None

    def get(self, sn, token, start_date=None, end_date=None, start_mrid=None, end_mrid=None):
        """
        Gets a device readings using a GET request to the Zentra API.
        Wraps build and parse functions.

        Parameters
        ----------
        sn : str
            The serial number of the device
        token : ZentraToken
            The user's access token
        start_date : int, optional
            Return readings with timestamps ≥ start_date. 
        end_date : int, optional
            Return readings with timestamps ≤ end_date. 
        start_mrid : int, optional
            Return readings with mrid ≥ start_mrid.
        end_mrid : int, optional
            Return readings with mrid ≤ start_mrid.

        """
        self.build(sn, token, start_date, end_date, start_mrid, end_mrid)
        self.make_request()

        return self

    def build(self, sn, token, start_date=None, end_date=None, start_mrid=None, end_mrid=None):
        """
        Gets a device readings using a GET request to the Zentra API.

        Parameters
        ----------
        sn : str
            The serial number of the device
        token : ZentraToken
            The user's access token
        start_date : int, optional
            Return readings with timestamps ≥ start_date. 
        end_date : int, optional
            Return readings with timestamps ≤ end_date. 
        start_mrid : int, optional
            Return readings with mrid ≥ start_mrid.
        end_mrid : int, optional
            Return readings with mrid ≤ start_mrid.

        """
        self.request = Request('GET',
                               url='https://zentracloud.com/api/v3/get_readings',
                               headers={
                                   'Authorization': "Token " + token},
                               params={'sn': sn,
                                       'start_date': start_date,
                                       'end_date': end_date,
                                       'start_mrid': start_mrid,
                                       'end_mrid': end_mrid}).prepare()
        return self

    def make_request(self):
        """
        Sends a token request to the Zentra API and stores the response.
        """
        # Send the request and get the JSON response
        resp = Session().send(self.request)
        if resp.status_code != 200:
            raise Exception(
                'Incorrectly formatted request. Please ensure the user token and device serial number are correct.')
        elif str(resp.content) == str(b'{"Error": "Device serial number entered does not exitst"}'):
            raise Exception(
                'Error: Device serial number entered does not exist')

        self.response = resp.json()

        return self

def main(req: func.HttpRequest) -> func.HttpResponse:
    readings = ZentraReadings(token = 'b6270f954de4758e36f03d092e95a4b1c780747c', sn = 'z6-12564')   
    return func.HttpResponse(json.dumps(readings.response))