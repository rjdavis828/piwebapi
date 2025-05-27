import requests
import pandas as pd
import json
import os
import logging
import requests_kerberos
from requests.auth import HTTPBasicAuth
import warnings




class PIAssetServerClient:
    def __init__(self, base_url: str, auth: str = "Kerberos", verify: bool =True, username: str =None, password: str =None, asset_server: str = None, asset_database: str = None):
        """ Initialize the PI Asset Server Client.
        :param base_url: The base URL of the PI Asset Server. 
        :param auth: The authentication method to use ("Kerberos" or "Basic").
        :param verify: Whether to verify SSL certificates (default is True).
        :param username: The username for basic authentication (required if auth is "Basic").
        :param password: The password for basic authentication (required if auth is "Basic").
        :param asset_server: The name of the asset server.
        :param asset_database: The name of the asset database.
        """
        self.base_url = base_url
        self.username = username
        self.password = password
        self.logger = logging.getLogger("PIAssetServerClient")
        logging.basicConfig(level=logging.INFO)
        self.session = requests.Session()
        if auth == "Kerberos":
            self.logger.info("Using Kerberos authentication.")
            self.session.auth = requests_kerberos.HTTPKerberosAuth(mutual_authentication=requests_kerberos.DISABLED)
        elif auth == "Basic":
            if not username or not password:
                raise ValueError("Username and password must be provided for basic authentication.")
            self.logger.info("Using Kerberos authentication.")
            self.session.auth = HTTPBasicAuth(username, password)
        self.session.verify = verify
        if self.session.verify == False:
            # Suppress warnings about unverified HTTPS requests
            warnings.filterwarnings("ignore", message="Unverified HTTPS request")
        self._getAssetServerID(asset_server)
        if not self.AssetServerID:
            raise ValueError(f"Asset server '{asset_server}' not found.")
        self._getAssetDatabaseID(asset_database)
        if not self.AssetDBID:
            raise ValueError(f"Asset database '{asset_database}' not found.")
        
    def _getAssetServerID(self, data_server_name: str) -> None:
        """
        Get the ID of a data server by its name.
        :param data_server_name: The name of the data server.
        :return: The ID of the data server.
        """

        url = f"{self.base_url}/assetservers"
        response = self.session.get(url)
        if response.status_code == 200:
            data_servers = response.json()["Items"]
            match = next((server for server in data_servers if server["Name"] == data_server_name), None)
            if match:
                self.logger.info(f"Data server '{data_server_name}' found with ID: {match['WebId']}")
                self.AssetServerID = match["WebId"]
            else:
                self.logger.error(f"Data server '{data_server_name}' not found.")
        else:
            self.logger.error(f"Failed to get data server ID: {response.status_code} - {response.text}")
        return None

    def _getAssetDatabaseID(self, asset_database_name: str) -> None:
        """
        Get the ID of an asset database by its name.
        :param asset_database_name: The name of the asset database.
        :return: The ID of the asset database.
        """
        url = f"{self.base_url}/assetservers/{self.AssetServerID}/assetdatabases"
        response = self.session.get(url)
        if response.status_code == 200:
            asset_databases = response.json()["Items"]
            match = next((db for db in asset_databases if db["Name"] == asset_database_name), None)
            if match:
                self.logger.info(f"Asset database '{asset_database_name}' found with ID: {match['WebId']}")
                self.AssetDBID = match["WebId"]
            else:
                self.logger.error(f"Asset database '{asset_database_name}' not found.")
        else:
            self.logger.error(f"Failed to get asset database ID: {response.status_code} - {response.text}")
        return None
    
    def webids(self, object_list):
        """
        Retrieve webids for the specified tags from the given asset database.
        :param object_list: A list of elements or attributes to return webids for.
        :return: A list of dictionaries containing object names and their corresponding webids.
        """


class PIDataServerClient:
    def __init__(self, base_url: str, auth: str = "Kerberos", verify: bool =True, username: str =None, password: str =None):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.logger = logging.getLogger("PIDataServerClient")
        logging.basicConfig(level=logging.INFO)
        self.session = requests.Session()
        if auth == "Kerberos":
            self.logger.info("Using Kerberos authentication.")
            self.session.auth = requests_kerberos.HTTPKerberosAuth(mutual_authentication=requests_kerberos.DISABLED)
        elif auth == "Basic":
            if not username or not password:
                raise ValueError("Username and password must be provided for basic authentication.")
            self.logger.info("Using Basic authentication.")
            self.session.auth = HTTPBasicAuth(username, password)
        self.session.verify = verify
        if self.session.verify == False:
            # Suppress warnings about unverified HTTPS requests
            warnings.filterwarnings("ignore", message="Unverified HTTPS request")
        

    def _getDataServerID(self, data_server_name: str) -> str:
        """
        Get the ID of a data server by its name.
        :param data_server_name: The name of the data server.
        :return: The ID of the data server.
        """

        url = f"{self.base_url}/dataservers"
        self.logger.info(f"{url}")
        response = self.session.get(url)
        self.logger.info(f"URL: {response.url}")
        if response.status_code == 200:
            data_servers = response.json()["Items"]
            match = next((server for server in data_servers if server["Name"] == data_server_name), None)
            if match:
                self.logger.info(f"Data server '{data_server_name}' found with ID: {match['WebId']}")
                return match["WebId"]
            else:
                self.logger.error(f"Data server '{data_server_name}' not found.")
        else:
            self.logger.error(f"Failed to get data server ID: {response.status_code} - {response.text}")
        return None
    
    
    def webids(self, dataserver: str, taglist: list):
        """
        Retrieve webids for the specified tags from the given data server.
        :param dataserver: The name of the data server.
        :param taglist: A list of tags to retrieve webids for.
        :return: A list of dictionaries containing tag names and their corresponding webids.
        """
        # Get the data server ID
        data_server_id = self._getDataServerID(dataserver)
        if not data_server_id:
            self.logger.error(f"Data server '{dataserver}' not found.")
            return None
        if len(taglist) == 0:
            self.logger.error(f"No tags provided")
            return None
        # Construct the URL for the points endpoint 
        pointsURL = f"{self.base_url}/dataservers/{data_server_id}/points"
    
        # Retrieve webids for the tags
        webids = []
        for tag in taglist:
            url = f"{pointsURL}?namefilter={tag}"
            response = self.session.get(url)
            if response.status_code == 200:
                data = response.json()
                if data["Items"]:
                    webids.append(dict({'tag': tag, 'webid': data["Items"][0]["WebId"]}))
                else:
                    self.logger.error(f"Tag '{tag}' not found in data server '{dataserver}'.")
            else:
                self.logger.error(f"Failed to get webid for tag '{tag}': {response.status_code} - {response.text}")
        
        if not webids:
            self.logger.error(f"No valid tags found in data server '{dataserver}'.")
            return None
        
        return webids
    
    def compressed_data(self, dataserver, taglist: list, starttime: str, endtime: str):
        """
        Retrieve compressed data from the specified data server for the given tags.
        :param dataserver: The name of the data server.
        :param taglist: A list of tags to retrieve data for.
        :param starttime: The start time for the data retrieval.
        :param endtime: The end time for the data retrieval.
        :return: A DataFrame containing the retrieved data.
        """
        # Get the data server ID
        data_server_id = self._getDataServerID(dataserver)
        if not data_server_id:
            self.logger.error(f"Data server '{dataserver}' not found.")
            return None
        if len(taglist) == 0:
            self.logger.error(f"No tags provided")
            return None
        # Construct the URL for the points endpoint 
        pointsURL = f"{self.base_url}/dataservers/{data_server_id}/points"
    
        # Retrieve webids for the tags
        webids = self.webids(dataserver, taglist)
        if not webids:
            self.logger.error(f"No valid tags found in data server '{dataserver}'.")
            return None
        
        # Pull data for each tag
        data = pd.DataFrame()
        for item in webids:
            url = f"{self.base_url}/streams/{item['webid']}/recorded"
            params = {
                "startTime": starttime,
                "endTime": endtime,
                "maxCount": 1000
                #selectedFields: selectedFields
            }
            response = self.session.get(url, params=params)
            if response.status_code == 409:
                self.logger.error(f"Conflict error for tag '{item['tag']}': {response.status_code} - {response.text}")
                continue
            if response.status_code == 400:
                self.logger.error(f"Bad request for tag '{item['tag']}': {response.status_code} - {response.text}")
                continue
            if response.status_code == 200:
                data_response = response.json()
                if data_response["Items"]:
                    df = pd.DataFrame(data_response["Items"])
                    df.insert(loc=0, column ='Tag', value = item['tag'])
                    df["Value"] = pd.to_numeric(df["Value"], errors='coerce')
                    df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True)
                    data = pd.concat([data, df], ignore_index=True)
                else:
                    self.logger.error(f"No data found for tag '{item['tag']}' in the specified time range.")
            else:
                self.logger.error(f"Failed to get data for tag '{item['tag']}': {response.status_code} - {response.text}")
        return data
    

    def summary_data(self, dataserver, taglist: list, starttime: str, endtime: str, summaryDuration: str, summaryType: str = "Average"):
        """
        Retrieve summary data from the specified data server for the given tags.
        :param dataserver: The name of the data server.
        :param taglist: A list of tags to retrieve data for.
        :param starttime: The start time for the data retrieval.
        :param endtime: The end time for the data retrieval.
        :param summarytype: The type of summary to retrieve (e.g., "Average", "Total").
        :return: A DataFrame containing the retrieved summary data.
        """
        # Get the data server ID
        data_server_id = self._getDataServerID(dataserver)
        if not data_server_id:
            self.logger.error(f"Data server '{dataserver}' not found.")
            return None
        if len(taglist) == 0:
            self.logger.error(f"No tags provided")
            return None
        # Construct the URL for the points endpoint 
        pointsURL = f"{self.base_url}/dataservers/{data_server_id}/points"
    
        # Retrieve webids for the tags
        webids = self.webids(dataserver, taglist)
        if not webids:
            self.logger.error(f"No valid tags found in data server '{dataserver}'.")
            return None
        
        if not summaryType:
            raise ValueError("Summary type must be provided.")
        if not summaryDuration:
            raise ValueError("Summary duration must be provided.")

        data = pd.DataFrame()
        for point in webids:
            url = f"{self.base_url}/streams/{point['webid']}/summary"
            #self.logger.info(f"Fetching summary data for tag '{point['tag']}' from {url}")
            params = {
                "startTime": starttime,
                "endTime": endtime,
                "summaryDuration": summaryDuration,
                "summaryType": summaryType,
                'selectedFields': "Items.Value.Timestamp;Items.Value.Value"                        
            }
            response = self.session.get(url, params=params)
            #self.logger.info(f"Fetching data at {response.url}")
            if response.status_code == 409:
                self.logger.error(f"Conflict error for tag '{point['tag']}': {response.status_code} - {response.text}")
                continue
            if response.status_code == 400:
                self.logger.error(f"Bad request for tag '{point['tag']}': {response.status_code} - {response.text}")
                continue
            if response.status_code == 200:
                data_response = response.json()
                if data_response["Items"]:
                    data_temp = [item["Value"] for item in data_response["Items"]]
                    df = pd.DataFrame(data_temp)
                    df.insert(loc=0, column ='Tag', value = point['tag'])
                    df["Value"] = pd.to_numeric(df["Value"], errors='coerce')
                    df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True)
                    data = pd.concat([data, df], ignore_index=True)
                else:
                    self.logger.error(f"No data found for tag '{point['tag']}' in the specified time range.")
            else:
                self.logger.error(f"Failed to get data for tag '{point['tag']}': {response.status_code} - {response.text}")
        return data
    
    def interpolated_data(self, dataserver, taglist: list, starttime: str, endtime: str, interval: str):
        """
        Retrieve summary data from the specified data server for the given tags.
        :param dataserver: The name of the data server.
        :param taglist: A list of tags to retrieve data for.
        :param starttime: The start time for the data retrieval.
        :param endtime: The end time for the data retrieval.
        :param summarytype: The type of summary to retrieve (e.g., "Average", "Total").
        :return: A DataFrame containing the retrieved summary data.
        """
        # Get the data server ID
        data_server_id = self._getDataServerID(dataserver)
        if not data_server_id:
            self.logger.error(f"Data server '{dataserver}' not found.")
            return None
        if len(taglist) == 0:
            self.logger.error(f"No tags provided")
            return None
        # Construct the URL for the points endpoint 
        pointsURL = f"{self.base_url}/dataservers/{data_server_id}/points"
    
        # Retrieve webids for the tags
        webids = self.webids(dataserver, taglist)
        if not webids:
            self.logger.error(f"No valid tags found in data server '{dataserver}'.")
            return None
        
        if not interval:
            raise ValueError("Interval must be provided.")

        data = pd.DataFrame()
        for item in webids:
            url = f"{self.base_url}/streams/{item['webid']}/interpolated"
            self.logger.info(f"Fetching interpolated data for tag '{item['tag']}' from {url}")
            params = {
                "startTime": starttime,
                "endTime": endtime,
                "interval": interval
            }
            response = self.session.get(url, params=params)
            if response.status_code == 409:
                self.logger.error(f"Conflict error for tag '{item['tag']}': {response.status_code} - {response.text}")
                continue
            if response.status_code == 400:
                self.logger.error(f"Bad request for tag '{item['tag']}': {response.status_code} - {response.text}")
                continue
            if response.status_code == 200:
                data_response = response.json()
                if data_response["Items"]:
                    df = pd.DataFrame(data_response["Items"])
                    df.insert(loc=0, column ='Tag', value = item['tag'])
                    df["Value"] = pd.to_numeric(df["Value"], errors='coerce')
                    df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True)
                    data = pd.concat([data, df], ignore_index=True)
                else:
                    self.logger.error(f"No data found for tag '{item['tag']}' in the specified time range.")
            else:
                self.logger.error(f"Failed to get data for tag '{item['tag']}': {response.status_code} - {response.text}")
        return data
    
    def send_recorded_value(self, webid: str, data: dict):
        """
        Send a single recorded value to a PI Point.

        Args:
            webid (str): The WebID of the PI Point.
            data (dict): A dictionary containing the value and timestamp to be sent.
                Example: {"Value": 123.45, "Timestamp": "2023-10-01T12:00:00Z"}
        """
        url = f"{self.base_url}/streams/{webid}/recorded"

        if data is None:
            raise ValueError("Data cannot be None.")
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary.")
        if "Value" not in data:
            raise ValueError("Data must contain a 'Value' key.")
        if "Timestamp" not in data:
            raise ValueError("Data must contain a 'Timestamp' key.")
        
        data = data

        try:
            response = requests.post(
                url,
                auth=self.kerberos_auth,
                data=json.dumps(data),
                headers=self.headers,
                verify=True,
            )
            response.raise_for_status()
            self.logger.info("Recorded value sent successfully.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to send recorded value: {e}")
       