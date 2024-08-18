import requests
import json
import pandas as pd
import os

from typing import *
from collections import defaultdict
from datetime import date, timedelta

from utils.services import Utils
from api.config.apiconfig import config

class Nse(Utils):

    def __init__(self,config:Dict[str,Any] = config) -> None:
        super().__init__()

        self.config = config
        
        self.__init_vars()

        self.__session = requests.session()

        self.__session.get(url=self.__host,headers = self.__header)

    def __init_vars(self):
        config = json.loads(self.config)
        self.__host       = self.get_config(config_data=config,key = "api.host")
        self.__header     = self.get_config(config_data=config,key = "api.headers")
        self.__path       = self.get_config(config_data=config,key = "api.path")
        self.__query      = self.get_config(config_data=config,key = "api.query")
        self.__sheet_id   = self.get_config(config_data=config,key = "google_sheet.id")
        self.__sheet_name = self.get_config(config_data=config,key = "google_sheet.name")
        self.__sheet_path = self.get_config(config_data=config,key = "google_sheet.path")

    def read_PramFile(self) -> dict:

        df = pd.read_csv(self.__sheet_path.format(sheet_id = self.__sheet_id,sheet_name = self.__sheet_name))

        dict_conxt = defaultdict(list)

        for script,Duration,is_Active in zip(list(df['Script_Name']),list(df['Duration']),list(df['Is_Active'])) : 
            dict_conxt[f'{script}'] = {'is_Active' : is_Active,'Duration': Duration}

        return dict_conxt 

    def get_DeliveryData(self) -> Any:
        dict_conxt = self.read_PramFile()

        for key, val in dict_conxt.items():
            if val.get("is_Active"):
                
                URL= self.__host + self.__path + self.__query.format(start = (date.today() - timedelta(days= val.get("Duration"))).strftime("%d-%m-%Y"),end = date.today().strftime("%d-%m-%Y"),key = key)
                try:
                    response = self.__session.get(URL, headers = self.__header)

                    if response.status_code == 200:
                        json_data = response.json()['data']
                        data = json.dumps(json_data,indent=4)

                        self.logger.info(f"Writing JSON data for {key}")

                        if not os.path.isdir(f"raw\{key}"):
                            os.mkdir(f"raw\{key}")
                            
                        with open(f"raw\{key}\{key}.json", 'w') as f:
                            f.write(data)
                    else:
                        self.logger.error(f"Status Code Returned for {key} is {response.status_code}")
                        
                except Exception as e:
                    self.logger.error(f'An error occured while grabbing the data type :{e.__class__.__name} Error : {e}')
                    raise e



if __name__ == '__main__':

    nse = Nse(config)

    nse.get_DeliveryData()