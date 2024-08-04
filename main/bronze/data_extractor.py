import requests
import json
import pandas as pd

from pyspark.sql import DataFrame
from  utils.services import Utils

class Nse(Utils):

    def __init__(self) -> None:
        self.__headers = {'User-Agent' :'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'}
        self.__session = requests.session()
        self.__session.get(url=host,headers = self.__headers)
        self.read_PramFile()

    def __init_vars(self):
        self.get_config()

    def read_PramFile(self) -> dict:

        sheet_name = "1yoQZNPwdYoQte13GKAsEzQ4WkABx77usQiSW2O-Hj-I"
        df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_name}/export?format=csv")

        self.dict_conxt = {}

        for script, is_Active in zip(list(df['Script_Name']),list(df['Is_Active'])) : self.dict_conxt[f'{script}'] = is_Active

        return self.dict_conxt 

    def get_DeliveryData(self):

        for key, val in self.dict_conxt.items():
            if val==1:
                
                URL=f"https://www.nseindia.com/api/historical/securityArchives?from=30-06-2023&to=30-12-2023&symbol={key}&dataType=priceVolumeDeliverable&series=ALL"

                response = self.__session.get(URL, headers = self.__headers)

                print("Acceped..!") if response.status_code == 200 else print(f"Response returned a status code {response.status_code}")

                if response.status_code == 200:

                    print(response.json())


if __name__ == '__main__':

    nse = Nse()

    nse.get_DeliveryData()