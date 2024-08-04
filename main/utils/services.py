import logging
import json

from typing import *

class Utils():

    def __init__(self) -> None:

        self.Log_format= f'[%(asctime)s]:[%(levelname)s]:[%(name)s:%(lineno)-2s] %(message)s'
        self.level = logging.INFO

        logging.basicConfig(level=self.level,format=self.Log_format)
        self.logger = logging.getLogger(name=__class__.__name__)

        self.logger.info("Logging setup done....!")

    def get_config(self,config:Dict, key:str) -> Any:
        '''
            This Recursive Function will  fetch the value 
            by passing the appropriate key for the given input JSON/dict 

            config : A JSON or Python Dictionary 
            key    : String seperated by . Ex: "key1.key2"

        '''
        list_of_keys = key.split('.')
        
        if list_of_keys[0] in config.keys():
            config = config[f'{list_of_keys[0]}']
        else:
            raise KeyError("Invalid Key...!")

        if len(list_of_keys)==1:
            return config
        
        list_of_keys = '.'.join(list_of_keys[1:])

        return self.get_config(json,list_of_keys)