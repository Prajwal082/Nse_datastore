from utils.services import Utils

from bronze.data_extractor import Nse



if __name__ == '__main__':

    nse = Nse(config)

    nse.get_DeliveryData()