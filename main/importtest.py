from utils.services import Utils

from bronze.data_extractor import Nse

config = '''{  
    "api" : {
        "host" : "https://www.nseindia.com",
        "path" : "/api/historical/securityArchives",
        "query" : "?from={start}&to={end}&symbol={key}&dataType=priceVolumeDeliverable&series=ALL",
        "headers" : {"User-Agent" :"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"}
        },
    "google_sheet" : {
            "id"   : "1yoQZNPwdYoQte13GKAsEzQ4WkABx77usQiSW2O-Hj-I", 
            "name" : "stock_lookup",
            "path" : "https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
        }
}'''


if __name__ == '__main__':

    nse = Nse(config)

    nse.get_DeliveryData()