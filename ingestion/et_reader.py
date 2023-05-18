from geo_utils.quadtile_utils import get_blank_feature_json, get_quad_key
from utilz.date_utils import get_ingestion_date_str
from utilz.load_config import load_from_yaml

class ETReader:
    def __init__(self):
        params = load_from_yaml("../metadata/configs.yaml")
        self.lat_index = params.lat_index
        self.lon_index = params.lon_index
        self.et1_index = params.et1_index
        self.county_index = params.county_index
        self.time_index = params.time_index
        self.quadtile_precision = params.quadtile_precision

    # Read CSV line by line
    # GENERATES A LIST OF ALL FEATURES FOR INSERTION AND ANOTHER MAPPED LIST FOR WELFORD GENERATION
    def read_csv_data(self, file_path):
        csv_file = open(file_path, 'r')
        recs = csv_file.readlines()

        # List for insertion
        feature_list = []
        # Actual data points grouped by key
        newdata_map = {}

        count = 0
        for line in recs:
            tokens = line.strip().split(",")

            # Ignore first header line
            if count == 0:
                count = 1
                continue

            lat = float(tokens[self.lat_index])
            lon = float(tokens[self.lon_index])
            ft_dict = get_blank_feature_json(lat, lon)

            et_val = tokens[self.et1_index]
            ft_dict["et_val"] = float(et_val)
            county = tokens[self.county_index]
            ft_dict["county"] = county
            timestamp = int(tokens[self.time_index])
            ft_dict["ts"] = timestamp

            # GET THE CUBELET KEY FOR THIS DATA POINT
            quad_key = get_quad_key(lat, lon, self.quadtile_precision)
            date_string = get_ingestion_date_str(timestamp)
            key = quad_key + "X" + date_string

            to_insert = []
            if key in newdata_map:
                to_insert = newdata_map[key]
            else:
                newdata_map[key] = to_insert
            to_insert.append(float(et_val))

            feature_list.append(ft_dict)


        return feature_list, newdata_map