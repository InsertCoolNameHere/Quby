import pymongo
import urllib.parse
from utilz.load_config import load_from_yaml
import pickle
import os.path

water_sites_location_file = "../geo_utils/water_sites.pkl"

class MongoBufferedReader:
    def __init__(self):
        params = load_from_yaml("../metadata/configs.yaml")
        self.lat_index = params.lat_index
        self.lon_index = params.lon_index
        self.et1_index = params.et1_index
        self.county_index = params.county_index
        self.time_index = params.time_index
        self.quadtile_precision = params.quadtile_precision

        username = urllib.parse.quote_plus(params.username)
        password = urllib.parse.quote_plus(params.password)

        mongo_url = 'mongodb://%s:%s@lattice-100:27018/' % (username, password)
        mongo_db_name = params.mongo_db_name
        sustainclient = pymongo.MongoClient(mongo_url)
        self.sustain_db = sustainclient[mongo_db_name]
        self.station_map = {}

    # READ THE ACTUAL DATA FROM THE WATER READINGS COLLECTION
    def read_water_data_from_mongo(self, collection_name, start, chunk_size):
        print("OFFSET >>>", start, ">>>", len(self.station_map.keys()))
        readings_collection = self.sustain_db[collection_name]
        key_query = {}

        if start == 0:
            records = readings_collection.find(key_query).limit(chunk_size)
        else:
            records = readings_collection.find(key_query).limit(chunk_size).skip(start)
        query_results = list(records)

        features = []
        if len(query_results) > 0:
            for qr in query_results:
                ft = {}
                ft['attribute_name'] = qr['measurement_name']
                ft['attribute_value'] = qr['measurement_value']
                ft['epoch_time'] = qr['epoch_time']
                location_id = qr['MonitoringLocationIdentifier']
                coordinates = self.station_map[location_id]
                ft['lat'] = coordinates[1]
                ft['lon'] = coordinates[0]
                features.append(ft)

        return features

    # READ STATION LOCATION DATA FROM MONGODB
    def initialize_station_locations(self, geo_collection_name):
        # GET WATER SITE LOCATIONS
        if os.path.isfile(water_sites_location_file):
            with open(water_sites_location_file, 'rb') as handle:
                self.station_map = pickle.load(handle)
        else:
            self.read_geo_data(geo_collection_name)

        print(">>>",len(self.station_map.keys()))


    # READ THE WATER SITES FROM MONGODB AND MAP THEM TO THEIR COORDINATES - ONE TIME
    def read_geo_data(self, geo_collection_name):
        geo_collection = self.sustain_db[geo_collection_name]
        key_query = {}
        query_results = list(geo_collection.find(key_query))

        print("GOT...")
        for station in query_results:
            geom_type = station['geometry']['type']

            if geom_type == "Point":
                location_id = station['properties']['MonitoringLocationIdentifier']
                coordinates = station['geometry']['coordinates']
                self.station_map[location_id] = coordinates

        print(len(self.station_map.keys()))

        with open(water_sites_location_file, 'wb') as handle:
            pickle.dump(self.station_map, handle, protocol=pickle.HIGHEST_PROTOCOL)



if __name__ == "__main__":
    collection_name = "aqua_data_backup"
    geo_collection_name = "water_quality_sites"

    br = MongoBufferedReader()
    br.initialize_station_locations(geo_collection_name)

