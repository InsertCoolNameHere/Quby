from geo_utils.quadtile_utils import get_quad_key
from utilz.date_utils import get_ingestion_date_str_milliseconds
from aggregates.welford import Welford
import numpy as np
from mongo_utils.mongo_buffered_reader import MongoBufferedReader
from mongo_utils.mongo_communications import Mongo_Communicator
from utilz.load_config import load_from_yaml


# Creates Cubelets from a MongoDB Collection
class CubeletGenerator():
    collection_name= "synthetic_ET"
    agg_collection_name= "synthetic_ET_agg"

    def __init__(self):
        params = load_from_yaml("../metadata/configs.yaml")
        self.lat_index = params.lat_index
        self.lon_index = params.lon_index
        self.et1_index = params.et1_index
        self.county_index = params.county_index
        self.time_index = params.time_index
        self.quadtile_precision = params.quadtile_precision

        self.cubelets_attributewise_map = {}

    # CREATE AND GROUP FETCHED DATA CHUNKS BY THEIR ATTRIBUTE AND KEY
    def group_newdata_by_attribute_key(self, recs):
        # Actual data points grouped by key
        newdata_attributewise_map = {}

        count = 0
        keys = set()
        for ft in recs:
            lat = ft['lat']
            lon = ft['lon']
            attribute_name = ft['attribute_name']
            attribute_value = ft['attribute_value']
            record_time = ft['epoch_time']

            # GET THE CUBELET KEY FOR THIS DATA POINT
            quad_key = get_quad_key(lat, lon, self.quadtile_precision)
            date_string = get_ingestion_date_str_milliseconds(record_time)

            if count == 0:
                print(date_string)
                count = 1
            key = quad_key + "X" + date_string
            keys.add(key)

            if attribute_name in newdata_attributewise_map:
                newdata_map = newdata_attributewise_map[attribute_name]
            else:
                newdata_map = {}
                newdata_attributewise_map[attribute_name] = newdata_map

            to_insert = []
            if key in newdata_map:
                to_insert = newdata_map[key]
            else:
                newdata_map[key] = to_insert
            to_insert.append(float(attribute_value))

        print("UNIQUE KEYS:", keys)
        return newdata_attributewise_map

    # CREATE AND MERGE CUBELETS
    # TAKES A SET OF GROUPED DICTIONARY WITH READINGS IN ARRAYS AS INPUT
    def create_and_merge_local_Cubelets(self, new_data_attributewise_map):

        for attribute in new_data_attributewise_map.keys():
            new_aggregates_map = new_data_attributewise_map[attribute]
            for k in new_aggregates_map.keys():
                vals = new_aggregates_map[k]

                if attribute in self.cubelets_attributewise_map:
                    saved_cubelet_map = self.cubelets_attributewise_map[attribute]
                else:
                    saved_cubelet_map = {}
                    self.cubelets_attributewise_map[attribute] = saved_cubelet_map

                if k in saved_cubelet_map:
                    w = saved_cubelet_map[k]
                    w.add_all(np.array(vals))
                else:
                    w = Welford()
                    w.add_all(np.array(vals))
                    saved_cubelet_map[k] = w

        tot_cubes = 0
        for attribute in self.cubelets_attributewise_map:
            cc = self.cubelets_attributewise_map[attribute]
            tot_cubes+=len(cc)

        print("TOTAL CUBELETS RIGHT NOW:", tot_cubes)


    def merge_Cubelets_With_MongoDB(self):
        mc = Mongo_Communicator()

        # FOR EACH ATTRIBUTE XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        for attribute in self.cubelets_attributewise_map.keys():
            cubelet_map_of_new_records = self.cubelets_attributewise_map[attribute]
            all_cubelet_keys = list(cubelet_map_of_new_records.keys())

            # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX REWRITE THIS!!!
            matching_Welfords_from_MongoDB = mc.fetch_multivariate_matching_welfords(all_cubelet_keys, attribute)
            print(matching_Welfords_from_MongoDB)

            overlapping_keys = []
            for m in matching_Welfords_from_MongoDB:
                overlapping_keys.append(m['cube_key'])

            print(overlapping_keys)

            new_keys = [item for item in all_cubelet_keys if item not in overlapping_keys]

            new_overlapping_aggregates_map = {k: cubelet_map_of_new_records[k] for k in overlapping_keys}
            new_unique_aggregates_map = {k: cubelet_map_of_new_records[k] for k in new_keys}

            print("Overlapping Cubelets:",len(new_overlapping_aggregates_map),"Brand New Cubelets:", len(new_unique_aggregates_map))

            #print(matching_Welfords_from_MongoDB)
            print(len(matching_Welfords_from_MongoDB))

            # Entries with no overlaps pushed directly
            #mc.push_aggregates_mongo(new_unique_aggregates_map, attribute, self.agg_collection_name)

            merged_aggregates_map = self.merge_aggregates(matching_Welfords_from_MongoDB, new_overlapping_aggregates_map)

            print(merged_aggregates_map)

            #mc.push_aggregates_mongo(merged_aggregates_map)


    def create_welford(self, count, m, s):
        w1 = Welford()
        w1.init_from_saved_attrs(float(count), float(m), float(s))
        return w1

    def merge_aggregates(self, matching_entries, new_overlapping_aggregates_map):
        updated_cubelet_map = {}
        for state in matching_entries:
            id = state['_id']
            cube_key = state['cube_key']
            w1 = self.create_welford(state["count"],state["m"],state["s"])
            new_stuff = new_overlapping_aggregates_map[cube_key]
            w1.merge(new_stuff)

            updated_cubelet_map[cube_key] = (w1,id)

        return updated_cubelet_map

if __name__=="__main__":

    collection_name = "aqua_data_backup"
    geo_collection_name = "water_quality_sites"

    cubelet_generator = CubeletGenerator()

    mongo_reader = MongoBufferedReader()
    mongo_reader.initialize_station_locations(geo_collection_name)

    start = 0
    chunk_size = 50000
    while True:
        # READ WATER DATA 50000 ENTRIES AT A TIME
        # COMINED WATER READING WITH CORRDINATES AND RETURNS AS A SINGLE RECORD
        fts = mongo_reader.read_water_data_from_mongo(collection_name, start*chunk_size, chunk_size)

        if len(fts) == 0:
            break
        start+=1

        new_data_attributewise_map = cubelet_generator.group_newdata_by_attribute_key(fts)

        # Aggregates - Cubelets
        cubelet_generator.create_and_merge_local_Cubelets(new_data_attributewise_map)
        cubelet_generator.merge_Cubelets_With_MongoDB()

