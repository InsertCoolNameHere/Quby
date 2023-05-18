
from aggregates.welford import Welford
import numpy as np
from ingestion.et_reader import ETReader
from mongo_utils.mongo_communications import Mongo_Communicator

# Reads a CSV File, inserts it into a collection
# Creates Welford aggregates and merges them with the existing ones in the DB
class Tagger():
    collection_name= "synthetic_ET"
    agg_collection_name= "synthetic_ET_agg"
    def ingest_data_and_Cubelets(self, fts, new_aggregates_map):
        mc = Mongo_Communicator()
        cubelet_map = {}

        for k in new_aggregates_map.keys():
            vals = new_aggregates_map[k]
            w = Welford()
            w.add_all(np.array(vals))
            cubelet_map[k] = w
            # print(k,"===",w)

        all_cubelet_keys = list(cubelet_map.keys())

        matching_entries = mc.fetch_matching_welfords(all_cubelet_keys)
        print(matching_entries)

        overlapping_keys = []
        for m in matching_entries:
            overlapping_keys.append(m['cube_key'])

        print(overlapping_keys)

        new_keys = [item for item in all_cubelet_keys if item not in overlapping_keys]

        new_overlapping_aggregates_map = {k: cubelet_map[k] for k in overlapping_keys}
        new_unique_aggregates_map = {k: cubelet_map[k] for k in new_keys}

        print(len(overlapping_keys), len(new_keys), len(new_overlapping_aggregates_map), len(new_unique_aggregates_map))

        print(matching_entries)
        print(len(matching_entries))

        # All new point data to be ingested
        mc.push_into_mongo(fts, self.collection_name)
        # Entries with no overlaps pushed directly
        mc.push_aggregates_mongo(new_unique_aggregates_map, self.agg_collection_name)

        merged_aggregates_map = self.merge_aggregates(matching_entries, new_overlapping_aggregates_map)

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
    tagger = Tagger()

    et_reader = ETReader()
    fts, new_data_map = et_reader.read_csv_data("../synthetic_data/sample_dump.csv")
    # Aggregates - Cubelets

    tagger.ingest_data_and_Cubelets(fts, new_data_map)

