import pymongo
import urllib.parse
from utilz.load_config import load_from_yaml

cube_key_field = "cube_key"
collection_field = "collection"
class Mongo_Communicator():

    def __init__(self):
        params = load_from_yaml("../metadata/configs.yaml")

        username = urllib.parse.quote_plus(params.username)
        password = urllib.parse.quote_plus(params.password)

        mongo_url = 'mongodb://%s:%s@lattice-100:27018/' % (username, password)

        collection_name = params.collection_name
        agg_collection_name = params.agg_collection_name

        self.agg_collection_name = agg_collection_name
        self.collection_name = collection_name

        mongo_db_name = params.mongo_db_name

        sustainclient = pymongo.MongoClient(mongo_url)
        sustain_db = sustainclient[mongo_db_name]
        self.sustain_db = sustain_db
        self.act_collection = sustain_db[collection_name]

        self.agg_collection = sustain_db[agg_collection_name]

        self.sustain_db_map = {collection_name: self.act_collection, agg_collection_name: self.agg_collection}

    def convert_range_to_polygon(self, lats, lons):
        lat1 = lats[0]
        lat2 = lats[1]
        lon1 = lons[0]
        lon2 = lons[1]

        op_str = "[["+str(lon1)+"," +str(lat2)+"], ["+str(lon2)+", "+str(lat2)+"], ["+str(lon2)+", "+str(lat1)+"], ["+str(lon1)+", "+str(lat1)+"], ["+str(lon1)+", "+str(lat2)+"]]"
        return op_str

    def fetch_matching_welfords(self, key_array, agg_collection_name=None):
        if not agg_collection_name:
            agg_collection_name = self.agg_collection_name
        db_agg_sus = self.sustain_db_map[agg_collection_name]
        key_query = {cube_key_field: {"$in": key_array}}
        query_results = list(db_agg_sus.find(key_query))
        #print(query_results)
        return query_results

    # Fetch Welfords for multiple attributes
    def fetch_multivariate_matching_welfords(self, all_cubelet_keys, attribute, agg_collection_name=None):
        if not agg_collection_name:
            agg_collection_name = self.agg_collection_name

        key_query = {cube_key_field: {"$in": all_cubelet_keys}, collection_field: attribute}

        db_agg_sus = self.sustain_db_map[agg_collection_name]
        query_results = list(db_agg_sus.find(key_query))
        # print(query_results)
        return query_results

    def dump_state_dict(self, wf, k, attribute):
        count, m, s = wf.dump_state()
        state = {}
        state[cube_key_field] = k
        state["attribute"] = attribute
        state["collection"] = self.agg_collection_name
        state["count"] = count
        state["m"] = m
        state["s"] = s
        #print(state)
        return state

    def push_aggregates_mongo(self, aggregate_map, attribute, agg_collection_name = None):
        if not agg_collection_name:
            agg_collection_name = self.agg_collection_name
        aggregate_features = []
        for k in aggregate_map.keys():
            wf = aggregate_map[k]
            agg_ft = self.dump_state_dict(wf, k, attribute)
            aggregate_features.append(agg_ft)

        if len(aggregate_features) > 0:
            self.push_into_mongo(aggregate_features, agg_collection_name)

    def push_into_mongo(self, features, collection_name=None):
        if not collection_name:
            collection_name = self.collection_name
        db_sus = self.sustain_db_map[collection_name]
        db_sus.insert_many(features)

    def fetch_geojsons(self, collection_name):
        collection = self.sustain_db[collection_name]
        key_query = {}
        query_results = list(collection.find(key_query))
        return query_results






if __name__ == "__main__":
    key_array = ['023101001X2023-04-23', '023101012X2023-04-23']

    mc = Mongo_Communicator()
    mc.fetch_matching_welfords(key_array)