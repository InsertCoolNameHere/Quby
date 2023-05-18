from mongo_utils.mongo_communications import Mongo_Communicator
from shapely.geometry import shape
from utilz.load_config import load_from_yaml
import pickle

params = load_from_yaml("../metadata/configs.yaml")
state_len = params.state_len
county_len = params.county_len
tract_len = params.tract_len

state_collection = "state_geo"
county_collection = "county_geo"
tract_collection = "tract_geo"

state_shapes = {}
county_shapes = {}
tract_shapes = {}

state_to_county_code = {}
county_to_tract_code = {}
state_to_tract_code = {}

all_shapes_map = {"state": state_shapes, "county": county_shapes, "tract": tract_shapes, "s2c": state_to_county_code, "c2t": county_to_tract_code, "s2t": state_to_tract_code}
final_hierarchy_file = "./fips_hierarchy.pkl"

mc = Mongo_Communicator()

def fetch_all_US_states():
    features = mc.fetch_geojsons(state_collection)
    for ft in features:
        poly_obj = shape(ft['geometry'])
        key = ft['GISJOIN']
        state_shapes[key] = poly_obj

    print(len(state_shapes))

def fetch_all_US_counties():
    features = mc.fetch_geojsons(county_collection)
    for ft in features:
        poly_obj = shape(ft['geometry'])
        gis_code = ft['GISJOIN']
        county_shapes[gis_code] = poly_obj

        # MAPPING TO STATE
        state_id = gis_code[0:state_len]
        if state_id in state_shapes:
            counties = []
            if state_id in state_to_county_code:
                counties = state_to_county_code[state_id]

            counties.append(gis_code)
            state_to_county_code[state_id] = counties
        else:
            print("1. HOLD ON....WAIT A MINUTE....SOMETHING AINT RIGHT!", gis_code, state_id)

    print(state_to_county_code)

def fetch_all_US_census_tracts():
    features = mc.fetch_geojsons(tract_collection)
    for ft in features:
        poly_obj = shape(ft['geometry'])
        gis_code = ft['GISJOIN']
        tract_shapes[gis_code] = poly_obj

        # MAP TO STATE
        state_id = gis_code[0:state_len]
        if state_id in state_shapes:
            tracts = []
            if state_id in state_to_tract_code:
                tracts = state_to_tract_code[state_id]
            tracts.append(gis_code)
            state_to_tract_code[state_id] = tracts
        else:
            print("2. HOLD ON....WAIT A MINUTE....SOMETHING AINT RIGHT!", gis_code, state_id)

        # MAP TO COUNTY
        county_id = gis_code[0:county_len]
        if county_id in county_shapes:
            tracts = []
            if county_id in county_to_tract_code:
                tracts = county_to_tract_code[county_id]
            tracts.append(gis_code)
            county_to_tract_code[county_id] = tracts
        else:
            print("3. HOLD ON....WAIT A MINUTE....SOMETHING AINT RIGHT!", gis_code, county_id)

    #print(county_to_tract_code)
    #print(state_to_tract_code)

def compile_shape_hierarchy():
    fetch_all_US_states()
    fetch_all_US_counties()
    fetch_all_US_census_tracts()


if __name__ == "__main__":
    compile_shape_hierarchy()

    print(len(all_shapes_map['s2c']))
    print(len(all_shapes_map['c2t']))
    print(len(all_shapes_map['s2t']))
    print(len(state_shapes))
    print(len(county_shapes))
    print(len(tract_shapes))

    with open(final_hierarchy_file, 'wb') as handle:
        pickle.dump(all_shapes_map, handle, protocol=pickle.HIGHEST_PROTOCOL)





