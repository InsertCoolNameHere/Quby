def get_blank_feature_json(lat, lon):
    ft_dict = {"type": "Feature"}
    geom_dict = {"type": "Point", "coordinates": [lon, lat]}
    ft_dict["geometry"] = geom_dict
    ft_dict["properties"] = {}

def get_welford_json(k,M,S):
    welford_dict = {}
    welford_dict['k'] = k
    welford_dict['M'] = M
    welford_dict['S'] = S

    return welford_dict
