import networkx as nx
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import numpy as np
from time import time
import re
from shapely.geometry import Point, MultiPoint

# Set up variables
start_time = time()
g = nx.DiGraph()
carib = '/Users/ricky/Downloads/tdxhydro_streams_70s_northamerica/TDX_streamnet_7020065090_01.gpkg'
australia = "/Users/ricky/Downloads/tdxhydro_streams_50s_australia/TDX_streamnet_5020049720_01.gpkg"
next_down_id_col = 'DSLINKNO'
stream_id_col = 'LINKNO'

network_to_use = australia # Assign
network = gpd.read_file(network_to_use)
print(f"    Read gpkg in {np.round(time()-start_time,2)} secs")

#### DROP ORDER 1  and small catchments, this is for faster testing. REMOVE WHEN IMPLEMENTING WITH RILEY"S PREPROCESSING SCRIPT:
network = network.loc[~((network['DSLINKNO'] == -1) & (network['USLINKNO1'] == -1) | (network['DSContArea'] < 75000000)), :]
########## 

# Use re to get the numbers we want to make the computational id
pattern = re.compile(r'.*streamnet_(\d{10})_0.*') # Get 10 digit hydrobasin id from path
try:
    hydrobasin = pattern.findall(network_to_use)[-1]
except:
    raise ValueError(f"Hydrobasin got is '{pattern.findall(network_to_use)}', which is not valid")
hydrobasin = hydrobasin[0] + '-' + hydrobasin[-5:] + '-' 

# Create network from dataframe
for next_down_id, stream_id in zip(network[next_down_id_col].values, network[stream_id_col].values):
    g.add_edge(next_down_id, stream_id)
        
print(f"    Made networkx in {np.round(time()-start_time,2)} secs")

###### METHOD 1
# compute the x- and y-coordinates of the centroids of each line, average them, sort by the absolute average and proximity of features, 
# leaves us with a geodataframe sorted by geometry
cols = ['geometry', 'LINKNO', 'DSLINKNO']
sorted_net = (
    network.loc[network['DSLINKNO'] == -1, cols]
    .to_crs({'proj':'cea'})
    .assign(**{'x':lambda df: np.abs(df['geometry'].centroid.x), 
                'y':lambda df: np.abs(df['geometry'].centroid.y),
                'rep_val':lambda df: df[['x', 'y']].mean(axis=1),
                'prev':lambda df: df['rep_val'].shift(1),
                'closeness': lambda df: df['rep_val'] - df['prev']}) 
    .sort_values(by=['rep_val','closeness'])
)
print(f"    Sorted gpkg in {np.round(time()-start_time,2)} secs")

# Create a list of outlets, in a sorted manner
outlets = sorted_net['LINKNO']
group_size = 100000
count = 0
computation_id = 0

for outlet in sorted_net['LINKNO']:
    # get a list of upstream streams, including this outlet
    descendants = list(map(int, nx.descendants(g, outlet))) + [outlet]
    # Give all streams that go to this node the same terminal id (the end stream id)
    network.loc[network['LINKNO'].isin(descendants), 'TERMINALID'] = outlet

    # Add to count the amount of features that have the current terminalid
    count += network[network.TERMINALID == outlet].shape[0]
    if count > group_size: # Too many features for current coputational_id - change it
        computation_id += 1
        count = 0
    network.loc[network['TERMINALID'] == outlet, 'computation_id'] = hydrobasin + str(computation_id)

print(f"    Assigned gpkg in {np.round(time()-start_time,2)} secs")
############### METHOD 2
# def get_centroid(group):
#     return group.geometry.centroid

# for outlet in network.loc[network['DSLINKNO'] == -1, 'LINKNO']:
#     # get a list of upstream streams, including this outlet
#     descendants = list(map(int, nx.descendants(g, outlet))) + [outlet]
#     # Give all streams that go to this node the same terminal id (the end stream id)
#     network.loc[network['LINKNO'].isin(descendants), 'TERMINALID'] = outlet


# # Compute the centroids of each group
# centroids = network.to_crs({'proj':'cea'}).groupby('TERMINALID')['geometry'].apply(get_centroid).reset_index()

# # Dissolve the GeoDataFrame to create a single geometry object
# df_new = centroids.dissolve(by='TERMINALID')
# df_new['x'] = df_new['geometry'].apply(
#     lambda geom: geom.centroid.x if isinstance(geom, MultiPoint) else geom.x
# )
# df_new['y'] = df_new['geometry'].apply(
#     lambda geom: geom.centroid.y if isinstance(geom, MultiPoint) else geom.y
# )

# # Add the centroids to the original GeoDataFrame using a join operation
# gdf = network[['LINKNO', 'DSLINKNO','TERMINALID']].join(df_new, on=['TERMINALID'])

# cols = ['LINKNO', 'DSLINKNO','TERMINALID','x','y']
# sorted_net = (
#     gdf.loc[network['DSLINKNO'] == -1, cols + ['geometry']]
#     .assign(**{'rep_val':lambda df: df[['x', 'y']].mean(axis=1),
#                 'prev':lambda df: df['rep_val'].shift(1),
#                 'closeness': lambda df: df['rep_val'] - df['prev']}) 
#     .sort_values(by=['rep_val', 'closeness'])
# )

# print(f"    Sorted gpkg in {np.round(time()-start_time,2)} secs")

# group_size = 100000
# count = 0
# computation_id = 0

# # Add to count the amount of features that have the current terminalid
# for termid in sorted_net['TERMINALID']:
#     count += network[network.TERMINALID == termid].shape[0]
#     if count > group_size: # Too many features for current coputational_id - change it
#         computation_id += 1
#         count = 0
#     network.loc[network['TERMINALID'] == termid, 'computation_id'] = hydrobasin + str(computation_id)

# print(f"    Assigned gpkg in {np.round(time()-start_time,2)} secs")

##################
# Save the updated GeoDataFrame to a new GeoPackage
network.to_file('aus_mthd_1.gpkg', driver='GPKG')
print(f"    Saved gpkg in {np.round(time()-start_time,2)} secs")
