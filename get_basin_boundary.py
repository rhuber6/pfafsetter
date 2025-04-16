import geopandas as gpd
import pandas as pd
import networkx as nx
from shapely.ops import unary_union
from shapely.geometry import Point
# Read input data
#points = gpd.read_file("/Users/rachel1/Downloads/filtered_GLOBAL_points_2000_withVPU.gpkg")
points = pd.read_csv("/Users/rachel1/Downloads/101 vpy area test.csv")
# Ensure latitude and longitude columns exist
if 'latitude' in points.columns and 'longitude' in points.columns:
    # Create geometry column
    points['geometry'] = points.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)

    # Convert to GeoDataFrame
    points = gpd.GeoDataFrame(points, geometry='geometry', crs="EPSG:4326")

    print("GeoDataFrame created successfully!")
else:
    print("Error: CSV must contain 'latitude' and 'longitude' columns.")
parquet_file = "/Users/rachel1/Downloads/model-metadata-table.parquet"
df = pd.read_parquet(parquet_file, engine="pyarrow")

# Create directed graph
G = nx.DiGraph()
edges = df[['LINKNO', 'DSLINKNO']].dropna().itertuples(index=False, name=None)
G.add_edges_from(edges)

# Group by VPUCode to create dictionary of LINKNOs
vpu_linkno_dict = points.groupby("VPUCode")["LINKNO"].apply(list).to_dict()
print(vpu_linkno_dict)

combined_features = []  # List to store results

for vpu, linkno_list in vpu_linkno_dict.items():
    print(vpu)
    # Define the path to the SpatiaLite database
    database_path = f'/Volumes/EB406_T7_2/source_catchments/catchments_{vpu}.spatialite'

    try:
        # Read the catchment data
        catchment_gdf = gpd.read_file(database_path)

        for linkno in linkno_list:
            # Find all upstream nodes for this linkno
            related_nodes = nx.ancestors(G, linkno)

            # Filter catchments to include only related upstream nodes
            related_rows = catchment_gdf[catchment_gdf['linkno'].isin(related_nodes)]

            # Merge with streams to add additional attributes
            related_rows = related_rows.merge(df[['LINKNO', 'USContArea', 'DSContArea', 'musk_k']],
                                              left_on='linkno', right_on='LINKNO', how='left')

            # Compute basin area
            related_rows['BasinArea'] = related_rows['DSContArea'] - related_rows['USContArea']

            # Combine geometries using unary_union
            combined_geometry = unary_union(related_rows.geometry)

            # Store the combined attributes for this LINKNO
            combined_attributes = {
                'LINKNO': linkno,
                'geometry': combined_geometry,
                'area': related_rows['BasinArea'].sum()
            }
            combined_features.append(combined_attributes)

    except Exception as e:
        print(f"Error processing VPU {vpu}: {e}")

# Create a new GeoDataFrame with the combined features
combined_gdf = gpd.GeoDataFrame(combined_features, crs=catchment_gdf.crs)
combined_gdf.to_file("/Users/rachel1/Downloads/YUBIN.gpkg", driver="GPKG")

# Save or process the combined_gdf further if needed

