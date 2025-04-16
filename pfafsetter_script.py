import pandas as pd
import networkx as nx
import geopandas as gpd

parquet_file = "/Users/rachel1/Downloads/model-metadata-table.parquet"
df = pd.read_parquet(parquet_file, engine="pyarrow")

terminal_links = df['TerminalLink'].unique()

terminal_df = df[df['LINKNO'].isin(terminal_links)]

large_terminal_df = terminal_df[terminal_df['DSContArea'] > 60000000000]

split_terminal_df = terminal_df[terminal_df['DSContArea'] >= 120000000000]

terminal_links_split = split_terminal_df['LINKNO'].unique()

links_to_split = df[df['TerminalLink'].isin(terminal_links_split)]
# Create a directed graph
G = nx.DiGraph()

# Add edges from the DataFrame
G.add_edges_from(df[['LINKNO', 'DSLINKNO']].dropna().itertuples(index=False, name=None))

all_top_tributaries = {}

for start_link in terminal_links_split:
    print(f"Processing terminal link {start_link}...")
    current_link = start_link
    top_tributaries = {}  # Reset for each terminal link

    while True:
        # Find all upstream links that flow into the current link
        upstream_links = df[df['DSLINKNO'] == current_link]['LINKNO'].tolist()

        # If there's no merging (only one upstream), continue upstream
        if len(upstream_links) < 2:
            if not upstream_links:  # No more upstream nodes
                break
            current_link = upstream_links[0]
            continue

        # Get DSContArea for each upstream link
        upstream_data = links_to_split[links_to_split['LINKNO'].isin(upstream_links)]

        # If DSContArea is missing for any, continue safely
        if upstream_data.empty or len(upstream_data) < 2:
            break

        # Identify main branch and tributary
        main_branch = upstream_data.loc[upstream_data['DSContArea'].idxmax(), 'LINKNO']
        tributary = upstream_data.loc[upstream_data['DSContArea'].idxmin(), 'LINKNO']

        # Check if this tributary is among the top 4 largest encountered for this start_link
        if len(top_tributaries) < 4 or upstream_data['DSContArea'].min() > min(top_tributaries.values()):
            if len(top_tributaries) == 4:
                # Remove the smallest tributary to maintain top 4
                min_key = min(top_tributaries, key=top_tributaries.get)
                del top_tributaries[min_key]
            top_tributaries[tributary] = upstream_data['DSContArea'].min()

        # Continue upstream with the main branch
        current_link = main_branch

    # Store the results for this terminal link
    all_top_tributaries[start_link] = pd.DataFrame(
        list(top_tributaries.items()), columns=['LINKNO', 'DSContArea']
    )

# Get a unique list of all top tributaries
all_tributaries = set()

for df in all_top_tributaries.values():
    all_tributaries.update(df['LINKNO'].tolist())

# Convert to a sorted list (optional)
all_tributaries_list = sorted(all_tributaries)

# Step 1: Find all DSLINKNO values where LINKNO is in all_tributaries_list
ds_links = links_to_split[links_to_split['LINKNO'].isin(all_tributaries_list)]['DSLINKNO'].unique()

# Step 2: Find all LINKNO values that have these DSLINKNO values
linked_linknos = links_to_split[links_to_split['DSLINKNO'].isin(ds_links)]['LINKNO'].tolist()

combined_linknos = list(set(linked_linknos) | set(terminal_links_split))


subgraph = G.subgraph(combined_linknos)

filtered_links_to_split = links_to_split[links_to_split['LINKNO'].isin(combined_linknos)]
filtered_links_to_split = filtered_links_to_split.sort_values('TopologicalOrder')
ordered = filtered_links_to_split['LINKNO'].to_list()
downstream_associations = {}
i = 0

# Step 3: Process the links in topological order
for link in ordered:
    i = i + 1
    print(f"Processing link {link}... ({i})")

    # Find all upstream nodes (including the node itself)
    upstream_nodes = nx.ancestors(G, link)
    upstream_nodes.add(link)  # Include the current link itself

    # For each upstream link, check if it's associated with any downstream link
    for upstream in upstream_nodes:
        if upstream not in downstream_associations:
            # If not already associated, assign the current downstream link
            downstream_associations[upstream] = link

# Step 4: Create a DataFrame from the associations
downstream_df = pd.DataFrame(downstream_associations.items(), columns=['LINKNO', 'Downstream_Link'])

# Step 5: Merge with links_to_split
links_to_split = pd.merge(links_to_split, downstream_df, on='LINKNO', how='left')

df = pd.merge(df, downstream_df, on='LINKNO', how='left')
df['Downstream_Link'] = df['Downstream_Link'].fillna(df['TerminalLink'])

#global_streams = gpd.read_file('/Users/rachel1/Downloads/global_streams_simplified (1).gpkg')
#global_streams_link = pd.merge(global_streams, downstream_df, on='LINKNO', how='left')

#global_streams_link.to_file('/Users/rachel1/Downloads/global_streams_small_links.gpkg', driver='GPKG')