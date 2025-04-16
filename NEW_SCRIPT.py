import pandas as pd
import networkx as nx
import geopandas as gpd

parquet_file = "/Users/rachel1/Downloads/model-metadata-table.parquet"
df = pd.read_parquet(parquet_file, engine="pyarrow")

keep = pd.read_json('/Volumes/EB406_T7_2/KEEP_POINTS.geojsonl.json', lines=True)

discard = [
    item['LINKNO']
    for item in keep['properties']
    if item.get('Keep') and item['Keep'].lower() == 'no'
]
trash = pd.read_json('/Users/rachel1/Downloads/trash_linknos.geojsonl.json', lines=True)
trash = [row['properties']['LINKNO'] for _, row in trash.iterrows()]
discard=trash+discard

terminal_links = df['TerminalLink'].unique()
terminal_df = df[df['LINKNO'].isin(terminal_links)]

large_terminal_df = terminal_df[terminal_df['DSContArea'] > 10000000000]
split_terminal_df = terminal_df[terminal_df['DSContArea'] >= 100000000000]
terminal_links_split = split_terminal_df['LINKNO'].unique()
links_to_split = df[df['TerminalLink'].isin(terminal_links_split)]

G = nx.DiGraph()

# Add edges from the DataFrame
G.add_edges_from(df[['LINKNO', 'DSLINKNO']].dropna().itertuples(index=False, name=None))

# Dictionary to store results for each terminal link
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

print(all_tributaries_list)

# Step 1: Find all DSLINKNO values where LINKNO is in all_tributaries_list
ds_links = links_to_split[links_to_split['LINKNO'].isin(all_tributaries_list)]['DSLINKNO'].unique()

# Step 2: Find all LINKNO values that have these DSLINKNO values
linked_linknos = links_to_split[links_to_split['DSLINKNO'].isin(ds_links)]['LINKNO'].tolist()

discard += [
    int(x) for x in [
        730274253.0, 730257615.0, 730266619.0, 730333224.0, 730267899.0,
        730253744.0, 730151278.0, 730171759.0, 730328054.0, 730331941.0,
        730305063.0, 730321655.0, 730395965.0, 730401090.0, 730392287.0,
        730389725.0
    ]
]
#%%
discard += [
    460088975, 460169892, 460308941, 460100748, 460101473, 460113252, 460199335, 460276556, 290814847, 410791924,
    210071877, 210066596, 210244170, 210252559, 210219780, 210249347, 210253615, 210272681, 470249536, 470248419,
    230261738, 230261741, 230297651, 230272638, 230261757, 230287852, 230307442, 230268266, 640411094, 640409940,
    151259095, 151255127, 151269023, 151185699, 151314635, 151330509, 290471619, 290620144, 290482616, 290499145,
    290482632, 290496385, 290492248, 290631152, 291066991, 291054629, 291046371, 291066998, 291058734, 291089014,
    770590963, 770567016, 770556618, 770570200, 770605530, 770446349, 770542057, 770560778, 291075254, 291090390,
    770481700, 770650264, 770483780, 770625284, 770600329, 770486908, 770667944, 770588922, 420809459, 420811507,
    750274424, 750223563, 750276200, 750241320, 750250826, 750241319, 750262044, 750250232, 441124533, 441227735,
    710304866, 710183875, 710279093, 710271149, 710281077, 710227525, 710314796, 710271141, 410526795, 410859509,
    650215746, 650183747, 650123773, 650216546, 650185347, 650126969, 650128573, 650140568, 410600536, 410553430,
    360069329, 360075378, 360046908, 360064977, 360070787, 360056891, 360052729, 360052937, 110819332, 180329864,
    180395145, 180405703, 650266760, 650252393, 650298792, 650265164, 650321158, 650255561, 650305959, 650262765,
    640531664, 640533968, 621087515, 621071008, 640531664, 640533968, 610453760, 610335809, 290604846, 290646136,
    290892379, 290782297, 290604858, 290571824, 290800188, 290787802, 410600536, 410721288, 410553430, 410560588,
    410763274, 290823103, 290853377, 220721559, 220601978, 220743580, 220606106
]
additional_to_split = [
    180329864, 180395145, 180405703, 710194483, 720229576, 720233537, 720197926,
    710938579, 720146346, 720164197, 710914988, 710797929, 710835637,
    541868810, 541699607, 541886341, 440952642, 441167299, 120732725, 120699444,
]

combined_linknos = list(
    set(linked_linknos) |
    set(terminal_links_split) |
    set(large_terminal_df['LINKNO']) |
    set(additional_to_split)
)
filtered_links_to_split = links_to_split[links_to_split['LINKNO'].isin(combined_linknos)]
filtered_links_to_split = filtered_links_to_split.sort_values('TopologicalOrder')
ordered = filtered_links_to_split['LINKNO'].to_list()

filtered_combined_linknos = [ln for ln in combined_linknos if ln not in discard]
filtered_ordered = [ln for ln in ordered if ln not in discard]

subgraph = G.subgraph(filtered_combined_linknos )

# Step 2: Get the topological order for the subgraph
topological_order = list(nx.topological_sort(subgraph))

downstream_associations = {}
i = 0

# Step 3: Process the links in topological order
for link in filtered_ordered:
    i = i + 1
    print(f"Processing link {link}... ({i}/{len(topological_order)})")

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
downstream_df.to_csv('/Users/rachel1/Downloads/downstream_links_10_100_2.csv', index=False)
# Step 5: Merge with links_to_split
links_to_split = pd.merge(links_to_split, downstream_df, on='LINKNO', how='left')

links_to_split = links_to_split[['LINKNO', 'Downstream_Link']]

links_to_split.to_csv('/Users/rachel1/Downloads/new_links22.csv', index=False)