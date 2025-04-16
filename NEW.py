import pandas as pd
import networkx as nx

# Load data
df = pd.read_parquet("/Users/rachel1/Downloads/model-metadata-table.parquet", engine="pyarrow")

# Identify terminal links
terminal_links = df['TerminalLink'].unique()
terminal_df = df[df['LINKNO'].isin(terminal_links)]

# Filter large terminal links
large_terminal_df = terminal_df[terminal_df['DSContArea'] > 2500000000]
split_terminal_df = large_terminal_df[large_terminal_df['DSContArea'] >= 22500000000]
terminal_links_split = split_terminal_df['LINKNO'].unique()

links_to_split = df[df['TerminalLink'].isin(terminal_links_split)]

# Create directed graph
G = nx.DiGraph()
G.add_edges_from(df[['LINKNO', 'DSLINKNO']].dropna().itertuples(index=False, name=None))

all_top_tributaries = {}


def track_top_tributaries(start_link):
    current_link = start_link
    top_tributaries = {}

    while True:
        upstream_links = df[df['DSLINKNO'] == current_link]['LINKNO'].tolist()
        if len(upstream_links) < 2:
            if not upstream_links:
                break
            current_link = upstream_links[0]
            continue

        upstream_data = links_to_split[links_to_split['LINKNO'].isin(upstream_links)]
        if upstream_data.empty or len(upstream_data) < 2:
            break

        main_branch = upstream_data.loc[upstream_data['DSContArea'].idxmax(), 'LINKNO']
        tributary = upstream_data.loc[upstream_data['DSContArea'].idxmin(), 'LINKNO']

        if len(top_tributaries) < 4 or upstream_data['DSContArea'].min() > min(top_tributaries.values()):
            if len(top_tributaries) == 4:
                min_key = min(top_tributaries, key=top_tributaries.get)
                del top_tributaries[min_key]
            top_tributaries[tributary] = upstream_data['DSContArea'].min()

        current_link = main_branch

    all_top_tributaries[start_link] = pd.DataFrame(
        list(top_tributaries.items()), columns=['LINKNO', 'DSContArea']
    )


# Process terminal links
for link in terminal_links_split:
    print(f"Processing terminal link {link}...")
    track_top_tributaries(link)

# Compile list of all tributaries
all_tributaries = set()
for tributary_df in all_top_tributaries.values():
    all_tributaries.update(tributary_df['LINKNO'].tolist())
all_tributaries_list = sorted(all_tributaries)

# Find associated downstream links
ds_links = links_to_split[links_to_split['LINKNO'].isin(all_tributaries_list)]['DSLINKNO'].unique()
linked_linknos = links_to_split[links_to_split['DSLINKNO'].isin(ds_links)]['LINKNO'].tolist()
combined_linknos = list(set(linked_linknos) | set(terminal_links_split))

# Create subgraph and process topological order
subgraph = G.subgraph(combined_linknos)
filtered_links_to_split = links_to_split[links_to_split['LINKNO'].isin(combined_linknos)]
filtered_links_to_split = filtered_links_to_split.sort_values('TopologicalOrder')
ordered = filtered_links_to_split['LINKNO'].to_list()

downstream_associations = {}
for i, link in enumerate(ordered, 1):
    print(f"Processing link {link}... ({i})")
    upstream_nodes = nx.ancestors(G, link)
    upstream_nodes.add(link)
    for upstream in upstream_nodes:
        if upstream not in downstream_associations:
            downstream_associations[upstream] = link

# Create and merge downstream DataFrame
downstream_df = pd.DataFrame(downstream_associations.items(), columns=['LINKNO', 'Downstream_Link'])
links_to_split = pd.merge(links_to_split, downstream_df, on='LINKNO', how='left')
df = pd.merge(df, downstream_df, on='LINKNO', how='left')
df['Downstream_Link'] = df['Downstream_Link'].fillna(df['TerminalLink'])

# Save results
df.to_csv("/Users/rachel1/Downloads/updated_streams.csv", index=False)
