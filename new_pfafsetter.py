import pandas as pd
import os
import networkx as nx

def process_stream_network(df, output_dir="stream_network_iterations", max_iterations=5):
    """Automates stream network processing while tracking top 4 largest tributaries, updating DSContArea,
    and ensuring downstream associations follow topological order."""
    iteration = 0
    os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists

    while iteration < max_iterations:
        print(f"\nIteration {iteration + 1}:")
        new_downstream_col = f"Downstream_Link_Iteration_{iteration+1}"
        new_area_col = f"Adjusted_DSContArea_Iteration_{iteration+1}"

        terminal_links = df['TerminalLink'].unique()
        large_terminal_df = df[(df['LINKNO'].isin(terminal_links)) & (df['DSContArea'] > 2500000000)]
        split_terminal_df = large_terminal_df[large_terminal_df['DSContArea'] >= 22500000000]
        terminal_links_split = split_terminal_df['LINKNO'].unique()

        downstream_associations = {}
        adjusted_area = {}
        all_top_tributaries = {}

        for start_link in terminal_links_split:
            print(f"  Processing terminal link {start_link}...")
            current_link = start_link
            top_tributaries = {}

            while True:
                upstream_links = df[df['DSLINKNO'] == current_link]['LINKNO'].tolist()
                if len(upstream_links) < 2:  # If no merging, continue upstream
                    if not upstream_links:  # Stop if no more upstream nodes
                        break
                    current_link = upstream_links[0]
                    continue

                upstream_data = df[df['LINKNO'].isin(upstream_links)]
                if upstream_data.empty or len(upstream_data) < 2:
                    break

                # Identify main branch and smallest tributary
                main_branch = upstream_data.loc[upstream_data['DSContArea'].idxmax(), 'LINKNO']
                tributary = upstream_data.loc[upstream_data['DSContArea'].idxmin(), 'LINKNO']

                # Track the top 4 largest tributaries
                if len(top_tributaries) < 4 or upstream_data['DSContArea'].min() > min(top_tributaries.values()):
                    if len(top_tributaries) == 4:
                        min_key = min(top_tributaries, key=top_tributaries.get)
                        del top_tributaries[min_key]
                    top_tributaries[tributary] = upstream_data['DSContArea'].min()

                # Continue upstream with the main branch
                current_link = main_branch

            # Store the top tributaries found
            all_top_tributaries[start_link] = pd.DataFrame(
                list(top_tributaries.items()), columns=['LINKNO', 'DSContArea']
            )

            # Compute adjusted DSContArea for the terminal link
            original_area = df.loc[df['LINKNO'] == start_link, 'DSContArea'].values[0]
            new_area = original_area - sum(top_tributaries.values())
            new_area = max(new_area, 0)  # Ensure non-negative values
            adjusted_area[start_link] = new_area

            # Assign new downstream associations
            for trib in top_tributaries:
                downstream_associations[trib] = start_link

        # Step 1: Get a unique list of all top tributaries
        all_tributaries = set()
        for tributary_df in all_top_tributaries.values():
            all_tributaries.update(tributary_df['LINKNO'].tolist())
        all_tributaries_list = sorted(all_tributaries)

        # Step 2: Find DSLINKNO values where LINKNO is in all_tributaries_list
        ds_links = df[df['LINKNO'].isin(all_tributaries_list)]['DSLINKNO'].unique()

        # Step 3: Find all LINKNO values associated with these DSLINKNO values
        linked_linknos = df[df['DSLINKNO'].isin(ds_links)]['LINKNO'].tolist()

        combined_linknos = list(set(linked_linknos) | set(terminal_links_split))
        filtered_links_to_split = df[df['LINKNO'].isin(combined_linknos)].sort_values('TopologicalOrder')
        ordered = filtered_links_to_split['LINKNO'].tolist()

        # Step 4: Create a directed graph for upstream tracking
        G = nx.DiGraph()
        for _, row in df.iterrows():
            G.add_edge(row['LINKNO'], row['DSLINKNO'])

        # Step 5: Process links in topological order and assign downstream links
        downstream_associations = {}
        for i, link in enumerate(ordered):
            print(f"Processing link {link}... ({i+1}/{len(ordered)})")

            # Find all upstream nodes including the node itself
            upstream_nodes = nx.ancestors(G, link)
            upstream_nodes.add(link)

            # Assign downstream links
            for upstream in upstream_nodes:
                if upstream not in downstream_associations:
                    downstream_associations[upstream] = link

        # Step 6: Create a DataFrame from the associations and merge back into df
        downstream_df = pd.DataFrame(downstream_associations.items(), columns=['LINKNO', new_downstream_col])
        area_df = pd.DataFrame(adjusted_area.items(), columns=['LINKNO', new_area_col])

        df = pd.merge(df, downstream_df, on='LINKNO', how='left')
        df = pd.merge(df, area_df, on='LINKNO', how='left')

        df[new_downstream_col] = df[new_downstream_col].fillna(df['TerminalLink'])
        df['TerminalLink'] = df[new_downstream_col]
        df['DSContArea'] = df[new_area_col]

        output_file = os.path.join(output_dir, f"/Users/rachel1/Downloads/stream_network_iteration_{iteration+1}.csv")
        df.to_csv(output_file, index=False)
        print(f"  Saved: {output_file}")

        iteration += 1

        if df[new_downstream_col].equals(df['TerminalLink']):
            print("No further changes detected. Stopping iterations.")
            break

    return df

# Load your Parquet file
df = pd.read_parquet("/Users/rachel1/Downloads/model-metadata-table.parquet", engine="pyarrow")

# Run the automated process
df_final = process_stream_network(df, max_iterations=5)

# Save the final result
df_final.to_csv("/Users/rachel1/Downloads/processed_stream_network_final.csv", index=False)
print("\nFinal processed file saved as 'processed_stream_network_final.csv'.")
