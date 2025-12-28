import rustworkx as rx
import csv

PAGES_PATH = 'import/pages.csv'
LINKS_PATH = 'import/links.csv'

def load_wiki_graph(pages_path, links_path):
    # Initialize a Directed Graph
    # multigraph=False prevents duplicate links
    graph = rx.PyDiGraph(multigraph=False)
    
    # Mapping: { original_page_id : rustworkx_internal_index }
    id_map = {}
    # Mapping: { rustworkx_internal_index : title }
    title_map = {}
    # Mapping: { title : rustworkx_internal_index } for djikstra
    title_to_idx = {}

    with open(pages_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            p_id = row['pageId:ID']
            title = row['title']
            
            # Add node to graph and store its internal index
            # We store the title as the 'weight' of the node
            idx = graph.add_node(title)
            id_map[p_id] = idx
            title_map[idx] = title
            title_to_idx[title] = idx
    
    # Prepare a list of edges to add in bulk (much faster than one-by-one)
    edges_to_add = []
    with open(links_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            src = row[':START_ID']
            dst = row[':END_ID']
            
            # Check if both IDs exist in our page map (filters out broken links)
            if src in id_map and dst in id_map:
                edges_to_add.append((id_map[src], id_map[dst], None))

    # Bulk add edges
    graph.add_edges_from(edges_to_add)
    print(f"Graph ready with {graph.num_edges()} edges.")
    
    return graph, title_to_idx

def shortest_path(graph, start_title, end_title, title_to_idx):
    # Find the internal indices for the start and end titles
    start_idx = title_to_idx.get(start_title)
    end_idx = title_to_idx.get(end_title)
    
    if start_idx is None or end_idx is None:
        print(start_idx, " ", end_idx)
        print("One or both titles not found in the graph.")
        return None
    
    # Compute shortest path using Dijkstra's algorithm
    paths_dict = rx.digraph_dijkstra_shortest_paths(graph, start_idx, end_idx)
    
    if end_idx not in paths_dict:
        return None
    
    path_indices = paths_dict[end_idx]
    
    # Convert indices back to titles
    return [graph[node_idx] for node_idx in path_indices]


if __name__ == "__main__":
    print("Loading graph...")
    g, title_to_idx = load_wiki_graph(PAGES_PATH, LINKS_PATH)
    while (True):
        print("Enter start and end article titles, separated by a space and spaces replaced with underscores: ")
        # path = shortest_path(g, "Crater", "Isaac_Newton", title_to_idx)
        user_input = input().strip()
        start_title, end_title = user_input.split(" ")
        path = shortest_path(g, start_title, end_title, title_to_idx)

        if path:
            print(" -> ".join(path))
        else:
            print("No path found!")
        
        print("Continue? (y/n): ")
        continue_input = input().strip().lower()
        if continue_input != 'y':
            break