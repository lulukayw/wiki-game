import gzip
import csv
import re
import os
import sys

# --- CONFIGURATION ---
DUMP_DIR = "./data/"  # Where your .sql.gz files are
OUTPUT_DIR = "./import/" # Where to save CSVs
PAGE_DUMP = "simplewiki-latest-page.sql.gz"
LINKS_DUMP = "simplewiki-latest-pagelinks.sql.gz"
LINKTARGET_DUMP = "simplewiki-latest-linktarget.sql.gz"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Capture ID, Namespace, Title
PAGE_PATTERN = re.compile(r"\((\d+),(\d+),'([^']+)',")
# Capture LinkTarget_ID, Namespace, Title
TARGET_PATTERN = re.compile(r"\((\d+),(\d+),'([^']+)'\)")
# Capture pl_from, pl_target_id or pl_from, pl_namespace, pl_title
LINK_PATTERN = re.compile(r"\((\d+),(?:(?:\d+),)?\s*(\d+)\)")

def create_nodes():
    """
    Step 1: Parse the page dump to create pages.csv
    """
    title_to_id = {} # { Title_String : Page_ID }
    
    try:
        infile = gzip.open(os.path.join(DUMP_DIR, PAGE_DUMP), 'rt', encoding='latin-1', errors='replace')
    except FileNotFoundError:
        print(f"Could not find {PAGE_DUMP} in {DUMP_DIR}")
        sys.exit(1)

    with infile, open(os.path.join(OUTPUT_DIR, "pages.csv"), 'w', newline='', encoding='utf-8') as outfile:
        
        writer = csv.writer(outfile)
        writer.writerow(["pageId:ID", "title", ":LABEL"])
        
        for line in infile:
            if line.startswith("INSERT INTO"):
                for match in PAGE_PATTERN.findall(line):
                    page_id, namespace, title = match
                    if namespace == '0': # Main Articles only (indentifier 0)
                        clean_title = title.replace("\\'", "'")
                        writer.writerow([page_id, clean_title, "Page"])
                        title_to_id[clean_title] = int(page_id)
                
    print(f"\nFinished Pages. Map size: {len(title_to_id)}")
    return title_to_id

def map_targets(title_to_id):
    """
    Step 2: Parse linktarget.sql.gz to create a mapping of LinkTarget_ID -> Real_Page_ID
    """
    target_id_to_page_id = {} # { LinkTarget_ID : Real_Page_ID }
    
    try:
        infile = gzip.open(os.path.join(DUMP_DIR, LINKTARGET_DUMP), 'rt', encoding='latin-1', errors='replace')
    except FileNotFoundError:
        print(f"Could not find {LINKTARGET_DUMP} in {DUMP_DIR}")
        sys.exit(1)

    with infile:
        for line in infile:
            if line.startswith("INSERT INTO"):
                for match in TARGET_PATTERN.findall(line):
                    lt_id, namespace, title = match
                    if namespace == '0':
                        clean_title = title.replace("\\'", "'")
                    
                        # If the LinkTarget title exists in our Page Map, we found a valid connection.
                        if clean_title in title_to_id:
                            real_page_id = title_to_id[clean_title]
                            target_id_to_page_id[int(lt_id)] = real_page_id
            

    print(f"\nFinished Targets. Mapped {len(target_id_to_page_id)} valid link targets.")
    return target_id_to_page_id

def create_relationships(target_map):
    """
    Step 3: Parse pagelinks.sql.gz to create links.csv.
    """
    try:
        infile = gzip.open(os.path.join(DUMP_DIR, LINKS_DUMP), 'rt', encoding='latin-1', errors='replace')
    except FileNotFoundError:
        print(f"Could not find {LINKS_DUMP} in {DUMP_DIR}")
        sys.exit(1)

    with infile, open(os.path.join(OUTPUT_DIR, "links.csv"), 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow([":START_ID", ":END_ID", ":TYPE"])
        valid_links = 0
        
        for line in infile:
            if line.startswith("INSERT INTO"):
                for match in LINK_PATTERN.findall(line):
                    source_id, lt_id = match
                    
                    src = int(source_id)
                    target_lt_id = int(lt_id)
                    
                    # Does this LinkTarget ID point to a known Page ID?
                    if target_lt_id in target_map:
                        final_page_id = target_map[target_lt_id]
                        writer.writerow([src, final_page_id, "LINKS_TO"])
                        valid_links += 1

    print(f"\nFinished. Total Valid Links Written: {valid_links}")

if __name__ == "__main__":
    # 1. Load Pages
    title_map = create_nodes()
    
    # 2. Map Link IDs to Page IDs 
    # (We delete title_map to free up RAM because we don't need it for step 3)
    target_map = map_targets(title_map)
    del title_map 
    
    # 3. Create the final links CSV
    create_relationships(target_map)