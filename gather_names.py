import os

def dfs()->None:
    pass
    
def check_duplicates(data:list):
    duplicatates = {}
    found_html = False
    
    for datum in data:
        try:
            found = duplicatates[datum]
            found_html = True
        except:
            duplicatates[datum] = True
        
    return found_html

def dfs_wrapper()->None:
    locations = ['./2022','./2023']
    file_names = []
    
    for i in range(len(locations)):
        for root,subs,files in os.walk(locations[i]):
            file_names.extend(files)
    
    print(check_duplicates(file_names))

if __name__ == "__main__":
    dfs_wrapper()