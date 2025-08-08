import pandas as pd

def create_error_file(filename:str):
    """
    Input the error file and create an excel with corrsponding categories (Bike-Path, One-way, Out Calc.)
    """
    
    # File is space seperated into format: 'Discrepancy within 1119796 for Northbound Out direction'
    # Grab index 2, and 4 for ID and direction
    
    
    
    errors = open(filename,'r').readlines()
    ids = [error.split(' ')[2] for error in errors]
    directions = [error.split(' ')[4] for error in errors]
    excel_data = {"ID" : [],"Category" : [],"Link" : []}
    link = "https://datalink.miovision.com/studies/"
    error_mapping : dict[str,list] = {}
    direction_mapping = {
            'Southbound' : 1,
            'Westbound' : 2,
            'Northbound' : 3,
            'Eastbound' : 4
        }
    # Store each id mapping, and the corresponding direction errors
    for i,id in enumerate(ids):
        if id in error_mapping:
            error_mapping[id].append(direction_mapping[directions[i]])
        else:
            error_mapping[id] = [direction_mapping[directions[i]]]
    
    for id in list(error_mapping.keys()):
        excel_data["ID"].append(id)
        excel_data["Link"].append(f'{link}{id}')
        directions = error_mapping[id]
        if len(directions) == 1:
            excel_data["Category"].append("One-way")
        else:
            # Ped way problems are always Southbound, then Northbound, which would be 1 + 2 = 3
            # Or Westbound then Eastbound, which would be 2 + 2 = 4
            if directions[0] + 2 == directions[1]:
                excel_data["Category"].append("Bike-Path")
            else:
                excel_data["Category"].append("Out Calc.")
    
    
    excel_frame = pd.DataFrame(excel_data)
    excel_frame.to_excel('Errors.xlsx',index=False)
   
    
    
    
    

if __name__ == "__main__":
    filename = "./errors1.txt"
    create_error_file(filename=filename)