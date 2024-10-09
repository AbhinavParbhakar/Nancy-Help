
import os
import pandas as pd

class ParseInfo:
    def __init__(self) -> None:
        self.main_frame = pd.DataFrame(columns=['ID','DATE' ,'LAT','LONG','VOLUME EAST','VOLUME WEST','VOLUME NORTH','VOLUME SOUTH','TOTAL VOLUME','ADJ. VOLUME'])
        
    def parse_file(self,file:str)->None:
        data : dict[str,pd.DataFrame] = pd.read_excel(io=file,sheet_name=["Summary"])
    
        print(data['Summary'][data['Summary'].columns[1]].iloc[8])

if __name__ == "__main__":
    pi = ParseInfo()
    
    pi.parse_file('./2023/12/13/1148981.xlsx')

