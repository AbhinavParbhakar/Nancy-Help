import os
import pandas as pd
import time

class ColumnNames:
    def __init__(self) -> None:
        self.file_names = self.dfs_wrapper()
        self.column_names = self.get_column_names(self.file_names)
    
    def get_cols(self)->list[str]:
        return self.column_names
    
    def extract_names(self,names:dict[str,bool],file:str)->None:
        sheet_name = "Total Volume Class Breakdown"
        frame = pd.read_excel(io=file,sheet_name=[sheet_name],engine='openpyxl')
        df = frame["Total Volume Class Breakdown"]
        
        columns_index = df.index[df['Leg'] == '% Total'].tolist()[0]
        area_interest = frame[sheet_name].iloc[columns_index + 1:]
        
        
        for i in range(area_interest.__len__()):
            if i % 2 == 0:
                label = area_interest['Leg'].iloc[i]
                try:
                    found = names[label]
                except:
                    names[label] = True    
        

    def get_column_names(self,files:list[str])->list[str]:
        distinct_columns = {}
        for file in files:
            try:
                self.extract_names(distinct_columns,file=file)
            except Exception as e:
                print(e.args)
                print(f'{file} caused a problem')
            
        return list(distinct_columns.keys())
        
    def check_duplicates(self,data:list):
        duplicatates = {}
        found_html = False
        
        for datum in data:
            try:
                found = duplicatates[datum]
                found_html = True
            except:
                duplicatates[datum] = True
            
        return found_html

    def dfs_wrapper(self)->None:
        locations = ['./2022','./2023']
        file_names = []
        
        for i in range(len(locations)):
            for root,subs,files in os.walk(locations[i]):
                updated_files = [root.replace('\\','/') + '/' + file for file in files]
                file_names.extend(updated_files)
                
                
        return file_names
if __name__ == "__main__":
    cl = ColumnNames()
    