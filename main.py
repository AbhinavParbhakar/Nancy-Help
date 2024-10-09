
import os
import pandas as pd
from gather_names import ColumnNames

class ParseInfo:
    def __init__(self,extra_cols=[]) -> None:
        self.columns = ['Id', 'Date', 'Lat', 'Long', 'Volume East', 'Volume West', 'Volume North', 'Volume South', 'Total Volume', 'Adj. Volume']
        self.columns.extend(extra_cols)
        self.main_frame = pd.DataFrame(self.columns)
        
    def parse_file(self,file:str)->None:
        sheets = ["Summary","Total Volume Class Breakdown"]
        file_breakdown = file.split('/')
        file_id = file_breakdown[-1].replace('.xlsx','')
        data : dict[str,pd.DataFrame] = pd.read_excel(io=file,sheet_name=sheets)
        summary = data[sheets[0]]
        total = data[sheets[1]]
        
        # get id and date from file name
        data = {'Id':file_id}
        data['Date'] = file_breakdown[1] + '/' + file_breakdown[2] + '/' + file_breakdown[3]
        
        # get lat-long
        lat_long_index = summary.index[summary[summary.columns[0]] == 'Latitude and Longitude'].tolist()[0]
        lat_long = summary.iloc[lat_long_index][summary.columns[1]].split(',')
        data['Lat'] =  float(lat_long[0])
        data['Long'] = float(lat_long[1])
             
        # get directional data
        print(total.head(n=20))
        
        # print(data['Summary'][data['Summary'].columns[1]].iloc[8])

if __name__ == "__main__":
    cols = ColumnNames()
    pi = ParseInfo(cols.get_cols())
    pi.parse_file('./2023/12/13/1148981.xlsx')


