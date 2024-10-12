
import os
import pandas as pd
from gather_names import ColumnNames
from datetime import datetime

class ParseInfo:
    def __init__(self,extra_cols=[]) -> None:
        self.columns = ['Id', 'Date','Time (hrs)', 'Lat', 'Long', 'Road Segment Type']
        self.directions = ['Southbound', 'Westbound', 'Northbound', 'Eastbound']
        final_col = 'Int. Total'
        
        for direction in self.directions:
            self.columns.append(direction)
            self.columns.extend([f'{direction[0]} {ec}' for ec in extra_cols])
        
        self.columns.append(final_col)
        self.columns.extend(extra_cols)
        self.files_to_delete = []
        self.main_frame = pd.DataFrame(columns=self.columns)
    
    def reformat_dict(self,data_dict:dict)->dict:
        """
        Takes in a dict, and then returns the same dict with the values now being inside of lists and with the 
        """
        keys = list(data_dict.keys())
        new_dict = {}
        for key in keys:
            new_dict[key] = [data_dict[key]]
        
        return new_dict
    
    def create_aggregate(self,files:list[str])->None:
        """
        Input a list of files and aggregate information inside.
        Creates an excel file as the output
        """
        excel_file = './Miovision Aggregate Data.xlsx'
        for file in files:
            return_data = self.parse_file(file)
            if return_data:
                new_frame = pd.DataFrame(self.reformat_dict(return_data))
                self.main_frame = pd.concat([self.main_frame,new_frame],ignore_index=True)
            
        self.main_frame.to_excel(excel_file,index=False)
    
    def parse_file(self,file:str)->dict:
        """
        Parse files and return dict with aggregated information or return None if not enough hours in the study
        """
        sheets = ["Summary","Total Volume Class Breakdown"]
        file_breakdown = file.split('/')
        file_id = file_breakdown[-1].replace('.xlsx','')
        data : dict[str,pd.DataFrame] = pd.read_excel(io=file,sheet_name=sheets)
        summary = data[sheets[0]]
        total = data[sheets[1]]
        
        # get duration of the study, if shorter than 24 hours, toss the study
        summary_col_1 = summary.columns[0]
        summary_col_2 = summary.columns[1]
        start_time_index  = summary.index[summary[summary_col_1] == 'Start Time'].tolist()[0]
        end_time_index  = summary.index[summary[summary_col_1] == 'End Time'].tolist()[0]
        
        start_date_time : datetime= summary.iloc[start_time_index][summary_col_2]
        end_date_time : datetime= summary.iloc[end_time_index][summary_col_2]
        
        duration = (end_date_time - start_date_time).total_seconds()
        one_day = 24 * 60 * 60
        one_hour = 60*60
        difference = duration - one_day

        if difference >= 0.0:
            # get id and date from file name
            sheet_data = {'Id' : file_id}
            sheet_data['Date'] = file_breakdown[1] + '/' + file_breakdown[2] + '/' + file_breakdown[3]
            sheet_data['Time (hrs)'] = duration/one_hour
            
            # get lat-long
            lat_long_index = summary.index[summary[summary_col_1] == 'Latitude and Longitude'].tolist()[0]
            lat_long = summary.iloc[lat_long_index][summary_col_2].split(',')
            sheet_data['Lat'] =  float(lat_long[0])
            sheet_data['Long'] = float(lat_long[1])
            
            # classify as midblock or intersection
            self.get_road_type(sheet_data,total)
                
            # get directional data
            grand_total_index = self.get_directiontial_data(sheet_data,total)
            
            # add the int total, assume that it is in the last column
            grand_total = int(total.iloc[grand_total_index][total.columns[-1]].iloc[0])
            sheet_data['Int. Total'] = grand_total
            
            # extract categories
            self.extract_attributes(sheet_data,total)
            
            return sheet_data
        else:
            self.files_to_delete.append(file)
            return None
    
    def get_road_type(self,data_dict:dict,total:pd.DataFrame):
        """
        Classifies file as intersection or midblock
        """
        directions = {
            'North':True,
            'East':True,
            'West':True,
            'South':True
        }
        
        
        cols = total.columns.tolist()
        column_name = "Road Segment Type"
        
        i = 0
        midblock = True
        while i < len(cols) and midblock:
            try:
                found = directions[cols[i]]
                midblock = False
            except:
                midblock = True
            i += 1
        
        if midblock:
            data_dict[column_name] = "Midblock"
        else:
            data_dict[column_name] = "Intersection"
            
    
    def get_directiontial_data(self,data_dict:dict,total:pd.DataFrame):
        """
        Add the directional data to the row, and return the row index for the grand total
        """
        directions = {
            'Northbound':True,
            'Eastbound':True,
            'Westbound':True,
            'Southbound':True
        }
        cols = total.columns.tolist()
        directions_present = []
        directions_total = []
        app_totals_index = []
        
        direction_row = 0
        grand_total_index = total.index[total[cols[0]] == 'Grand Total']
        start_row = 1
        
        for i,col in enumerate(cols):
            direction_name = total.iloc[direction_row][col]
            start_name = total.iloc[start_row][col] 
            try:
                found = directions[direction_name]
                directions_present.append(direction_name)
            except:
                this = 1
            
            if start_name == 'App Total':
                app_totals_index.append(i + 1)
                directions_total.append(int(total.iloc[grand_total_index][col].iloc[0]))
        
        for i in range(len(directions_present)):
            data_dict[directions_present[i]] = directions_total[i]
            self.extract_attributes(data_dict,total.iloc[:,:app_totals_index[i]],modifier=f'{directions_present[i][0]} ')
            
            
        return grand_total_index
         
    def extract_attributes(self,data_dict:dict,total:pd.DataFrame,modifier=''):
        row_index = total.index[total['Leg'] == '% Total'].tolist()[0]
        labels = total.iloc[row_index + 1:]['Leg'].tolist()
        totals = total.iloc[row_index + 1:][total.columns[-1]].tolist()
        
        
        for i in range(len(labels)):
            # only even rows have values, odd rows have percentages
            if i % 2 == 0:
                # only if data is not for Pedestrians and Bicycles on Crosswalk
                if pd.notna(totals[i]):
                    try:
                        data_dict[modifier + labels[i]] = int(totals[i])
                    except:
                        print('here')
    
    def delete_files(self):
        for file in self.files_to_delete:
            os.remove(file)
                
        

if __name__ == "__main__":
    cols = ColumnNames()
    pi = ParseInfo(cols.get_cols())
    files = cols.file_names
    pi.create_aggregate(files)
    pi.delete_files()

