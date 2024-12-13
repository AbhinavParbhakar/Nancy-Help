
import os
import pandas as pd
from gather_names import ColumnNames
from datetime import datetime

class ParseInfo:
    def __init__(self,extra_cols=[]) -> None:
        self.columns = ['Id','Study Name','Project','Location', 'Date','Time (hrs)', 'Lat', 'Long', 'Road Segment Type']
        self.directions = ['Southbound', 'Westbound', 'Northbound', 'Eastbound']
        self.movements = ['In','Out']
        final_col = 'Int. Total'
        
        for direction in self.directions:
            self.columns.extend([f'{direction} {mv}' for mv in self.movements])
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
    
    def create_aggregate(self,files:list[str],file_name='./Miovision Aggregate Data.xlsx')->None:
        """
        Input a list of files and aggregate information inside.
        Creates an excel file as the output
        """
        excel_file = file_name
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
            
            
            # add Study Name
            sheet_data[summary_col_1] = summary_col_2
            
            
            # add Project
            project_index = summary.index[summary[summary_col_1] == 'Project'].tolist()[0]
            sheet_data['Project'] = summary.iloc[project_index][summary_col_2]
            
            # add location
            location_index = summary.index[summary[summary_col_1] == 'Location'].tolist()[0]
            sheet_data['Location'] = summary.iloc[location_index][summary_col_2]
            
            # get lat-long
            lat_long_index = summary.index[summary[summary_col_1] == 'Latitude and Longitude'].tolist()[0]
            lat_long = summary.iloc[lat_long_index][summary_col_2].split(',')
            sheet_data['Lat'] =  float(lat_long[0])
            sheet_data['Long'] = float(lat_long[1])
            
            # classify as midblock or intersection
            self.get_road_type(sheet_data,total)
                
            # get directional data for in
            grand_total_index = self.get_directional_data_in(sheet_data,total)
            
            # get directional data for out
            movement_dict = self.get_directional_data_out(sheet_data,total)
            
            # make directional adjusted out
            self.directional_out_adjusted(sheet_data,total)
            
            # add the int total, assume that it is in the last column
            grand_total = int(total.iloc[grand_total_index][total.columns[-1]].iloc[0])
            sheet_data['Int. Total'] = grand_total
            
            # Extract vehicle class breakdown for all of the directions combined
            self.extract_attributes(sheet_data,total)
            
            # Update the in volumes to fill in gaps for pedestrian studies 
            self.update_directional_data_in(sheet_data,movement_dict)
            
            # Show the opposite direction in and out for one-ways
            self.detect_one_ways(sheet_data)
            
            return sheet_data
        else:
            self.files_to_delete.append(file)
            return None
    
    def return_adjusted_volume(self,total:pd.DataFrame):
        """
        For given file, return the breakdown of the total row excluding the
        Omitted classes: Bikes on road, peds, and bikes on crosswalk
        """
        
        # classes to ommit
        omission_classes = {"Bicycles on Road":'', "Pedestrians":'',"Bicycles on Crosswalk":''}
        directions = ['Southbound','Westbound','Northbound','Eastbound']
        
        # Retrieve the rows containing the total col and breakdowns
        # Retreive the last col which is the column of interest for us
        grand_total_index = total.index[total['Leg']=='Grand Total'].tolist()[0]
        grand_total = total.loc[grand_total_index,total.columns[-1]]
        row_index = total.index[total['Leg'] == '% Total'].tolist()[0]
        labels = total.iloc[row_index + 1:]['Leg'].tolist()
        totals = total.iloc[row_index + 1:][total.columns[-1]].tolist()
        
        
        for i in range(len(labels)):
            # only even rows have values, odd rows have percentages
            if i % 2 == 0 and labels[i] in omission_classes:
                # only if data is not for Pedestrians and Bicycles on Crosswalk
                if pd.notna(totals[i]):
                    try:
                        grand_total -= int(totals[i])
                    except:
                        print('Problem in return_adjusted_volume')
        
        return grand_total
                    
                
    
    def get_directional_data_out(self,data_dict:dict,total:pd.DataFrame):
        """
        Get out data for each dimension, which really means all the flow going in the opposite direction
        Super confusing even to me, but hey, that's how they asked for it.
        """
        direction_num_mapping = {
            'Southbound In' : 1,
            'Westbound In' : 2,
            'Northbound In' : 3,
            'Eastbound In' : 4
        }
        
        num_direction_mapping = {
            1 : 'Southbound',
            2 : 'Westbound',
            3 : 'Northbound',
            4 : 'Eastbound'
        }
        
        movements = {
            'Right' : True,
            'Thru' : True,
            'Left' : True,
            'U-Turn' : True

        }
        
        total_row_index = total.index[total['Leg'] == 'Grand Total'].tolist()[0]
        direction_row = 0
        start_row = 1
        directions = []
        movement_dict = {}
        cols = total.columns.tolist()
        valid_direction_flag = False
        last_direction = ''
        
        # if data_dict['Date'] == '2022/10/17':
        #     print(total.head())
        
        # populate the movement column
        for col in cols:
            # try to add last direction to last_direction
            direction = total.iloc[direction_row][col]
            movement = total.iloc[start_row][col]
            try:
                found = direction_num_mapping[f'{direction} In']
                last_direction = direction
                valid_direction_flag = True
            except:
                if type(direction) == type(str()):
                    if 'bound' in direction:
                        valid_direction_flag = False
                pass
            
            try:
                # Check to see if we've hit the last column for the direction
                # If so the valid direction should be set to false
                if valid_direction_flag:            
                    # Edge case for some files where instead of the 'Thru' Column, it has it under 'Direction'            
                    if movement == 'Direction':
                        movement_dict[f'{last_direction[0]} Thru'] = total.iloc[total_row_index][col]
                    else:
                        found = movements[movement]
                        move = f'{last_direction[0]} {movement}'
                        
                        # This way, even if there are multiple movements detected, we add them up
                        if move in movement_dict:
                            movement_dict[move] += total.iloc[total_row_index][col]
                        else:
                            movement_dict[move] = total.iloc[total_row_index][col]
            except:
                pass

        for column in list(data_dict.keys()):
            try:
                direction_num = direction_num_mapping[column]
                directions.append(direction_num)
            except:
                pass
        

        
        for direction in directions:
            # get directions for calculations
            out_total = 0
            thru_direction = direction + 2
            if thru_direction == 6:
                thru_direction = 2
            elif thru_direction == 5:
                thru_direction = 1
            uturn_direction = direction + 0
            counter_clockwise_direction = direction - 1
            if counter_clockwise_direction < 1:
                counter_clockwise_direction = 4
            clockwise_direction = direction + 1
            if clockwise_direction > 4:
                clockwise_direction = 1
            
            # try to add each direction
            try:
                out_total += movement_dict[f'{num_direction_mapping[thru_direction][0]} Thru']
            except:
                pass
                
            try:
                out_total += movement_dict[f'{num_direction_mapping[counter_clockwise_direction][0]} Left']
            except:
                pass
            
            try:
                out_total += movement_dict[f'{num_direction_mapping[clockwise_direction][0]} Right']
            except:
                pass
            
            try:
                out_total += movement_dict[f'{num_direction_mapping[uturn_direction][0]} U-Turn']
            except:
                pass
            
            data_dict[f'{num_direction_mapping[direction]} Out'] = out_total
        return movement_dict
    
    def directional_out_adjusted(self,data_dict:dict,total:pd.DataFrame):
        """
        Does the same task as get_directional_data_out with tweaks large enough that 
        a new function needed to be created to make new columns that contain adjusted out volume
        rows.
        """
        direction_num_mapping = {
            'Southbound In' : 1,
            'Westbound In' : 2,
            'Northbound In' : 3,
            'Eastbound In' : 4
        }
        
        num_direction_mapping = {
            1 : 'Southbound',
            2 : 'Westbound',
            3 : 'Northbound',
            4 : 'Eastbound'
        }
        
        movements = {
            'Right' : True,
            'Thru' : True,
            'Left' : True,
            'U-Turn' : True

        }
        
        total_row_index = total.index[total['Leg'] == 'Grand Total'].tolist()[0]
        direction_row = 0
        start_row = 1
        directions = []
        movement_dict = {}
        cols = total.columns.tolist()
        valid_direction_flag = False
        last_direction = ''
        
        # if data_dict['Date'] == '2022/10/17':
        #     print(total.head())
        
        # populate the movement column
        for col in cols:
            # try to add last direction to last_direction
            direction = total.iloc[direction_row][col]
            movement = total.iloc[start_row][col]
            try:
                found = direction_num_mapping[f'{direction} In']
                last_direction = direction
                valid_direction_flag = True
            except:
                # Only stop reading data, aka valid_direction_flag = False,
                # When a direction is found that is not either
                # NB,SB,EB,WB
                if type(direction) == type(str()):
                    if 'bound' in direction:
                        valid_direction_flag = False
                pass
            
            try:
                # Check to see if we've hit the last column for the direction
                # If so the valid direction should be set to false
                if valid_direction_flag:            
                    # Edge case for some files where instead of the 'Thru' Column, it has it under 'Direction'
                    adjusted_value = self.return_adjusted_volume(total.loc[:,:col])            
                    if movement == 'Direction':
                        movement_dict[f'{last_direction[0]} Thru'] = adjusted_value
                    else:
                        found = movements[movement]
                        move = f'{last_direction[0]} {movement}'
                        
                        # This way, even if there are multiple movements detected, we add them up
                        if move in movement_dict:
                            movement_dict[move] += adjusted_value
                        else:
                            movement_dict[move] = adjusted_value
            except:
                pass

        for column in list(data_dict.keys()):
            try:
                direction_num = direction_num_mapping[column]
                directions.append(direction_num)
            except:
                pass
        

        
        for direction in directions:
            # get directions for calculations
            out_total = 0
            thru_direction = direction + 2
            if thru_direction == 6:
                thru_direction = 2
            elif thru_direction == 5:
                thru_direction = 1
            uturn_direction = direction + 0
            counter_clockwise_direction = direction - 1
            if counter_clockwise_direction < 1:
                counter_clockwise_direction = 4
            clockwise_direction = direction + 1
            if clockwise_direction > 4:
                clockwise_direction = 1
            
            # try to add each direction
            try:
                out_total += movement_dict[f'{num_direction_mapping[thru_direction][0]} Thru']
            except:
                pass
                
            try:
                out_total += movement_dict[f'{num_direction_mapping[counter_clockwise_direction][0]} Left']
            except:
                pass
            
            try:
                out_total += movement_dict[f'{num_direction_mapping[clockwise_direction][0]} Right']
            except:
                pass
            
            try:
                out_total += movement_dict[f'{num_direction_mapping[uturn_direction][0]} U-Turn']
            except:
                pass
            
            data_dict[f'{num_direction_mapping[direction]} Adj. Out'] = out_total
            
            
    
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
    
    def update_directional_data_in(self,data_dict:dict,movement_dict:dict):
        """
        Update the direcional data in.
        
        At this point, all of the movements have been recorded for each one in movement dict.
        
        For each movement, the direction in will be equal to the u-turn + right + thru +  left
        """
        
        directions = [
            'Northbound',
            'Eastbound',
            'Westbound',
            'Southbound'
            ]
        
        movements = [
            'Right',
            'Thru',
            'Left',
            'U-Turn'
        ]
        
        for direction in directions:
            if f'{direction} In' in data_dict:
                new_total_in = 0
                for movement in movements:
                    if f'{direction[0]} {movement}' in movement_dict:
                        new_total_in += movement_dict[f'{direction[0]} {movement}']
                
                # Only update if the new total is less than or equal to the old total
                # This is the case because the update should essentially add a new in
                # When the app total column in the original file was 0, however there
                # was more than 0 for the different movements for the direction
                if new_total_in >= data_dict[f'{direction} In']:
                    data_dict[f'{direction} In'] = new_total_in
                        
    
    def get_directional_data_in(self,data_dict:dict,total:pd.DataFrame):
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
        
        # Create a flag to only allow app data to be read when one of the four directions that we care about has been read
        found_direction = False
        
        # Turn it off after the app data column has been read
        
        for i,col in enumerate(cols):
            direction_name = total.iloc[direction_row][col]
            start_name = total.iloc[start_row][col]
            try:
                found = directions[direction_name]
                directions_present.append(direction_name)
                found_direction = True
            except:
                if type(direction_name) == type(''):          
                  if 'bound' in direction_name:
                    found_direction = False
                pass

            if start_name == 'App Total' and found_direction:
                app_totals_index.append(i + 1)
                directions_total.append(int(total.iloc[grand_total_index][col].iloc[0]))
        
        for i in range(len(directions_present)):
            data_dict[f'{directions_present[i]} In'] = directions_total[i]
            self.extract_attributes(data_dict,total.iloc[:,:app_totals_index[i]],modifier=f'{directions_present[i][0]} ')
            
            
        return grand_total_index
    
    def detect_one_ways(self,data_dict:dict):
        """
        Checks to see if only one direction is present, meaning that it is a oneway,
        if this is the case, the opposite direction to the one that is present is added. 
        """
        direction_num_mapping = {
            'Southbound In' : 1,
            'Westbound In' : 2,
            'Northbound In' : 3,
            'Eastbound In' : 4
        }
        
        num_direction_mapping = {
            1 : 'Southbound',
            2 : 'Westbound',
            3 : 'Northbound',
            4 : 'Eastbound'
        }
        
        direction_ins = list(direction_num_mapping.keys())
        direction = ''
        direction_count = 0
        for d_in in direction_ins:
            # check to see how many direction in's are present - should be at least 2
            if d_in in data_dict:
                # meaning that it is not np.nan
                direction_count +=1
                direction = d_in
        
        if direction_count == 1:
            opposite_direction = direction_num_mapping[direction] + 2
            if opposite_direction == 6:
                opposite_direction = 2
            if opposite_direction == 5:
                opposite_direction = 1
            data_dict[f'{num_direction_mapping[opposite_direction]} Out'] = data_dict[direction]
            data_dict[f'{num_direction_mapping[opposite_direction]} In'] = 0
        
    
    def extract_attributes(self,data_dict:dict,total:pd.DataFrame,modifier=''):
        """
        Function extracting the vehicle class breakdown for the given scope of data
        """
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


def get_error_files(files:list[str],errors:pd.DataFrame)->list[str]:
    """
    Receive the list of all file locations, return the ones with errors
    """
    keys = list(errors["ID"])

    # Creating the dictionary with random values
    random_dict = {str(key): ' ' for key in keys}

    
    return_list = []
    # Each file looks like this: './YYYY/MM/DD/ID.xlsx'
    
    for file in files:
        splits = file.split('/')
        file_id = splits[-1].split('.')[0]
        if file_id in random_dict:
            return_list.append(file)
    
    return return_list
        
if __name__ == "__main__":
    cols = ColumnNames()
    pi = ParseInfo(cols.get_cols())
    files = cols.file_names
    # errors = pd.read_excel('Errors.xlsx')
    # files = get_error_files(files,errors)
    pi.create_aggregate(files,file_name="Miovision Aggregate Data (Out Vol. Adjustment).xlsx")
    # pi.delete_files()

