import pandas as pd


profiles_df = pd.read_excel('profiles.xlsx')
cols = profiles_df.columns
max_length = profiles_df.__len__()
pd_dict : dict[str,list]= {"Col1" : [],"Col2":[],"Col3":[]}


# Mapping of variable levels with their descriptions
level_mapping = {
    "Deviation": {
        -10: "10 minutes early",
        -5: "5 minutes early",
         0: "exactly on time",
         5: "5 minutes late",
        10: "10 minutes late"
    },
    "Predictability": {
        0: "Hard to predict (can never tell when it will arrive)",
        1: "Somewhat predictable (predictable some days)",
        2: "Easy to predict (always late or always early)"
    },
    "Frequency": {
        10: "10 minutes",
        15: "15 minutes",
        20: "20 minutes",
        25: "25 minutes",
    },
    "Info. Availability": {
        0: "Access to only the posted schedule",
        1: "Access to real-time information (e.g., Google Maps)"
    },
    "Num Connections": {
        0: "No transfers",
        1: "1 transfer",
        2: "2 transfers"
    }
}
attribute_mapping = {
    "Deviation": "On average your bus is",
    "Predictability": "Your bus is always",
    "Frequency": "Your bus comes every",
    "Info. Availability": "You have",
    "Num Connections": "To get to your destination, it takes"
}



pd_dict["Col1"].append("")
pd_dict["Col2"].append("Option 1")
pd_dict["Col3"].append("Option 2")

i = 0
option_count = 0
while i < max_length:
    if i == max_length - 1:
        print("here")
    if option_count == 2:
        pd_dict["Col1"].append("")
        pd_dict["Col2"].append("Option 1")
        pd_dict["Col3"].append("Option 2")
        option_count = 0

    if option_count == 0:
        for col in cols:
            pd_dict['Col1'].append(attribute_mapping[col])
            pd_dict["Col2"].append(level_mapping[col][profiles_df.loc[i,col]])
    if option_count == 1:
        for col in cols:
            pd_dict["Col3"].append(level_mapping[col][profiles_df.loc[i,col]])
        
        if len(pd_dict['Col2']) != len(pd_dict['Col3']):
            print("Different")
    option_count += 1
    
    i += 1

print(pd_dict["Col1"].__len__())
print(len(pd_dict['Col2']))
print(len(pd_dict["Col3"]))
    
new_df = pd.DataFrame.from_dict(pd_dict)

new_df.to_excel('new_profiles.xlsx',index=False)
