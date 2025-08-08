from playwright.sync_api import sync_playwright,Playwright,Browser,BrowserContext,Page,Locator
from playwright.async_api import async_playwright
import pandas as pd
import time
import numpy as np
import asyncio
import json
from tqdm import tqdm

def reformat_dict(data_dict:dict)->dict:
    """
    Takes in a dict, and then returns the same dict with the values now being inside of lists and with the 
    """
    keys = list(data_dict.keys())
    new_dict = {}
    for key in keys:
        new_dict[key] = [data_dict[key]]
    
    return new_dict

async def validate_data(file_name:str,auth_file_name:str,validation_file_name):
    """
    Validate the results from the miovision aggregate data.
    """
    data = pd.read_excel(file_name)
    id_col = data.columns[0]
    out_cols = ['Southbound Out','Northbound Out','Westbound Out','Eastbound Out']
    estimate_cols = ['Alg. ' + i for i in out_cols]
    actual_cols = ['Act. ' + i for i in out_cols]
    vol_cols = estimate_cols + actual_cols
    out_data_dict = {}
    out_data_dict["ID"] = []
    out_data_dict['Link'] = []
    
    # Contains all of the headers to be used for the outputted excel file
    cols = ["ID"] + vol_cols + ["Link"]
    
    # pd DataFrame to be used to output excel
    
    out_data = pd.DataFrame(columns=cols)
    col_html_map = {
        'Southbound Out':'exit_1',
        'Westbound Out':'exit_3',
        'Northbound Out':'exit_5',
        'Eastbound Out':'exit_7'
    }
    ids = list(data.loc[:,id_col])
    
    async with async_playwright() as playwright:
        browser : Browser = await playwright.chromium.launch(headless=True)
        context : BrowserContext= await browser.new_context(storage_state=auth_file_name)
        context.set_default_navigation_timeout(300000)
        
        page : Page = await context.new_page()
        locator_str = '.movement.exit_total.'
        
        
        
        for i,id in enumerate(tqdm(ids)):
            await page.goto(f'https://datalink.miovision.com/studies/{id}')
            out_data_dict['ID'].append(id)
            out_data_dict['Link'].append(f'https://datalink.miovision.com/studies/{id}')
            
            # used to track which directions were inputted, and which were not
            # Needed in order to fill the 
            col_index = [i for i in range(len(estimate_cols) + len(actual_cols))]
            for col in out_cols:
                try:
                    locator : Locator =  page.locator(f'{locator_str}{col_html_map[col]}')
                    if await locator.count() != 0:
                        # if  locator is found, then record the actual values for that column
                        text : str = await locator.text_content()
                        actual = int(text.split(':')[1])
                        estimate = data.loc[i,col]
                        actual_col = f'Act. {col}'
                        estimate_col = f'Alg. {col}'             
                    else:
                        # record nothing for that column
                        actual = np.nan
                        estimate = np.nan
                        actual_col = f'Act. {col}'
                        estimate_col = f'Alg. {col}'
                    
                    
                    # Check to see if the col has been added before adding it
                    if actual_col in out_data_dict:
                        out_data_dict[actual_col].append(actual)
                    else:
                        out_data_dict[actual_col] = [actual]
                    if estimate_col in out_data_dict:
                        out_data_dict[estimate_col].append(estimate)
                    else:
                        out_data_dict[estimate_col] = [estimate]
                        
                    # Log all of the discrepancies, as long as the value exists (actual > -1, np.nan would be False)
                    # and if the actual value does not equal the estimate
                    if actual != estimate and actual > -1:
                        with open('./errors1.txt','a') as file:
                            file.write(f'Discrepancy within {id} for {col} direction\n')
                            print(f'Discrepancy within {id} for {col} direction')
                            file.close()
                        
                except Exception as e:
                    print(e)

                
        new_frame = pd.DataFrame(out_data_dict)
        main_frame = pd.concat([out_data,new_frame],ignore_index=True)
        await page.close()
        await context.close()
        await browser.close()
        
        main_frame.to_excel(validation_file_name,index=False)
    
    

def get_credentials(auth_file:str):
    link = 'https://datalink.miovision.com/'
    playwright : Playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto(link)
    time.sleep(60)
    context.storage_state(path=auth_file)
    context.close()
    page.close()
    browser.close()
if __name__ == '__main__':
    auth_file = './auth.json'
    excel_file = './Miovision Aggregate Data Updated 2014-2024.xlsx'
    validation_file_name = "2014-2024_validation.xlsx"
    # get_credentials(auth_file)
    asyncio.run(validate_data(excel_file,auth_file,validation_file_name))
    