from playwright.sync_api import sync_playwright, Playwright, Page
from dataclasses import dataclass
from dotenv import dotenv_values
import logging
import datetime
import os
import time

# Gobal Variables
MAX_AUTH_DEFAULT_NAV_TIMEOUT = 60000 # miliseconds
MAX_MIOVISION_ID_MAX_DEFAULT_NAVIGATION_TIMEOUT = 90000# milliseconds
MIOVISION_SCREENSHOT_MAX_LOCATOR_TIMEOUT = 240000 #milliseconds
SCRAPING_START_YEAR = 2021
SCRAPING_END_YEAR = 2024

def configure_logging()->logging.Logger:
    """
    Configure logging
    
    ### Returns
    logger with configuration set up
    """
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename="scraping_directions.log",level=logging.INFO,filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    return logger

@dataclass
class ConfigurationDetails:
    AUTH_FILE_NAME:str
    AUTH_USERNAME:str
    AUTH_PASSWORD:str
    AUTH_LINK:str
    AUTH_MAX_DEFAULT_NAVIGATION_TIMEOUT:int
    AUTH_USERNAME_LOCATOR:str
    AUTH_SUBMIT_USERNAME_BUTTON_LOCATOR:str
    AUTH_PASSWORD_LOCATOR:str
    AUTH_SUBMIT_PASSWORD_BUTTON_LOCATOR:str
    SCRAPING_START_YEAR : int
    SCRAPING_END_YEAR : int
    MIOVISION_ID_LOCATOR : str
    MIOVISION_ID_MAX_DEFAULT_NAVIGATION_TIMEOUT:int
    MIOVISION_SCREENSHOTS_FOLDER_NAME:str
    MIOVISION_SCREENSHOT_LOCATOR:str
    MIOVISION_SCREENSHOT_MAX_LOCATOR_TIMEOUT:int

def create_auth_credentials(playwright:Playwright, config: ConfigurationDetails,logger:logging.Logger)->None:
    """
    Automates the process of generating a auth json config file using the name
    provided in the config object.
    
    ### Parameters
    1. playwright: ``Playwright``
        - Used to access the browser for automation
    2. config: ``ConfigurationDetails``
        - Contains information for populating details on the authentication page
    3. logger: ``logging.Logger``
        - Logger object used for logging
    
    ### Effects
    Started a headless automation of chromium for the link provided
    
    ### Returns
    None
    """
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    context.set_default_navigation_timeout(config.AUTH_MAX_DEFAULT_NAVIGATION_TIMEOUT)
    page = context.new_page()
    
    logger.info("[create_auth_credentials]: Started navigation to auth link")
    page.goto(config.AUTH_LINK)
    
    # Input and submit username
    logger.info("[create_auth_credentials]: Started completion of username")
    page.locator(config.AUTH_USERNAME_LOCATOR).type(config.AUTH_USERNAME)
    page.locator(config.AUTH_SUBMIT_USERNAME_BUTTON_LOCATOR).first.click()
    
    # Input and submit password
    logger.info("[create_auth_credentials]: Started completion of password")
    page.locator(config.AUTH_PASSWORD_LOCATOR).type(config.AUTH_PASSWORD)
    page.locator(config.AUTH_SUBMIT_PASSWORD_BUTTON_LOCATOR).first.click()
    
    # Wait for page load
    logger.info("[create_auth_credentials]: Waiting for load state")
    page.wait_for_load_state()

    # Saving auth details
    logger.info("[create_auth_credentials]: Saved auth details")
    context.storage_state(path=config.AUTH_FILE_NAME)
    
    # Clean up
    logger.info("[create_auth_credentials]: Closing page, context, and browser")
    page.close()
    context.close()
    browser.close()

def check_date_pattern(date_string:str)->bool:
    """
    Check if the date matches YYYY-MM-DD
    ### Parameters
    1. date_string : ``str``
        - String to be compared against the format
        
    ### Returns
    True or False for a valid or invalid pattern, respectively.
    """
    try:
        datetime.datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def retrieve_ids(page:Page,logger:logging.Logger,start_date:str,end_date:str,base_url:str,id_locator:str)->list[str]:
    """
    Given the page, navigate to the base url after adding the start date and end date.
    
    ### Parameters
    1. page: ``Page``
        - Page object used for navigation
    2. logger: ``logging.Logger``
        - Used for logging purposes
    3. start_date: ``str``
        - Start date should be provided in 'YYYY-MM-DD'
    4. end_date : ``str``
        - End date should be provided in 'YYYY-MM-DD'
    5. base_url : ``str``
        - Base url upon which the navigation url is built
    6. id_locator : ``str``
        - Element css used to locate the space where the IDs are stored.
    
    ### Returns
    List of id's stored in ``str`` format
    """
    if not check_date_pattern(start_date) or not check_date_pattern(end_date):
        raise Exception("Dates muste be given in YYYY-MM-DD format")
    
    link = base_url + f'studies/?end_date={end_date}&start_date={start_date}&state=Published'
    
    # Grab all id's after parsing
    logger.info(f'[retrieve_ids] Navigating to {link}')
    page.goto(link)
    
    uncleaned_ids = page.locator(id_locator).all_inner_texts()
    cleaned_ids = []
    
    for id_text in uncleaned_ids:
        # Remove whitespace
        id_text = id_text.strip(' ')
        clean_id = id_text.split("#")[-1]
        cleaned_ids.append(clean_id)
    
    logger.info(f'[retrieve_ids] Returning {len(cleaned_ids)} ids')
    return cleaned_ids
    
    
def scrape_miovision_ids(playwright:Playwright,config:ConfigurationDetails,logger:logging.Logger)->list[str]:
    """
    Using the start and end year values stored in config, scrape the website
    and return the id's for the studies that exist within that temporal window.
    
    ### Parameters
    1. playwright: ``Playwright``
        - Object used for scraping
    2. config: ``ConfigurationDetails``
        - Stores configuration details for scraping
    3. logger: ``logging.Logger``
        - Used for logging
    
    ### Effects
    Starts headless chromium browsers
    
    ### Returns
    List of id's stored as ``str``
    """
    
    # Visit each month in each year
    start_year = config.SCRAPING_START_YEAR
    end_year = config.SCRAPING_END_YEAR
    
    miovision_ids = []
    
    logger.info('[scrape_miovision_ids] Starting browser')
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(storage_state=config.AUTH_FILE_NAME)
    context.set_default_navigation_timeout(config.MIOVISION_ID_MAX_DEFAULT_NAVIGATION_TIMEOUT)
    page = context.new_page()
    while start_year <= end_year:
        # Cycle through the months
        
        for i in range(1,13):
            start_date = f'{start_year}-{i}-01'
            if i == 12:
                end_date = f'{start_year + 1}-01-01'
            else:
                end_date = f'{start_year}-{i+1}-01'
            
            logger.info(f'[scrape_miovision_ids] Getting ids from {start_date} to {end_date}')
            try:
                monthly_ids = retrieve_ids(
                    page=page,
                    logger=logger,
                    start_date=start_date,
                    end_date=end_date,
                    base_url=config.AUTH_LINK,
                    id_locator=config.MIOVISION_ID_LOCATOR
                )
                
                miovision_ids.extend(monthly_ids)
            except Exception as e:
                logger.error(f'[scrape_miovision_ids] Error: {e}')
        
        start_year +=1
    
    logger.info("[scrape_miovision_ids] Closing page, context, and browser")
    page.close()
    context.close()
    browser.close()
    return miovision_ids

def scrape_miovision_screenshots(logger:logging.Logger, playwright:Playwright,config:ConfigurationDetails,miovision_ids:list[str])->None:
    """
    Given the list of miovision IDs, visit each webpage, and save a screenshot of the location mapping for each one in local storage. The images
    will be stored in the name of folder specified in the ``Configuration Details`` object. 
    
    ### Parameters
    1. logger : ``logging.Logger``
        - Logger object used to create logs
    2. playwright : ``Playwright``
        - Playwright object used to create browsers to be used for automation
    3. config : ``ConfigurationDetails``
        - Contains the details for congiration of the browser
    4. miovision_ids : ``list[str]``
        - List of IDs to be used to guide scraping process
    
    ### Effects
    Starts headless browsers and downloads screenshots in local storage
    
    ### Returns
    
    ``None``
    """
    
    # Configure browser set up
    browser = playwright.chromium.launch()
    context = browser.new_context(storage_state=config.AUTH_FILE_NAME)
    context.set_default_navigation_timeout(config.MIOVISION_ID_MAX_DEFAULT_NAVIGATION_TIMEOUT)
    page = context.new_page()
    page.set_default_timeout(config.MIOVISION_SCREENSHOT_MAX_LOCATOR_TIMEOUT)
    
    logger.info("[scrape_miovision_screenshots] Configured browser")
    
    # Create base folder if it doesn't exist
    relative_folder_path = f'./{config.MIOVISION_SCREENSHOTS_FOLDER_NAME}'
    if not os.path.exists(relative_folder_path):
        os.mkdir(relative_folder_path)
    
    
    for miovision_id in miovision_ids:
        image_path = f'{relative_folder_path}/{miovision_id}.png'
        
        logger.info(f"[scrape_miovision_screenshots] Navigating to {miovision_id}")
        page.goto(f'{config.AUTH_LINK}studies/{miovision_id}')
        page.wait_for_load_state()
        
        # Check if the study results in a 404
        if "404" not in page.url:  
            page.locator(config.MIOVISION_SCREENSHOT_LOCATOR).click()
            
            logger.info(f"[scrape_miovision_screenshots] Taking screenshot for {miovision_id}")
            page.wait_for_load_state()
            time.sleep(2)
            page.screenshot(path=image_path)
    
    # Cleaning up automation session
    logger.info("[scrape_miovision_screenshots] Closing page, context, and browser")
    page.close()
    context.close()
    browser.close()
        
    
        
def main(config:ConfigurationDetails,logger:logging.Logger):
    """
    Main method to orchestrate the flow of execution
    
    ### Parameters
    1. config: ``ConfigurationDetails``
        - Data class that contains details for configuration
    2. logger: ``logging.Logging``
        - Logger object used for logging
        
    ### Effects
    Starts the flow of extracting auth details and scraping
    
    ### Returns
    None
    """
    with sync_playwright() as playwright:
        logger.info("Started subroutine for auth credentials generation.")
        create_auth_credentials(playwright=playwright,config=config,logger=logger)
        miovision_ids = scrape_miovision_ids(playwright=playwright,config=config,logger=logger)
        scrape_miovision_screenshots(logger=logger,playwright=playwright,config=config,miovision_ids=miovision_ids)
        

if __name__ == "__main__":
    config = dotenv_values(".env")
    
    auth_username_locator = 'input[name="username"]'
    auth_username_submit_button_locator = 'button[type="submit"]'
    auth_password_locator = 'input[name="password"]'
    auth_password_submit_button_locator = 'button[type="submit"]'
    miovision_id_locator = 'div.miogrey'
    miovision_screenshots_folder_name = "Screenshots"
    miovision_screenshot_locator = 'button.gm-control-active.gm-fullscreen-control'
    
    auth_config = ConfigurationDetails(
        AUTH_FILE_NAME=config['AUTH_FILE_NAME'],
        AUTH_USERNAME=config['USERNAME'],
        AUTH_PASSWORD=config['PASSWORD'],
        AUTH_LINK=config['AUTH_LINK'],
        AUTH_MAX_DEFAULT_NAVIGATION_TIMEOUT=MAX_AUTH_DEFAULT_NAV_TIMEOUT,
        AUTH_USERNAME_LOCATOR=auth_username_locator,
        AUTH_SUBMIT_USERNAME_BUTTON_LOCATOR=auth_username_submit_button_locator,
        AUTH_PASSWORD_LOCATOR=auth_password_locator,
        AUTH_SUBMIT_PASSWORD_BUTTON_LOCATOR=auth_password_submit_button_locator,
        SCRAPING_START_YEAR=SCRAPING_START_YEAR,
        SCRAPING_END_YEAR=SCRAPING_END_YEAR,
        MIOVISION_ID_LOCATOR=miovision_id_locator,
        MIOVISION_ID_MAX_DEFAULT_NAVIGATION_TIMEOUT=MAX_MIOVISION_ID_MAX_DEFAULT_NAVIGATION_TIMEOUT,
        MIOVISION_SCREENSHOTS_FOLDER_NAME=miovision_screenshots_folder_name,
        MIOVISION_SCREENSHOT_LOCATOR=miovision_screenshot_locator,
        MIOVISION_SCREENSHOT_MAX_LOCATOR_TIMEOUT=MIOVISION_SCREENSHOT_MAX_LOCATOR_TIMEOUT
    )
    
    logger = configure_logging()
    
    main(config=auth_config,logger=logger)