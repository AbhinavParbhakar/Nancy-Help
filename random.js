import path, { resolve } from 'path';
import fs from 'fs';
import fetch from 'node-fetch'; // Assuming you're using node-fetch for fetch
import playwright from 'playwright'
import { exitCode } from 'process';


const userSavePath = './auth.json'

function get_session_id(path){
    const content = fs.readFileSync(path,{encoding:'utf-8',flag:'r'})
    var auth_tokens = JSON.parse(content)

    const session_id = auth_tokens.cookies[0].value
    return session_id
}


async function download_file(downloadPath,id,session_id){
    // Ensure the 'downloads' directory exists
    const dirPath = path.dirname(downloadPath);
    if (!fs.existsSync(downloadPath)) {
    fs.mkdirSync(dirPath, { recursive: true });  // Create directory recursively if it doesn't exist
    }

    let response = await fetch(`https://datalink.miovision.com/studies/${id}/report?download_token=1727917620&report%5Bformat%5D=xlsx&report%5Bbin_size%5D=3600&report%5Bworksheet_grouping%5D=by_class&report%5Bapproach_order%5D=n_ne_e_se_s_sw_w_nw&report%5Binclude_raw_data%5D=false&report%5Bforced_peak_enabled%5D=false`, {
    headers: {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "sec-ch-ua": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "\"Android\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "cookie": `central_production_session_id=${session_id}; return_to=https://datalink.miovision.com/studies?end_date=2023-09-30&start_date=2023-09-01; intercom-device-id-mi3ti0da=3fd9cefa-c2a6-4b36-8774-30be6acd47a0; intercom-session-mi3ti0da=TXBVR3lWTmErNFRtWWFhTEZSYy93SGJacVNFMVlIaVJiR2ZtblRadWxyYXF5NC9RSXc4UThCS3lYVnQ1SnVHLy0tRmg5UFhvWExvbVV2ejhjVGJtMndXZz09--c5a252b23a20dd34264913de39638c846f16f6e8; download_token_1727916324=1727916324`,
        "Referer": "https://datalink.miovision.com/studies/1127843",
        "Referrer-Policy": "strict-origin-when-cross-origin"
    },
    method: "GET"
    });

    if (response.ok) {

    // Create a writable file stream
    const fileStream = fs.createWriteStream(downloadPath);

    // Pipe the response stream to the file stream
    response.body.pipe(fileStream);

    // Handle success and error
    fileStream.on('finish', () => {
        return new Promise((resolve,reject)=>{
            resolve()
        })
    });

    fileStream.on('error', (err) => {
        console.error('Error writing to file:', err);
    });
    } else {
    console.error(`Failed to download file: ${response.statusText}`);
    }
}

async function extract_files(start_date,end_date,sessionId){
    const base_folder = './Miovision'
    const monthMapping = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12
    };

    const chromium = playwright.chromium

    const browser = await chromium.launch({
        headless:false
    })

    const context = await browser.newContext({
        storageState:userSavePath
    })

    const page = await context.newPage()
    context.setDefaultNavigationTimeout(300000)
    await page.goto(`https://datalink.miovision.com/?state=Published&end_date=${end_date}&start_date=${start_date}`,{
    })

    const container_locator = 'tr[class="marker_hover"]'
    const id_locator = "div.miogrey"
    const date_locator = 'td[class="nowrap"]'

    const containers = await page.locator(container_locator)
    const raw_ids = await containers.locator(id_locator).all()
    const raw_dates = await containers.locator(date_locator).all()

    const ids = []
    const paths = []
    const studyTypes = []

    for (let i = 0;i < raw_ids.length;i++){
        const id_split =  (await raw_ids[i].innerText()).split('#')
        const id = id_split[1].trim()
        const studyType = id_split[0].split(' h')[1].trim()
        ids.push(id)
        studyTypes.push(studyType)
    }

    for (let i = 0;i < raw_dates.length;i++){
        const cleaned_date = (await raw_dates[i].innerText()).replace(',','')
        const date_split = cleaned_date.split(' ')
        // Day_name Month Day_num Year
        const day_name = date_split[0]
        const month = date_split[1]
        const day_num = date_split[2]
        const year = date_split[3]

        const path = `${base_folder}/${year}/${monthMapping[month]}/${day_num}/${studyTypes[i]}-${ids[i]}.xlsx`
        paths.push(path)
    }

    await page.close()
    await context.close()
    await browser.close()

    const sleep = (timeout) => {return new Promise((resolve,reject)=>setTimeout(resolve,timeout))}
    for (let i = 0;i<ids.length;i++){
        download_file(paths[i],ids[i],sessionId)
        await sleep(3000)
    }

    return new Promise((resolve,reject)=>{
        resolve()
    })
}

async function extract_session(){
    const chromium = playwright.chromium

    const sleep = (timeout) => {return new Promise((resolve,reject)=>setTimeout(resolve,timeout))}
    
    const browser = await chromium.launch({
        headless:false
    })

    const context = await browser.newContext()
    const page =  await context.newPage()

    await page.goto('https://datalink.miovision.com/',{
    })
    
    console.log("Closing Page")

    await sleep(60000)

    await context.storageState({
        path:userSavePath
    })
    await browser.close()
    return new Promise((resolve,reject)=>{
        resolve()
    })
}

async function main(){
    const daysInMonth = {
        1: 31,  // January
        2: 28,  // February (non-leap year)
        3: 31,  // March
        4: 30,  // April
        5: 31,  // May
        6: 30,  // June
        7: 31,  // July
        8: 31,  // August
        9: 30,  // September
        10: 31, // October
        11: 30, // November
        12: 31  // December
      };
    let start_year = 2022
    const end_year = 2024
    const sessionId = get_session_id(userSavePath)
    while (start_year <= end_year){
        for(let i = 1;i <= 12;i++){
            const start_date = `${start_year}-${i}-01`
            const end_date = `${start_year}-${i}-${daysInMonth[i]}`
            await extract_files(start_date,end_date,sessionId)
        }
        start_year += 1
    }

    return new Promise((resolve,reject)=>{
        resolve()
    })
}

//extract_session()
// download_file(downloadPath)
main()
