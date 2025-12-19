from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from pathlib import Path
import configparser
import logging

# === 直接讀取配置 ===
BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config" / "config.ini"

config = configparser.ConfigParser()
config.read(CONFIG_FILE, encoding='utf-8')

etf_code = '00981A'
etf_name = config[etf_code]['name']
data_path = BASE_DIR / config[etf_code]['data_path']
log_path = BASE_DIR / config[etf_code]['log_path']

data_path.mkdir(parents=True, exist_ok=True)
log_path.mkdir(parents=True, exist_ok=True)

# === 設定日誌 ===
log_file = log_path / f"{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info(f"ETF: {etf_name}")
logger.info(f"資料將儲存至: {data_path}")
logger.info(f"日誌路徑: {log_path}")

# === 設定 Chrome Headless 模式 ===
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=1920,1080')

driver = webdriver.Chrome(options=chrome_options)

try:
    logger.info("開始爬取資料...")
    driver.get("https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW")
    
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '股票名稱')]")))
    
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    # === 提取日期 ===
    data_date = None
    timestamp = None
    
    # 方法1: 尋找符合日期格式的 span (YYYY/MM/DD)
    for span in soup.find_all('span'):
        text = span.text.strip()
        if '/' in text and len(text.split('/')) == 3:
            try:
                # 嘗試解析日期
                date_obj = datetime.strptime(text, '%Y/%m/%d')
                data_date = text
                timestamp = date_obj.strftime('%Y%m%d')
                logger.info(f"找到資料日期: {data_date} -> 檔名格式: {timestamp}")
                break
            except ValueError:
                continue
    
    # 如果找不到日期，使用當前日期作為備用
    if timestamp is None:
        timestamp = datetime.now().strftime('%Y%m%d')
        logger.warning(f"未找到網頁日期，使用當前日期: {timestamp}")
    
    # === 提取持股明細表格 ===
    table = None
    for t in soup.find_all('table'):
        if '股票名稱' in t.text:
            table = t
            break
    
    if table is None:
        logger.error("找不到持股明細表格")
    else:
        logger.info("找到表格了！")
        
        data = []
        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) == 4:
                spans = [td.find('span') for td in tds]
                if all(spans):
                    data.append({
                        '股票代號': spans[0].text.strip(),
                        '股票名稱': spans[1].text.strip(),
                        '股數': spans[2].text.strip(),
                        '持股權重': spans[3].text.strip()
                    })
        
        df = pd.DataFrame(data)
        
        # 數據清理
        df['股數'] = df['股數'].str.replace(',', '').astype(int)
        df['持股權重'] = df['持股權重'].str.rstrip('%').astype(float)*0.01
        
        logger.info(f"共找到 {len(df)} 筆持股資料")
        logger.info(f"前 10 筆資料:\n{df.head(10).to_string()}")
        
        # 儲存檔案（使用從網頁提取的日期作為檔名）
        output_file = data_path / f"{timestamp}.parquet"
        df.to_parquet(output_file, index=False)
        logger.info(f"資料已儲存至: {output_file}")
    
except Exception as e:
    logger.error(f"執行時發生錯誤: {str(e)}", exc_info=True)
    
finally:
    driver.quit()
    logger.info("爬蟲程式執行完畢")