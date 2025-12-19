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
import time

# === 直接讀取配置 ===
BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config" / "config.ini"

config = configparser.ConfigParser()
config.read(CONFIG_FILE, encoding='utf-8')

# 取得 00982A 的配置 (Capital Fund)
etf_code = '00982A'
etf_name = config[etf_code]['name']
data_path = BASE_DIR / config[etf_code]['data_path']
log_path = BASE_DIR / config[etf_code]['log_path']

# 自動建立資料夾
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
    url = "https://www.capitalfund.com.tw/etf/product/detail/399/portfolio"
    driver.get(url)
    
    # 等待表格內容載入
    wait = WebDriverWait(driver, 15)
    wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '台積電')]")))
    time.sleep(2)
    logger.info("頁面載入完成")
    
    # === 提取日期 ===
    data_date = None
    timestamp = None
    
    try:
        # 尋找「資料日期」或包含日期的元素
        html_for_date = driver.page_source
        soup_date = BeautifulSoup(html_for_date, 'html.parser')
        
        # 方法1: 尋找包含「資料日期」、「更新日期」等文字的元素
        for element in soup_date.find_all(['div', 'span', 'p']):
            text = element.text.strip()
            if any(keyword in text for keyword in ['資料日期', '更新日期', '數據日期']):
                # 在同一元素或相鄰元素中尋找日期
                for span in element.find_all('span'):
                    span_text = span.text.strip()
                    if '/' in span_text:
                        try:
                            date_obj = datetime.strptime(span_text, '%Y/%m/%d')
                            data_date = span_text
                            timestamp = date_obj.strftime('%Y%m%d')
                            logger.info(f"找到資料日期: {data_date} -> 檔名格式: {timestamp}")
                            break
                        except ValueError:
                            continue
                if timestamp:
                    break
        
        # 方法2: 如果方法1失敗，尋找所有符合日期格式的 span
        if timestamp is None:
            for span in soup_date.find_all('span'):
                text = span.text.strip()
                if '/' in text and len(text.split('/')) == 3:
                    try:
                        date_obj = datetime.strptime(text, '%Y/%m/%d')
                        data_date = text
                        timestamp = date_obj.strftime('%Y%m%d')
                        logger.info(f"找到資料日期: {data_date} -> 檔名格式: {timestamp}")
                        break
                    except ValueError:
                        continue
    
    except Exception as e:
        logger.warning(f"提取日期時發生錯誤: {e}")
    
    # 如果找不到日期，使用當前日期作為備用
    if timestamp is None:
        timestamp = datetime.now().strftime('%Y%m%d')
        logger.warning(f"未找到網頁日期，使用當前日期: {timestamp}")
    
    # === 點擊展開全部按鈕 ===
    try:
        toggle_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '.pct-stock-table-tbody-toggle-btn'))
        )
        
        button_text = toggle_button.text
        logger.info(f"按鈕文字: {button_text}")
        
        if '展開' in button_text:
            logger.info("點擊展開全部按鈕...")
            toggle_button.click()
            time.sleep(2)
        else:
            logger.info("資料已經是展開狀態")
            
    except Exception as e:
        logger.warning(f"處理展開按鈕時發生錯誤: {e}")
        logger.info("繼續嘗試抓取資料...")
    
    # === 提取持股明細表格 ===
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    table_body = soup.find('div', class_='pct-stock-table-tbody')
    
    if table_body is None:
        logger.error("找不到持股明細表格")
    else:
        logger.info("找到表格了！")
        
        # 找到所有的 tr（只要桌面版的）
        rows = table_body.find_all('div', class_='tr show-for-medium')
        
        data = []
        for row in rows:
            cells = row.find_all('div', class_=['th', 'td'])
            
            if len(cells) >= 4:
                stock_code = cells[0].text.strip()
                stock_name = cells[1].text.strip()
                weight = cells[2].text.strip()
                shares = cells[3].text.strip()
                
                data.append({
                    '股票代號': stock_code,
                    '股票名稱': stock_name,
                    '持股權重': weight,
                    '股數': shares
                })
        
        df = pd.DataFrame(data)
        
        # 數據清理
        df['股數'] = df['股數'].str.replace(',', '').astype(int)
        df['持股權重'] = df['持股權重'].str.rstrip('%').astype(float) * 0.01
        
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