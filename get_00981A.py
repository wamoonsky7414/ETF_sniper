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
import re

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

def parse_number(text):
    """解析包含逗號的數字字串"""
    if not text:
        return None
    # 移除逗號和空白
    cleaned = text.strip().replace(',', '')
    try:
        # 嘗試轉換為浮點數
        return float(cleaned)
    except ValueError:
        return None

def extract_table_data(table):
    """從table中提取資料，回傳 {項目名稱: 金額} 的字典"""
    data = {}
    rows = table.find_all('tr')
    
    for row in rows:
        tds = row.find_all('td')
        if len(tds) >= 2:
            # 第一個td是項目名稱
            item_span = tds[0].find('span')
            if item_span:
                item_name = item_span.text.strip()
                
                # 第二個td包含金額（可能有多個span，最後一個通常是數字）
                value_spans = tds[1].find_all('span')
                if value_spans:
                    # 取最後一個span（通常是實際數值）
                    value_text = value_spans[-1].text.strip()
                    value = parse_number(value_text)
                    data[item_name] = value
                    logger.info(f"  - {item_name}: {value_text} -> {value}")
    
    return data

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
    
    for span in soup.find_all('span'):
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
    
    if timestamp is None:
        timestamp = datetime.now().strftime('%Y%m%d')
        logger.warning(f"未找到網頁日期，使用當前日期: {timestamp}")
    
    # ============================================================
    # === 1. 提取持股明細 (Holding) ===
    # ============================================================
    logger.info("=" * 60)
    logger.info("開始提取持股明細...")
    
    holding_table = None
    for t in soup.find_all('table'):
        if '股票名稱' in t.text:
            holding_table = t
            break
    
    if holding_table is None:
        logger.error("找不到持股明細表格")
    else:
        logger.info("找到持股明細表格！")
        
        holding_data = []
        for row in holding_table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) == 4:
                spans = [td.find('span') for td in tds]
                if all(spans):
                    holding_data.append({
                        '股票代號': spans[0].text.strip(),
                        '股票名稱': spans[1].text.strip(),
                        '股數': spans[2].text.strip(),
                        '持股權重': spans[3].text.strip()
                    })
        
        holding_df = pd.DataFrame(holding_data)
        
        # 數據清理
        holding_df['股數'] = holding_df['股數'].str.replace(',', '').astype(int)
        holding_df['持股權重'] = holding_df['持股權重'].str.rstrip('%').astype(float) * 0.01
        
        logger.info(f"共找到 {len(holding_df)} 筆持股資料")
        logger.info(f"前 10 筆資料:\n{holding_df.head(10).to_string()}")
        
        # 儲存 holding 資料
        holding_path = data_path / "holding"
        holding_path.mkdir(parents=True, exist_ok=True)
        
        holding_file = holding_path / f"{timestamp}.parquet"
        holding_df.to_parquet(holding_file, index=False)
        logger.info(f"持股明細已儲存至: {holding_file}")
    
    # ============================================================
    # === 2. 提取投資組合資訊 (Portfolio) ===
    # ============================================================
    logger.info("=" * 60)
    logger.info("開始提取投資組合資訊...")
    
    # 找到所有 table.table-bordered
    all_tables = soup.find_all('table', class_='table-bordered')
    logger.info(f"找到 {len(all_tables)} 個 table")
    
    portfolio_data = {'日期': data_date if data_date else timestamp}
    
    # 遍歷所有 table
    for idx, table in enumerate(all_tables):
        table_text = table.text
        logger.info(f"\n處理 Table {idx + 1}:")
        
        # Table 1: 基金資產 (淨資產、流通在外單位數、每單位淨值)
        if '基金資產' in table_text or '淨資產' in table_text:
            logger.info("找到基金資產表格")
            data = extract_table_data(table)
            portfolio_data.update(data)
        
        # Table 2 & 3: 包含"項目"和"金額"的表格
        elif '項目' in table_text and '金額' in table_text:
            logger.info("找到資產配置表格")
            data = extract_table_data(table)
            portfolio_data.update(data)
    
    # 建立 DataFrame
    portfolio_df = pd.DataFrame([portfolio_data])
    
    # 重新排列欄位順序（如果欄位存在的話）
    desired_columns = [
        '日期',
        '淨資產',
        '流通在外單位數',
        '每單位淨值',
        '期貨(名目本金)',
        '股票',
        '現金',
        '期貨保證金',
        '申贖應付款',
        '應收付證券款'
    ]
    
    # 只保留存在的欄位
    existing_columns = [col for col in desired_columns if col in portfolio_df.columns]
    # 加上其他未列出的欄位
    other_columns = [col for col in portfolio_df.columns if col not in existing_columns]
    final_columns = existing_columns + other_columns
    
    portfolio_df = portfolio_df[final_columns]
    
    logger.info(f"\n投資組合資訊:")
    logger.info(f"\n{portfolio_df.T.to_string()}")  # 轉置顯示更清楚
    
    # 儲存 portfolio 資料
    portfolio_path = data_path / "portfolio"
    portfolio_path.mkdir(parents=True, exist_ok=True)
    
    portfolio_file = portfolio_path / f"{timestamp}.parquet"
    portfolio_df.to_parquet(portfolio_file, index=False)
    logger.info(f"\n投資組合資訊已儲存至: {portfolio_file}")
    
    logger.info("=" * 60)
    logger.info("所有資料爬取完成！")
    
except Exception as e:
    logger.error(f"執行時發生錯誤: {str(e)}", exc_info=True)
    
finally:
    driver.quit()
    logger.info("爬蟲程式執行完畢")