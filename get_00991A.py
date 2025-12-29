"""
通用 ETF 資料自動下載與處理腳本 (Headless + Parquet)
支援多個 ETF: 00982A, 00991A 等
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import os
import re


def clean_download_directory(download_path):
    """
    清空下載目錄中的所有檔案
    
    參數:
    download_path: 下載目錄路徑
    """
    import shutil
    
    if not os.path.exists(download_path):
        print(f"⚠ 目錄不存在: {download_path}")
        return
    
    try:
        file_count = 0
        for filename in os.listdir(download_path):
            file_path = os.path.join(download_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    file_count += 1
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    file_count += 1
            except Exception as e:
                print(f"⚠ 無法刪除 {filename}: {e}")
        
        if file_count > 0:
            print(f"✓ 已清理 {file_count} 個檔案/目錄")
        else:
            print("✓ 目錄已經是空的")
            
    except Exception as e:
        print(f"✗ 清理目錄時發生錯誤: {e}")


def download_etf_file(url, download_path, button_selector, selector_type="CSS", headless=True):
    """下載 ETF 檔案"""
    os.makedirs(download_path, exist_ok=True)
    
    chrome_options = Options()
    
    prefs = {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Headless 模式設定
    if headless:
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        print("✓ 使用 Headless 模式 (無視窗)")
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 15)
        time.sleep(2)
        
        if selector_type == "CSS":
            download_button = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, button_selector))
            )
        else:
            download_button = wait.until(
                EC.presence_of_element_located((By.XPATH, button_selector))
            )
        
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", download_button)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", download_button)
        print("✓ 已點擊下載按鈕，等待下載完成...")
        
        time.sleep(5)
        
        files = os.listdir(download_path)
        if files:
            latest_file = max([os.path.join(download_path, f) for f in files], 
                             key=os.path.getctime)
            
            original_filename = os.path.basename(latest_file)
            file_extension = os.path.splitext(original_filename)[1]
            filename_without_ext = os.path.splitext(original_filename)[0]
            
            date_match = re.search(r'(\d{4}_\d{2}_\d{2})', filename_without_ext)
            
            if date_match:
                date_str = date_match.group(1)
                new_filename = date_str.replace('_', '') + file_extension
            else:
                print(f"⚠ 警告: 無法從檔名中提取日期，使用原始檔名: {original_filename}")
                new_filename = original_filename
            
            new_filepath = os.path.join(download_path, new_filename)
            
            if os.path.exists(new_filepath):
                os.remove(new_filepath)
            
            os.rename(latest_file, new_filepath)
            print(f"✓ 檔案已下載並重命名為: {new_filename}")
            return new_filepath
        else:
            print("✗ 沒有找到下載的檔案")
            return None
            
    except Exception as e:
        print(f"✗ 下載時發生錯誤: {e}")
        return None
    finally:
        driver.quit()


def preprocess_portfolio_data(df):
    """
    Portfolio 資料前處理
    - 日期轉換為 datetime
    - 移除數值欄位的逗號並轉換為數值型態
    """
    # 建立副本避免修改原始資料
    df = df.copy()
    
    # 日期處理: 轉換為 datetime
    df['日期'] = df['日期'].astype(str)
    df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')
    
    # 處理基金資產淨值 (移除逗號)
    if df['基金資產淨值'].dtype == 'object':
        df['基金資產淨值'] = df['基金資產淨值'].astype(str).str.replace(',', '').astype(float)
    else:
        df['基金資產淨值'] = pd.to_numeric(df['基金資產淨值'], errors='coerce')
    
    # 處理基金在外流通單位數 (移除逗號)
    if df['基金在外流通單位數'].dtype == 'object':
        df['基金在外流通單位數'] = df['基金在外流通單位數'].astype(str).str.replace(',', '').astype(float)
    else:
        df['基金在外流通單位數'] = pd.to_numeric(df['基金在外流通單位數'], errors='coerce')
    
    # 處理基金每單位淨值
    df['基金每單位淨值'] = pd.to_numeric(df['基金每單位淨值'], errors='coerce')
    
    # 移除含有 NaN 的行
    df = df.dropna()
    
    return df


def preprocess_holdings_data(df):
    """
    Holdings 資料前處理
    - 日期轉換為 datetime
    - 證券代號清理
    - 移除數值欄位的逗號並轉換為數值型態
    - 權重去掉 % 並轉換為小數 (例如: 18.156% → 0.18156)
    """
    # 建立副本避免修改原始資料
    df = df.copy()
    
    # 日期處理: 轉換為 datetime
    df['日期'] = df['日期'].astype(str)
    df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')
    
    # 證券代號處理: 轉為字串並移除 .0
    df['證券代號'] = df['證券代號'].astype(str).str.replace('.0', '', regex=False)
    
    # 證券名稱確保是字串
    df['證券名稱'] = df['證券名稱'].astype(str)
    
    # 處理股數 (移除逗號)
    if df['股數'].dtype == 'object':
        df['股數'] = df['股數'].astype(str).str.replace(',', '').astype(float)
    else:
        df['股數'] = pd.to_numeric(df['股數'], errors='coerce')
    
    # 處理金額 (移除逗號)
    if df['金額'].dtype == 'object':
        df['金額'] = df['金額'].astype(str).str.replace(',', '').astype(float)
    else:
        df['金額'] = pd.to_numeric(df['金額'], errors='coerce')
    
    # 處理權重 (去掉 % 並轉換為小數)
    if df['權重(%)'].dtype == 'object':
        # 移除 % 符號並轉換為數值，然後除以 100
        df['權重(%)'] = df['權重(%)'].astype(str).str.replace('%', '').astype(float) / 100
    else:
        # 如果已經是數值，檢查是否需要除以 100
        # 假設原始數據如果 > 1 就是百分比形式 (例如 18.156)
        df['權重(%)'] = pd.to_numeric(df['權重(%)'], errors='coerce')
        # 如果最大值 > 1，假設是百分比需要除以 100
        if df['權重(%)'].max() > 1:
            df['權重(%)'] = df['權重(%)'] / 100
    
    # 移除含有 NaN 的行 (僅檢查關鍵欄位)
    df = df.dropna(subset=['證券代號', '證券名稱'])
    
    return df


def process_00991A_excel(input_file, base_path):
    """處理 00991A (復華台灣未來50) Excel 檔案"""
    portfolio_path = os.path.join(base_path, "portfolio")
    holding_path = os.path.join(base_path, "holding")
    
    os.makedirs(portfolio_path, exist_ok=True)
    os.makedirs(holding_path, exist_ok=True)
    
    df = pd.read_excel(input_file, sheet_name=0, header=None)
    
    # 提取日期
    filename = os.path.basename(input_file)
    date_match = re.search(r'(\d{8})', filename)
    if date_match:
        date_str = date_match.group(0)
    else:
        from datetime import datetime
        date_str = datetime.now().strftime("%Y%m%d")
    
    # 提取基金資訊
    fund_nav = df.iloc[4, 0]
    fund_units = df.iloc[6, 0]
    fund_nav_per_unit = df.iloc[8, 0]
    
    portfolio_df = pd.DataFrame({
        '日期': [date_str],
        '基金資產淨值': [fund_nav],
        '基金在外流通單位數': [fund_units],
        '基金每單位淨值': [fund_nav_per_unit]
    })
    
    portfolio_df = preprocess_portfolio_data(portfolio_df)
    
    # 儲存為 Parquet
    portfolio_file = os.path.join(portfolio_path, f"{date_str}.parquet")
    portfolio_df.to_parquet(portfolio_file, index=False, engine='pyarrow', compression='snappy')
    print(f"✓ Portfolio 已儲存至: {portfolio_file}")
    
    # 提取持股資訊
    holdings_start_idx = None
    for idx, row in df.iterrows():
        if row[0] == '證券代號':
            holdings_start_idx = idx
            break
    
    if holdings_start_idx is not None:
        holdings_df = pd.read_excel(input_file, sheet_name=0, header=holdings_start_idx)
        holdings_df = holdings_df.dropna(how='all')
        holdings_df = holdings_df.loc[:, ~holdings_df.columns.str.contains('^Unnamed')]
        holdings_df.insert(0, '日期', date_str)
        
        holdings_df = preprocess_holdings_data(holdings_df)
        
        # 儲存為 Parquet
        holding_file = os.path.join(holding_path, f"{date_str}.parquet")
        holdings_df.to_parquet(holding_file, index=False, engine='pyarrow', compression='snappy')
        print(f"✓ Holdings 已儲存至: {holding_file}")
        print(f"  共 {len(holdings_df)} 筆持股資料")
    else:
        print("✗ 找不到持股資料!")
        holdings_df = None
    
    return portfolio_df, holdings_df


def process_00982A_excel(input_file, base_path):
    """處理 00982A (中信中國50) Excel 檔案 - 需根據實際格式調整"""
    print("⚠ 00982A 處理功能待實作")
    print("請提供 00982A 的 Excel 檔案範例以完成此功能")
    return None, None


# ETF 配置字典
ETF_CONFIGS = {
    "00991A": {
        "name": "復華台灣未來50主動式ETF",
        "url": "https://www.fhtrust.com.tw/ETF/etf_detail/ETF23?utm_campaign=2025ETF00991A#stockhold",
        "button_selector": "//span[text()='檔案下載']",
        "selector_type": "XPATH",
        "processor": process_00991A_excel
    },
    "00982A": {
        "name": "中信中國50",
        "url": "https://www.capitalfund.com.tw/etf/product/detail/399/portfolio",
        "button_selector": "button.buyback-search-section-btn",
        "selector_type": "CSS",
        "processor": process_00982A_excel
    }
}


def download_and_process_etf(etf_code, base_dir=r"C:\Users\User\Documents\GitHub\ETF_sniper\data", headless=True):
    """
    下載並處理指定的 ETF 資料
    
    參數:
    etf_code: ETF 代碼 (例如 "00991A", "00982A")
    base_dir: 資料基礎目錄
    headless: 是否使用無視窗模式
    """
    if etf_code not in ETF_CONFIGS:
        print(f"✗ 不支援的 ETF 代碼: {etf_code}")
        print(f"支援的代碼: {', '.join(ETF_CONFIGS.keys())}")
        return
    
    config = ETF_CONFIGS[etf_code]
    
    print("=" * 60)
    print(f"開始處理 {etf_code} ({config['name']})")
    print("=" * 60)
    print()
    
    # 設定路徑
    base_path = os.path.join(base_dir, etf_code)
    download_path = os.path.join(base_path, "download")
    
    # 步驟 1: 下載檔案
    print("步驟 1: 下載 Excel 檔案...")
    print("-" * 60)
    downloaded_file = download_etf_file(
        url=config["url"],
        download_path=download_path,
        button_selector=config["button_selector"],
        selector_type=config["selector_type"],
        headless=headless
    )
    
    if not downloaded_file:
        print("\n✗ 下載失敗，流程中止")
        return
    
    print()
    
    # 步驟 2: 處理檔案
    print("步驟 2: 處理 Excel 檔案並儲存為 Parquet...")
    print("-" * 60)
    try:
        portfolio_df, holdings_df = config["processor"](downloaded_file, base_path)
        
        if portfolio_df is not None:
            print()
            print("=" * 60)
            print("處理完成!")
            print("=" * 60)
            print("\n基金資訊:")
            print(portfolio_df.to_string(index=False))
            
            if holdings_df is not None:
                print(f"\n持股資料: 共 {len(holdings_df)} 筆")
                print("\n前 5 大持股:")
                print(holdings_df.head(5).to_string(index=False))
        
    except Exception as e:
        print(f"\n✗ 處理檔案時發生錯誤: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 步驟 3: 清空下載目錄
        print("\n步驟 3: 清理下載目錄...")
        print("-" * 60)
        clean_download_directory(download_path)


def download_and_process_all_etfs(base_dir=r"C:\Users\User\Documents\GitHub\ETF_sniper\data", headless=True):
    """下載並處理所有已配置的 ETF"""
    for etf_code in ETF_CONFIGS.keys():
        download_and_process_etf(etf_code, base_dir, headless)
        print("\n" + "=" * 60 + "\n")


def read_parquet_example(etf_code, date_str, base_dir=r"C:\Users\User\Documents\GitHub\ETF_sniper\data"):
    """
    讀取 Parquet 檔案的範例
    
    參數:
    etf_code: ETF 代碼
    date_str: 日期字串 (YYYYMMDD)
    base_dir: 資料基礎目錄
    """
    base_path = os.path.join(base_dir, etf_code)
    
    # 讀取 portfolio
    portfolio_file = os.path.join(base_path, "portfolio", f"{date_str}.parquet")
    if os.path.exists(portfolio_file):
        portfolio_df = pd.read_parquet(portfolio_file)
        print(f"Portfolio 資料 ({date_str}):")
        print(portfolio_df)
        print()
    
    # 讀取 holding
    holding_file = os.path.join(base_path, "holding", f"{date_str}.parquet")
    if os.path.exists(holding_file):
        holdings_df = pd.read_parquet(holding_file)
        print(f"Holdings 資料 ({date_str}):")
        print(holdings_df.head(10))
        print(f"\n總共 {len(holdings_df)} 筆持股")
        print(f"\n資料型態:\n{holdings_df.dtypes}")


if __name__ == "__main__":
    # 方式 1: 處理單一 ETF (Headless 模式，會自動清理下載目錄)
    download_and_process_etf("00991A", headless=True)
    
    # 方式 2: 處理單一 ETF (顯示瀏覽器視窗，用於除錯)
    # download_and_process_etf("00991A", headless=False)
    
    # 方式 3: 處理所有 ETF
    # download_and_process_all_etfs(headless=True)
    
    # 方式 4: 讀取已儲存的 Parquet 檔案
    # read_parquet_example("00991A", "20251226")
    
    # 方式 5: 手動清理特定 ETF 的下載目錄
    # clean_download_directory(r"C:\Users\User\Documents\GitHub\ETF_sniper\data\00991A\download")