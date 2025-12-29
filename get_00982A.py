from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import gc
from pathlib import Path
import os
import shutil
from datetime import datetime

# 設定下載路徑
download_path = r"C:\Users\User\Documents\GitHub\ETF_sniper\data\00982A\download"
portfolio_path = r"C:\Users\User\Documents\GitHub\ETF_sniper\data\00982A\portfolio"
holding_path = r"C:\Users\User\Documents\GitHub\ETF_sniper\data\00982A\holding"

# 確保所有目錄存在
os.makedirs(download_path, exist_ok=True)
os.makedirs(portfolio_path, exist_ok=True)
os.makedirs(holding_path, exist_ok=True)

# 設定 Chrome 選項
chrome_options = Options()
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--window-size=1920,1080')
prefs = {
    "download.default_directory": download_path,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

# 啟動瀏覽器
driver = webdriver.Chrome(options=chrome_options)

try:
    # 開啟網頁
    url = "https://www.capitalfund.com.tw/etf/product/detail/399/portfolio"
    driver.get(url)
    
    # 等待頁面載入
    wait = WebDriverWait(driver, 10)
    
    # 找到並點擊下載按鈕
    download_button = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.buyback-search-section-btn"))
    )
    download_button.click()
    
    print("已點擊下載按鈕，等待下載完成...")
    
    # 等待下載完成
    time.sleep(5)
    
    # 取得下載的檔案並重命名
    files = os.listdir(download_path)
    if files:
        # 找到最新下載的檔案
        latest_file = max([os.path.join(download_path, f) for f in files], 
                         key=os.path.getctime)
        
        # 取得檔案副檔名
        file_extension = os.path.splitext(latest_file)[1]
        
        # 產生新檔名 (當前日期)
        date_str = datetime.now().strftime("%Y%m%d")
        new_filename = date_str + file_extension
        new_filepath = os.path.join(download_path, new_filename)
        
        # 重命名檔案
        if os.path.exists(new_filepath):
            os.remove(new_filepath)
        os.rename(latest_file, new_filepath)
        print(f"檔案已下載並重命名為: {new_filename}")
        
        # ========== 資料處理 ==========
        print("\n開始處理資料...")
        
        # 讀取 Excel 檔案
        excel_file = pd.ExcelFile(new_filepath)
        
        # 處理分頁1和3 - Portfolio
        print("處理投資組合資料 (分頁1和3)...")
        
        # 讀取分頁1 (投資組合)
        df_portfolio = pd.read_excel(new_filepath, sheet_name='投資組合', header=None)
        
        # 轉換為字典格式
        portfolio_data = {}
        for idx, row in df_portfolio.iterrows():
            if pd.notna(row[0]):
                portfolio_data[row[0]] = row[1] if pd.notna(row[1]) else ""
        
        # 讀取分頁3 (其他資產)
        df_other = pd.read_excel(new_filepath, sheet_name='其他資產', header=None)
        
        # 轉換為字典格式
        other_data = {}
        for idx, row in df_other.iterrows():
            if pd.notna(row[0]):
                other_data[row[0]] = row[1] if pd.notna(row[1]) else ""
        
        # 合併資料
        combined_portfolio = {**portfolio_data, **other_data}
        
        df_combined_portfolio = pd.DataFrame(list(combined_portfolio.items()), 
                                            columns=['項目', '金額'])

        # ========== 加入資料清理邏輯 ==========

        # 1. 確保「金額」是字串，方便進行文字替換
        df_combined_portfolio['金額'] = df_combined_portfolio['金額'].astype(str)

        # 2. 移除 'TWD'、' ' (空白) 以及 ',' (逗號)
        # 我們使用 regex=True 一次處理多種字元
        df_combined_portfolio['金額'] = (
            df_combined_portfolio['金額']
            .str.replace(r'[TWD,\s]', '', regex=True) # 移除 T, W, D, 逗號 與 空白
        )

        # 3. 轉換為數值格式 (使用 pd.to_numeric)
        # errors='coerce' 可以將無法轉換的文字變為 NaN，避免程式崩潰
        df_combined_portfolio['金額'] = pd.to_numeric(df_combined_portfolio['金額'], errors='coerce')

        # (可選) 填補缺失值，例如轉為 0
        df_combined_portfolio['金額'] = df_combined_portfolio['金額'].fillna(0)

        portfolio_output = os.path.join(portfolio_path, f"{date_str}.parquet")
        df_combined_portfolio.to_parquet(portfolio_output, index=False, engine='pyarrow')

        # 處理分頁2 - Holding (股票持股)
        print("處理持股資料 (分頁2)...")
        
        # 讀取分頁2 (股票)
        df_holding = pd.read_excel(new_filepath, sheet_name='股票')
        
        # 資料清理
        # 1. 移除符號並轉數值
        if '持股權重(%)' in df_holding.columns:
                    # 1. 建立新欄位並計算數值，同時確保是字串後再處理
                    df_holding['持股權重'] = (
                        df_holding['持股權重(%)']
                        .astype(str)
                        .str.replace('%', '')
                        .str.strip()
                        .astype(float) * 0.01
                    )


        # 確保股票代號為字串格式 (避免前導0消失)
        if '股數' in df_holding.columns:
            # 先轉為字串，取代掉逗號，再轉為浮點數或整數
            df_holding['股數'] = df_holding['股數'].astype(str).str.replace(',', '').str.strip().astype(float)

        # 3. 確保股票代號為字串格式
        if '股票代號' in df_holding.columns:
            df_holding['股票代號'] = df_holding['股票代號'].astype(str)
        
        cols = ['股票代號', '股票名稱', '持股權重', '股數']
        df_holding = df_holding[cols]
        
        # 儲存 Holding 資料為 Parquet
        holding_output = os.path.join(holding_path, f"{date_str}.parquet")
        df_holding.to_parquet(holding_output, index=False, engine='pyarrow')
        print(f"持股資料已儲存至: {holding_output}")
        
        print("\n資料處理完成！")
        print(f"- Portfolio 檔案: {portfolio_output}")
        print(f"- Holding 檔案: {holding_output}")
        
        # ========== 清空 download 目錄 ==========
        print("\n清空 download 目錄...")
        for filename in os.listdir(download_path):
            file_path = os.path.join(download_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                    print(f"已刪除: {filename}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"已刪除目錄: {filename}")
            except Exception as e:
                print(f'刪除 {file_path} 失敗. 原因: {e}')
        
        print("download 目錄已清空！")
        
    else:
        print("沒有找到下載的檔案")
        
finally:
    # 關閉瀏覽器
    driver.quit()
    print("\n瀏覽器已關閉")
    print("="*60)
    print("所有作業完成！")