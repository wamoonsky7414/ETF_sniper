from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import json
import time

# 設定 Chrome Options（新版語法）
chrome_options = Options()
chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
# chrome_options.add_argument('--headless')  # 先不用 headless 方便除錯

driver = webdriver.Chrome(options=chrome_options)

try:
    print("正在載入頁面...")
    driver.get("https://www.capitalfund.com.tw/etf/product/detail/399/portfolio")
    
    # 等待頁面載入完成
    time.sleep(8)
    
    print("\n" + "="*80)
    print("分析 Network 請求...")
    print("="*80 + "\n")
    
    # 取得所有 Network logs
    logs = driver.get_log('performance')
    
    for log in logs:
        try:
            message = json.loads(log['message'])
            method = message['message']['method']
            
            # 只關注 Network 相關的事件
            if method == 'Network.requestWillBeSent':
                request = message['message']['params']['request']
                url = request['url']
                
                # 只顯示 buyback API 的請求
                if 'buyback' in url:
                    print(f"🔍 找到 buyback 請求！")
                    print(f"URL: {url}")
                    print(f"Method: {request['method']}")
                    print(f"\n📋 Headers:")
                    for key, value in request['headers'].items():
                        print(f"  {key}: {value}")
                    
                    if 'postData' in request:
                        print(f"\n📦 POST Data:")
                        print(f"  {request['postData']}")
                    
                    print("\n" + "-"*80 + "\n")
            
            # 取得回應內容
            elif method == 'Network.responseReceived':
                response = message['message']['params']['response']
                if 'buyback' in response['url']:
                    request_id = message['message']['params']['requestId']
                    
                    print(f"✅ 收到 buyback 回應")
                    print(f"Status: {response['status']}")
                    print(f"Content-Type: {response.get('mimeType', 'N/A')}")
                    
                    # 嘗試取得回應內容
                    try:
                        response_body = driver.execute_cdp_cmd(
                            'Network.getResponseBody',
                            {'requestId': request_id}
                        )
                        
                        if response_body['base64Encoded']:
                            print("回應是 base64 編碼")
                        else:
                            body = response_body['body']
                            print(f"\n📄 Response Body (前 500 字元):")
                            print(body[:500])
                            
                            # 嘗試解析 JSON
                            try:
                                data = json.loads(body)
                                print(f"\n✨ JSON 資料筆數: {len(data)}")
                                if len(data) > 0:
                                    print(f"\n第一筆資料範例:")
                                    print(json.dumps(data[0], indent=2, ensure_ascii=False))
                            except:
                                pass
                    except Exception as e:
                        print(f"無法取得回應內容: {e}")
                    
                    print("\n" + "="*80 + "\n")
        
        except Exception as e:
            continue
    
    print("\n✅ 分析完成！")
    print("\n請截圖或複製上面的資訊給我")
    
except Exception as e:
    print(f"❌ 發生錯誤: {e}")
    import traceback
    traceback.print_exc()

finally:
    input("\n按 Enter 鍵關閉瀏覽器...")
    driver.quit()