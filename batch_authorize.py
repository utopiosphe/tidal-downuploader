import requests
import time
import json
import os
import webbrowser
import subprocess

TIDAL_CLIENT_ID = "7m7Ap0JC9j1cOM3n"
TIDAL_CLIENT_SECRET = "vRAdA108tlvkJpTsGZS8rGZ7xTlbJ0qaZ2K9saEzsgY="
TIDAL_AUTH_URL = "https://auth.tidal.com/v1/oauth2"

ACCOUNTS_FILE = "account_cleaned.txt"
OUTPUT_FILE = "authorized_accounts.json"

def copy_to_clipboard(text):
    try:
        process = subprocess.Popen(['pbcopy'], stdin=subprocess.PIPE)
        process.communicate(text.encode('utf-8'))
    except Exception:
        pass

def load_accounts():
    accounts = []
    if not os.path.exists(ACCOUNTS_FILE):
        print(f"找不到 {ACCOUNTS_FILE} 文件！")
        return accounts
        
    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if "," in line:
                email, pwd = line.split(",", 1)
                accounts.append((email.strip(), pwd.strip()))
    return accounts

def load_completed():
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_completed(completed_data):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(completed_data, f, indent=4, ensure_ascii=False)

def authorize_account(email, password):
    print(f"\n=============================================")
    print(f"🚀 准备授权账号: {email}")
    print(f"=============================================")
    
    # 1. 获取设备码
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                f"{TIDAL_AUTH_URL}/device_authorization",
                data={"client_id": TIDAL_CLIENT_ID, "scope": "r_usr w_usr"},
                timeout=10,
            )
            resp.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️ 获取设备码网络异常，正在重试... ({e})")
                time.sleep(2)
            else:
                print(f"\n❌ 获取设备码失败，请检查本地网络: {e}")
                return None
                
    data = resp.json()
    
    device_code = data["deviceCode"]
    user_code = data["userCode"]
    verify_url = data.get("verificationUriComplete", f"https://link.tidal.com/{user_code}")
    if not verify_url.startswith("http"):
        verify_url = "https://" + verify_url
        
    print(f"🔗 验证链接: {verify_url}")
    print(f"📋 密码: {password}  (已自动复制到剪贴板！)")
    
    # 复制密码到剪贴板，方便用户粘贴
    copy_to_clipboard(password)
    
    # 使用 Playwright 启动一个无缓存的全新浏览器窗口，避免 Cookie 串号
    print("\n⏳ 正在自动打开全新的无痕浏览器窗口...")
    print("   (在浏览器中填入邮箱，粘贴密码，然后点击确认即可，本窗口会自动感应)")
    
    # 尝试在 macOS 上打开 Chrome 的无痕模式，并显式指定本地 Clash 代理 (默认 7890 端口)
    try:
        import subprocess
        subprocess.run([
            'open', '-na', 'Google Chrome', '--args', 
            '--incognito', 
            '--proxy-server=http://127.0.0.1:7890', 
            verify_url
        ], check=True)
    except Exception:
        # 如果没有安装 Chrome，回退到默认浏览器
        import webbrowser
        print("⚠️ 未检测到 Google Chrome，将使用系统默认浏览器打开。")
        print("   【重要提示】：请确保您在浏览器中手动退出了上一个 Tidal 账号，或复制链接到无痕模式打开！")
        webbrowser.open(verify_url)
        
    interval = data.get("interval", 5)
    result_data = None
    
    # 2. 轮询 Token
    for i in range(120): # 最多等待10分钟
        time.sleep(interval)
        try:
            token_resp = requests.post(
                f"{TIDAL_AUTH_URL}/token",
                data={
                    "client_id": TIDAL_CLIENT_ID,
                    "client_secret": TIDAL_CLIENT_SECRET,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "scope": "r_usr w_usr",
                },
                timeout=10,
            )
            
            if token_resp.status_code == 200:
                token_data = token_resp.json()
                print(f"\n🎉 账号 {email} 授权成功！")
                result_data = {
                    "email": email,
                    "password": password,
                    "access_token": token_data.get("access_token"),
                    "refresh_token": token_data.get("refresh_token"),
                    "country_code": token_data.get("user", {}).get("countryCode", "US"),
                    "user_id": token_data.get("user", {}).get("userId")
                }
                break
                
            err = token_resp.json()
            if err.get("sub_status") == 1002: # authorization_pending
                print(".", end="", flush=True)
                continue
            elif err.get("sub_status") == 1000: # expired
                print("\n❌ 设备验证码已过期，请稍后重试该账号。")
                break
            else:
                print(f"\n❌ 发生错误: {err}")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"\n⚠️ 网络请求异常，正在重试... ({e})")
            time.sleep(interval)
            
    if not result_data:
        print("\n❌ 授权流程未成功。")
        
    return result_data

def main():
    print("=== Tidal 批量账号半自动授权助手 ===")
    accounts = load_accounts()
    if not accounts:
        print("没有可用的账号，请检查 account_cleaned.txt")
        return
        
    completed = load_completed()
    print(f"共加载 {len(accounts)} 个账号，其中已授权 {len(completed)} 个。")
    
    for email, password in accounts:
        if email in completed:
            print(f"⏭️ 账号 {email} 已授权，跳过。")
            continue
            
        result = authorize_account(email, password)
        if result:
            completed[email] = result
            save_completed(completed)
        else:
            ans = input("授权失败或超时，是否继续处理下一个账号？(y/n): ")
            if ans.lower() != 'y':
                break
                
    print(f"\n✅ 批量授权任务结束！所有成功的账号信息已保存在 {OUTPUT_FILE} 中。")
    print("届时我们会将该 JSON 文件统一导入到服务器数据库。")

if __name__ == "__main__":
    main()
