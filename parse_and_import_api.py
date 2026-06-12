import json
import re
import requests
from datetime import datetime, timedelta

API_URL = "http://117.55.199.208:8000/api/accounts"

def main():
    with open('account_cleaned.txt', 'r', encoding='utf-8') as f:
        content = f.read()

    accounts_to_import = []
    
    # Split the content by the email,Eee123 pattern
    pattern = re.compile(r'^([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+),(Eee123)\s*$', re.MULTILINE)
    
    matches = list(pattern.finditer(content))
    
    for i in range(len(matches)):
        email = matches[i].group(1)
        password = matches[i].group(2)
        
        start_idx = matches[i].end()
        end_idx = matches[i+1].start() if i + 1 < len(matches) else len(content)
        
        block = content[start_idx:end_idx].strip()
        
        # Extract json part from block
        json_start = block.find('{')
        json_end = block.rfind('}')
        
        if json_start != -1 and json_end != -1:
            json_str = block[json_start:json_end+1]
            try:
                data = json.loads(json_str)
                access_token = data.get('access_token')
                refresh_token = data.get('refresh_token')
                country_code = data.get('user', {}).get('countryCode', 'US')
                user_id = data.get('user_id')
                
                # Default expiration is 4 hours from now roughly
                expires_at = (datetime.now() + timedelta(seconds=data.get('expires_in', 14400))).strftime("%Y-%m-%d %H:%M:%S")
                
                if access_token and refresh_token:
                    accounts_to_import.append({
                        "email": email,
                        "user_id": user_id,
                        "country_code": country_code,
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "token_expires_at": expires_at,
                        "client_id": 8049,
                        "subscription_type": "PREMIUM",
                        "highest_quality": "LOSSLESS"
                    })
            except json.JSONDecodeError as e:
                print(f"解析 {email} 的 JSON 失败: {e}")
                
    print(f"成功提取了 {len(accounts_to_import)} 个账号数据。")
    
    if not accounts_to_import:
        return
        
    print("正在通过 API 导入到服务器...")
    success_count = 0
    for acc in accounts_to_import:
        try:
            resp = requests.post(API_URL, json=acc, timeout=10)
            if resp.status_code in [200, 201]:
                print(f"✅ 成功导入账号: {acc['email']}")
                success_count += 1
            else:
                print(f"❌ 导入账号 {acc['email']} 失败: HTTP {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"❌ 导入账号 {acc['email']} 发生网络异常: {e}")
            
    print(f"\n全部导入完成！成功将 {success_count} 个账号写入线上服务器。")

if __name__ == "__main__":
    main()
