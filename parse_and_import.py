import json
import re
import pymysql

DB_HOST = "117.55.199.208"
DB_PORT = 3306
DB_USER = "tidal"
DB_PASS = "Zt8520.."
DB_NAME = "tidal_dl"

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
                
                if access_token and refresh_token:
                    accounts_to_import.append({
                        "email": email,
                        "password": password,
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "country_code": country_code
                    })
            except json.JSONDecodeError as e:
                print(f"解析 {email} 的 JSON 失败: {e}")
                
    print(f"成功解析了 {len(accounts_to_import)} 个账号的数据。")
    
    if not accounts_to_import:
        return
        
    print("正在导入到远程数据库...")
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return
        
    cursor = conn.cursor()
    success_count = 0
    
    for info in accounts_to_import:
        try:
            cursor.execute("""
                INSERT INTO accounts 
                (email, password, access_token, refresh_token, country_code, status)
                VALUES (%s, %s, %s, %s, %s, 'active')
                ON DUPLICATE KEY UPDATE
                access_token = VALUES(access_token),
                refresh_token = VALUES(refresh_token),
                password = VALUES(password),
                country_code = VALUES(country_code),
                status = 'active'
            """, (
                info["email"], 
                info["password"], 
                info["access_token"], 
                info["refresh_token"], 
                info["country_code"]
            ))
            success_count += 1
        except Exception as e:
            print(f"插入账号 {info['email']} 时出错: {e}")
            
    conn.commit()
    conn.close()
    
    print(f"✅ 导入完成！成功将 {success_count} 个账号写入远程数据库。")

if __name__ == "__main__":
    main()
