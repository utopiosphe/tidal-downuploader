import json
import pymysql
import os

DB_HOST = "117.55.199.208"
DB_PORT = 3306
DB_USER = "tidal"
DB_PASS = "Zt8520.."
DB_NAME = "tidal_dl"

INPUT_FILE = "authorized_accounts.json"

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"找不到 {INPUT_FILE}，请先运行 batch_authorize.py 进行授权。")
        return
        
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    if not data:
        print("没有可导入的授权数据。")
        return
        
    print(f"准备连接远程数据库 {DB_HOST}，即将导入 {len(data)} 个账号...")
    
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
    
    for email, info in data.items():
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
                email, 
                info["password"], 
                info["access_token"], 
                info["refresh_token"], 
                info["country_code"]
            ))
            success_count += 1
        except Exception as e:
            print(f"插入账号 {email} 时出错: {e}")
            
    conn.commit()
    conn.close()
    
    print(f"\n✅ 导入完成！成功将 {success_count} 个账号写入远程数据库。")
    print("您可以启动或重启 Worker 开始处理下载任务了。")

if __name__ == "__main__":
    main()
