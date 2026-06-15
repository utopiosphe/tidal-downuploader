#!/usr/bin/env python3
"""
验证 TIDAL access token 并检查账号订阅状态
"""
import json
import urllib.request
import urllib.error
import ssl
import sys
from datetime import datetime

# 解决 macOS Python SSL 证书验证问题
ssl._create_default_https_context = ssl._create_unverified_context

# Token from the user's web player session
ACCESS_TOKEN = "eyJraWQiOiJ2OU1GbFhqWSIsImFsZyI6IkVTMjU2In0.eyJ0eXBlIjoibzJfYWNjZXNzIiwidWlkIjoyMDg1Mjc5MDEsInNjb3BlIjoicl91c3Igd191c3IiLCJnVmVyIjowLCJzVmVyIjowLCJjaWQiOjgwNDksImN1ayI6IjkwZGM1MzY0LWNhMjAtNDAyZi04M2Q0LWI2MzVhOTVlZmZmNCIsImNjIjoiTkciLCJhdCI6IklOVEVSTkFMIiwiZXhwIjoxNzgxMDEyNTg4LCJzaWQiOiI0MGM0MTQzOS00N2U3LTQ4ZjYtYWUyMy1lZTE1NGQ0OGFlNTMiLCJpc3MiOiJodHRwczovL2F1dGgudGlkYWwuY29tL3YxIn0.GnYqWXlPf64k4KkNf1TJcd7MSuAoAQnH-S_o1fVpz5vt20V1Rs4HrvW8XYSAWyVb72D9nAOsfYMO8sankseQzQ"

BASE_URL = "https://api.tidal.com/v1"

def make_request(url, token):
    """发送认证请求"""
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8")), resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return {"error": body, "status": e.code, "reason": e.reason}, e.code

def decode_jwt_claims(token):
    """解码 JWT 的 payload 部分（不验证签名）"""
    import base64
    parts = token.split(".")
    if len(parts) != 3:
        return None
    payload = parts[1]
    # 补齐 base64 padding
    padding = 4 - len(payload) % 4
    if padding != 4:
        payload += "=" * padding
    decoded = base64.urlsafe_b64decode(payload)
    return json.loads(decoded)

def main():
    print("=" * 60)
    print("TIDAL Token 验证与订阅状态检查")
    print("=" * 60)
    
    # 1. 解码 JWT
    print("\n[1] 解码 JWT Claims...")
    claims = decode_jwt_claims(ACCESS_TOKEN)
    if claims:
        print(json.dumps(claims, indent=2))
        exp = claims.get("exp")
        if exp:
            exp_dt = datetime.fromtimestamp(exp)
            now = datetime.now()
            print(f"\n  Token 过期时间: {exp_dt}")
            print(f"  当前时间:       {now}")
            if now > exp_dt:
                print("  ⚠️  Token 已过期！需要用 refresh_token 刷新")
            else:
                remaining = exp_dt - now
                print(f"  ✅ Token 有效，剩余 {remaining}")
    
    # 2. 获取用户信息
    print("\n[2] 获取用户信息 (/users/208527901)...")
    user_data, status = make_request(f"{BASE_URL}/users/208527901", ACCESS_TOKEN)
    print(f"  HTTP Status: {status}")
    print(f"  Response: {json.dumps(user_data, indent=2, ensure_ascii=False)}")
    
    # 3. 获取订阅信息
    print("\n[3] 获取订阅信息 (/users/208527901/subscription)...")
    sub_data, status = make_request(f"{BASE_URL}/users/208527901/subscription", ACCESS_TOKEN)
    print(f"  HTTP Status: {status}")
    print(f"  Response: {json.dumps(sub_data, indent=2, ensure_ascii=False)}")
    
    # 4. 尝试获取一首热门歌曲的播放信息
    # 使用 Adele - Hello (track ID: 45503336) 作为测试
    print("\n[4] 测试获取曲目播放信息...")
    
    # 先获取曲目元数据
    track_id = 45503336  # Adele - Hello
    track_url = f"{BASE_URL}/tracks/{track_id}?countryCode=NG"
    track_data, status = make_request(track_url, ACCESS_TOKEN)
    print(f"  曲目元数据 HTTP Status: {status}")
    if status == 200:
        print(f"  曲目: {track_data.get('title')} - {track_data.get('artist', {}).get('name', 'Unknown')}")
        print(f"  可用音质: {track_data.get('audioQuality')}")
        print(f"  时长: {track_data.get('duration')}s")
    else:
        print(f"  Response: {json.dumps(track_data, indent=2, ensure_ascii=False)}")
    
    # 5. 尝试请求播放流信息（这是关键的边界测试）
    print("\n[5] 测试请求播放流信息 (playbackinfopostpaywall)...")
    qualities = ["LOW", "HIGH", "LOSSLESS", "HI_RES", "HI_RES_LOSSLESS"]
    for quality in qualities:
        stream_url = f"{BASE_URL}/tracks/{track_id}/playbackinfopostpaywall?audioquality={quality}&playbackmode=STREAM&assetpresentation=FULL&countryCode=NG"
        stream_data, status = make_request(stream_url, ACCESS_TOKEN)
        if status == 200:
            print(f"  ✅ {quality}: 可用 | 实际音质={stream_data.get('audioQuality')} | 编码={stream_data.get('audioMode')} | manifest类型={stream_data.get('manifestMimeType')}")
            # 查看 manifest 内容
            manifest = stream_data.get("manifest")
            if manifest:
                import base64
                try:
                    decoded_manifest = base64.b64decode(manifest).decode("utf-8")
                    manifest_json = json.loads(decoded_manifest)
                    print(f"         编解码器={manifest_json.get('codecs')} | MIME={manifest_json.get('mimeType')}")
                    urls = manifest_json.get("urls", [])
                    if urls:
                        print(f"         URL数量={len(urls)} | URL前100字符={urls[0][:100]}...")
                except:
                    print(f"         manifest (raw): {manifest[:80]}...")
        else:
            error_msg = stream_data.get("error", stream_data)
            print(f"  ❌ {quality}: 失败(HTTP {status}) | {error_msg}")
    
    # 6. 搜索功能测试
    print("\n[6] 测试搜索功能...")
    search_url = f"{BASE_URL}/search?query=Adele&limit=3&countryCode=NG&types=TRACKS"
    search_data, status = make_request(search_url, ACCESS_TOKEN)
    print(f"  HTTP Status: {status}")
    if status == 200:
        tracks = search_data.get("tracks", {}).get("items", [])
        for t in tracks[:3]:
            print(f"  - {t.get('title')} by {t.get('artist', {}).get('name')} | ID={t.get('id')} | Quality={t.get('audioQuality')}")
    
    print("\n" + "=" * 60)
    print("验证完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
