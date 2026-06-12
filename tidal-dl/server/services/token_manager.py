"""TIDAL 设备码授权 + Token 刷新 API"""
from fastapi import APIRouter, Depends, HTTPException
from database import get_db_dependency
import requests as http_requests
import json
import time
import threading
import logging

logger = logging.getLogger("token_manager")

router = APIRouter(prefix="/api/auth", tags=["Auth"])

# TIDAL OAuth2 客户端凭证 (TV - 支持设备码 + 完整播放权限)
TIDAL_CLIENT_ID = "7m7Ap0JC9j1cOM3n"
TIDAL_CLIENT_SECRET = "vRAdA108tlvkJpTsGZS8rGZ7xTlbJ0qaZ2K9saEzsgY="
TIDAL_AUTH_URL = "https://auth.tidal.com/v1/oauth2"

# 存储正在进行的设备码授权会话
pending_auths = {}  # session_id -> { device_code, user_code, verify_url, status, token_data }


@router.post("/device-code")
def start_device_auth():
    """发起设备码授权 —— 返回验证链接让用户在浏览器中登录"""
    try:
        resp = http_requests.post(
            f"{TIDAL_AUTH_URL}/device_authorization",
            data={"client_id": TIDAL_CLIENT_ID, "scope": "r_usr w_usr"},
            verify=False,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        session_id = data["deviceCode"][:16]
        verify_url = data.get("verificationUriComplete", f"https://link.tidal.com/{data['userCode']}")
        if not verify_url.startswith("http"):
            verify_url = "https://" + verify_url

        pending_auths[session_id] = {
            "device_code": data["deviceCode"],
            "user_code": data["userCode"],
            "verify_url": verify_url,
            "interval": data.get("interval", 5),
            "expires_in": data.get("expiresIn", 300),
            "status": "pending",
            "token_data": None,
            "error": None,
        }

        # 启动后台轮询线程
        thread = threading.Thread(target=_poll_for_token, args=(session_id,), daemon=True)
        thread.start()

        return {
            "session_id": session_id,
            "user_code": data["userCode"],
            "verify_url": pending_auths[session_id]["verify_url"],
            "expires_in": data.get("expiresIn", 300),
            "message": "请在浏览器中打开链接并登录 TIDAL 账号",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取设备码失败: {str(e)}")


@router.get("/device-code/{session_id}")
def check_device_auth(session_id: str, db=Depends(get_db_dependency)):
    """检查设备码授权状态"""
    session = pending_auths.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在或已过期")

    if session["status"] == "completed" and session["token_data"]:
        token = session["token_data"]
        logger.info(f"设备码授权完成，保存 token... keys={list(token.keys())}")

        try:
            account_id = _save_account_from_token(db, token)
            logger.info(f"✅ 账号保存成功: account_id={account_id}")
        except Exception as e:
            logger.error(f"❌ 保存账号失败: {e}", exc_info=True)
            # 清理会话
            del pending_auths[session_id]
            return {
                "status": "error",
                "error": f"Token 获取成功但保存失败: {str(e)}",
            }

        # 清理会话
        del pending_auths[session_id]

        return {
            "status": "completed",
            "message": "授权成功，账号已自动添加",
            "account_id": account_id,
        }

    return {
        "status": session["status"],
        "error": session.get("error"),
        "user_code": session.get("user_code"),
        "verify_url": session.get("verify_url"),
    }


@router.post("/refresh/{account_id}")
def refresh_token(account_id: int, db=Depends(get_db_dependency)):
    """手动刷新指定账号的 Token"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, refresh_token, client_id, oauth_client_id FROM tidal_accounts WHERE id = %s",
        (account_id,)
    )
    account = cursor.fetchone()
    if not account:
        raise HTTPException(status_code=404, detail="账号不存在")

    if not account["refresh_token"]:
        raise HTTPException(status_code=400, detail="该账号没有 refresh_token")

    # 尝试刷新（优先用 oauth_client_id）
    result = _do_refresh_token(account["refresh_token"], oauth_client_id=account.get("oauth_client_id"))
    if result.get("error"):
        # 刷新失败
        cursor.execute(
            "UPDATE tidal_accounts SET status = 'refresh_failed', "
            "error_message = %s, updated_at = NOW() WHERE id = %s",
            (result.get("error_description", str(result)), account_id)
        )
        db.commit()
        raise HTTPException(status_code=400, detail=f"刷新失败: {result.get('error_description')}")

    # 刷新成功
    from datetime import datetime, timedelta
    expires_at = datetime.now() + timedelta(seconds=result.get("expires_in", 86400))

    cursor.execute(
        "UPDATE tidal_accounts SET access_token = %s, "
        "token_expires_at = %s, status = 'active', error_message = '', "
        "updated_at = NOW() WHERE id = %s",
        (result["access_token"], expires_at.strftime("%Y-%m-%d %H:%M:%S"), account_id)
    )
    # 如果返回了新的 refresh_token，也更新
    if result.get("refresh_token"):
        cursor.execute(
            "UPDATE tidal_accounts SET refresh_token = %s WHERE id = %s",
            (result["refresh_token"], account_id)
        )
    db.commit()

    return {
        "message": "Token 刷新成功",
        "expires_in": result.get("expires_in"),
        "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _poll_for_token(session_id: str):
    """后台轮询设备码授权结果"""
    session = pending_auths.get(session_id)
    if not session:
        return

    interval = session["interval"]
    max_polls = session["expires_in"] // interval

    for i in range(max_polls):
        time.sleep(interval)

        if session_id not in pending_auths:
            return

        try:
            resp = http_requests.post(
                f"{TIDAL_AUTH_URL}/token",
                data={
                    "client_id": TIDAL_CLIENT_ID,
                    "client_secret": TIDAL_CLIENT_SECRET,
                    "device_code": session["device_code"],
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "scope": "r_usr w_usr",
                },
                verify=False,
                timeout=10,
            )

            if resp.status_code == 200:
                session["status"] = "completed"
                session["token_data"] = resp.json()
                logger.info(f"设备码授权成功: session={session_id}")
                return

            err = resp.json()
            sub_status = err.get("sub_status")

            if sub_status == 1002:  # authorization_pending
                continue
            elif sub_status == 1000:  # expired
                session["status"] = "expired"
                session["error"] = "设备码已过期"
                return
            else:
                session["status"] = "error"
                session["error"] = err.get("error_description", str(err))
                return

        except Exception as e:
            logger.error(f"轮询异常: {e}")
            continue

    session["status"] = "expired"
    session["error"] = "授权超时"


def _do_refresh_token(refresh_token: str, oauth_client_id: str = None) -> dict:
    """执行 token 刷新
    oauth_client_id: 用户从 curl 请求中复制的真正 OAuth client_id
    如果有 oauth_client_id → 用它刷新（Web/公开客户端，无需 secret）
    如果没有 → 用 TV client_id + secret 刷新（设备码授权的账号）
    """
    if oauth_client_id:
        # Web / 公开客户端：只需 client_id
        data = {
            "client_id": oauth_client_id,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        logger.info(f"刷新使用 oauth_client_id: {oauth_client_id}")
    else:
        # TV 客户端（设备码授权）：需要 client_id + client_secret
        data = {
            "client_id": TIDAL_CLIENT_ID,
            "client_secret": TIDAL_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        logger.info(f"刷新使用 TV client_id: {TIDAL_CLIENT_ID}")

    try:
        resp = http_requests.post(
            f"{TIDAL_AUTH_URL}/token",
            data=data,
            verify=False,
            timeout=10,
        )
        return resp.json()
    except Exception as e:
        return {"error": "network_error", "error_description": str(e)}


def _save_account_from_token(db, token_data: dict) -> int:
    """从 token 数据自动创建账号"""
    import base64
    access_token = token_data.get("access_token", "")
    refresh_token = token_data.get("refresh_token", "")
    expires_in = token_data.get("expires_in", 86400)
    user_id = token_data.get("user", {}).get("userId", 0)
    country_code = token_data.get("user", {}).get("countryCode", "NG")
    email = token_data.get("user", {}).get("email", "")

    # 解析 JWT 获取更多信息
    try:
        parts = access_token.split(".")
        if len(parts) == 3:
            payload = parts[1]
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += "=" * padding
            claims = json.loads(base64.urlsafe_b64decode(payload))
            if not user_id:
                user_id = claims.get("uid", 0)
            if not country_code:
                country_code = claims.get("cc", "NG")
    except Exception:
        pass

    from datetime import datetime, timedelta
    expires_at = datetime.now() + timedelta(seconds=expires_in)

    cursor = db.cursor()

    # 检查是否已存在
    if user_id:
        cursor.execute("SELECT id FROM tidal_accounts WHERE user_id = %s", (user_id,))
        existing = cursor.fetchone()
        if existing:
            # 更新已有账号
            cursor.execute(
                "UPDATE tidal_accounts SET access_token = %s, refresh_token = %s, "
                "token_expires_at = %s, status = 'active', error_message = '', "
                "updated_at = NOW() WHERE id = %s",
                (access_token, refresh_token, expires_at.strftime("%Y-%m-%d %H:%M:%S"),
                 existing["id"])
            )
            db.commit()
            return existing["id"]

    # 创建新账号
    cursor.execute(
        "INSERT INTO tidal_accounts "
        "(email, user_id, country_code, access_token, refresh_token, "
        "token_expires_at, subscription_type, highest_quality, status) "
        "VALUES (%s, %s, %s, %s, %s, %s, 'PREMIUM', 'LOSSLESS', 'active')",
        (email, user_id, country_code, access_token, refresh_token,
         expires_at.strftime("%Y-%m-%d %H:%M:%S"))
    )
    db.commit()
    return cursor.lastrowid


def auto_refresh_loop():
    """后台自动刷新 Token（每 5 分钟检查一次）"""
    from database import get_db
    from datetime import datetime, timedelta

    logger.info("🔄 Token 自动刷新服务已启动（每 5 分钟检查）")

    while True:
        time.sleep(300)  # 5 分钟
        try:
            with get_db() as db:
                cursor = db.cursor()

                # 查找即将过期的 active 账号（30 分钟内过期）
                # 也查找 token_expired 状态但有 refresh_token 的账号（尝试恢复）
                threshold = (datetime.now() + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(
                    "SELECT id, email, refresh_token, oauth_client_id FROM tidal_accounts "
                    "WHERE status IN ('active', 'token_expired') AND refresh_token IS NOT NULL "
                    "AND refresh_token != '' AND token_expires_at < %s",
                    (threshold,)
                )
                accounts = cursor.fetchall()

                for acc in accounts:
                    logger.info(f"🔄 自动刷新: {acc['email']} (ID={acc['id']}, oauth={acc.get('oauth_client_id')})")
                    result = _do_refresh_token(acc["refresh_token"], oauth_client_id=acc.get("oauth_client_id"))

                    if result.get("error"):
                        logger.warning(f"  ❌ 刷新失败: {result.get('error_description', result.get('error'))}")
                        continue

                    expires_at = datetime.now() + timedelta(seconds=result.get("expires_in", 86400))
                    cursor.execute(
                        "UPDATE tidal_accounts SET access_token = %s, "
                        "token_expires_at = %s, status = 'active', "
                        "error_message = '', updated_at = NOW() WHERE id = %s",
                        (result["access_token"], expires_at.strftime("%Y-%m-%d %H:%M:%S"), acc["id"])
                    )
                    if result.get("refresh_token"):
                        cursor.execute(
                            "UPDATE tidal_accounts SET refresh_token = %s WHERE id = %s",
                            (result["refresh_token"], acc["id"])
                        )
                    db.commit()
                    logger.info(f"  ✅ 刷新成功，新过期时间: {expires_at}")

        except Exception as e:
            logger.error(f"自动刷新异常: {e}", exc_info=True)


