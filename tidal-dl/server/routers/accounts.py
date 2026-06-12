"""TIDAL 账号管理 API"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from database import get_db_dependency

router = APIRouter(prefix="/api/accounts", tags=["Accounts"])


class AccountCreate(BaseModel):
    email: Optional[str] = ""
    user_id: Optional[int] = None
    country_code: str = "NG"
    access_token: str
    refresh_token: Optional[str] = ""
    token_expires_at: Optional[str] = None
    client_id: int = 8049
    oauth_client_id: Optional[str] = "49YxDN9a2aFV6RTG"
    subscription_type: Optional[str] = "PREMIUM"
    subscription_expires: Optional[str] = None
    highest_quality: Optional[str] = "LOSSLESS"


@router.get("")
def list_accounts(db=Depends(get_db_dependency)):
    """获取所有账号"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, email, user_id, country_code, status, subscription_type, "
        "subscription_expires, highest_quality, total_downloads, last_used_at, "
        "token_expires_at, error_message, cooldown_until, rate_limit_count, "
        "created_at, updated_at "
        "FROM tidal_accounts ORDER BY id"
    )
    return cursor.fetchall()


@router.get("/available")
def get_available_accounts(db=Depends(get_db_dependency)):
    """获取所有可用账号（Worker 用）"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, access_token, refresh_token, country_code, user_id, "
        "client_id, token_expires_at "
        "FROM tidal_accounts "
        "WHERE status = 'active' AND token_expires_at > NOW() "
        "ORDER BY id"
    )
    return cursor.fetchall()


@router.post("")
def create_account(account: AccountCreate, db=Depends(get_db_dependency)):
    """添加 TIDAL 账号"""
    cursor = db.cursor()
    cursor.execute(
        "INSERT INTO tidal_accounts "
        "(email, user_id, country_code, access_token, refresh_token, oauth_client_id, "
        "token_expires_at, client_id, subscription_type, subscription_expires, "
        "highest_quality, status) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')",
        (account.email, account.user_id, account.country_code,
         account.access_token, account.refresh_token, account.oauth_client_id,
         account.token_expires_at, account.client_id,
         account.subscription_type, account.subscription_expires,
         account.highest_quality)
    )
    db.commit()
    return {"message": "账号添加成功", "id": cursor.lastrowid}


class TokenImport(BaseModel):
    """手动导入 Token
    用户从 F12 获取：
    1. token 响应 JSON 中的 access_token / refresh_token
    2. curl 请求中的 client_id（如 49YxDN9a2aFV6RTG）
    """
    access_token: str
    refresh_token: Optional[str] = ""
    oauth_client_id: Optional[str] = None
    email: Optional[str] = None


@router.post("/import-token")
def import_token(data: TokenImport, db=Depends(get_db_dependency)):
    """手动导入 Token（自动解析 JWT，存储 OAuth client_id 用于自动续期）"""
    import base64, json
    from datetime import datetime, timedelta

    token = data.access_token.strip()
    user_id = 0
    country_code = "NG"
    client_id = 0  # JWT 内部 cid（数字）
    expires_at = datetime.now() + timedelta(hours=4)

    # 解析 JWT
    try:
        parts = token.split(".")
        if len(parts) == 3:
            payload = parts[1]
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += "=" * padding
            claims = json.loads(base64.urlsafe_b64decode(payload))
            user_id = claims.get("uid", 0)
            country_code = claims.get("cc", "NG")
            client_id = claims.get("cid", 0)
            if claims.get("exp"):
                expires_at = datetime.fromtimestamp(claims["exp"])
    except Exception:
        pass

    # oauth_client_id：优先用用户提供的，否则留空
    oauth_client_id = data.oauth_client_id.strip() if data.oauth_client_id else None

    cursor = db.cursor()

    # 检查是否已存在
    if user_id:
        cursor.execute("SELECT id FROM tidal_accounts WHERE user_id = %s", (user_id,))
        existing = cursor.fetchone()
        if existing:
            update_sql = (
                "UPDATE tidal_accounts SET access_token = %s, refresh_token = %s, "
                "token_expires_at = %s, client_id = %s, status = 'active', "
                "error_message = '', rate_limit_count = 0, updated_at = NOW()"
            )
            params = [token, data.refresh_token,
                      expires_at.strftime("%Y-%m-%d %H:%M:%S"), client_id]
            if oauth_client_id:
                update_sql += ", oauth_client_id = %s"
                params.append(oauth_client_id)
            update_sql += " WHERE id = %s"
            params.append(existing["id"])
            cursor.execute(update_sql, params)
            db.commit()
            return {"message": "Token 已更新", "id": existing["id"],
                    "user_id": user_id, "country_code": country_code,
                    "oauth_client_id": oauth_client_id,
                    "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S")}

    # 新建账号
    insert_fields = (
        "email, user_id, country_code, access_token, refresh_token, "
        "token_expires_at, client_id, subscription_type, highest_quality, status"
    )
    insert_values = "%s, %s, %s, %s, %s, %s, %s, 'PREMIUM', 'LOSSLESS', 'active'"
    params = [data.email or "", user_id, country_code, token, data.refresh_token,
              expires_at.strftime("%Y-%m-%d %H:%M:%S"), client_id]
    if oauth_client_id:
        insert_fields += ", oauth_client_id"
        insert_values += ", %s"
        params.append(oauth_client_id)

    cursor.execute(
        f"INSERT INTO tidal_accounts ({insert_fields}) VALUES ({insert_values})",
        params
    )
    db.commit()
    return {"message": "账号添加成功", "id": cursor.lastrowid,
            "user_id": user_id, "country_code": country_code,
            "oauth_client_id": oauth_client_id,
            "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S")}


@router.get("/{account_id}")
def get_account(account_id: int, db=Depends(get_db_dependency)):
    """获取单个账号详情"""
    cursor = db.cursor()
    cursor.execute("SELECT * FROM tidal_accounts WHERE id = %s", (account_id,))
    account = cursor.fetchone()
    if not account:
        raise HTTPException(status_code=404, detail="账号不存在")
    return account


@router.delete("/{account_id}")
def delete_account(account_id: int, db=Depends(get_db_dependency)):
    """删除账号"""
    cursor = db.cursor()
    cursor.execute("DELETE FROM tidal_accounts WHERE id = %s", (account_id,))
    db.commit()
    return {"message": "账号已删除"}


@router.post("/{account_id}/report")
def report_account_issue(account_id: int, data: dict, db=Depends(get_db_dependency)):
    """Worker 上报账号异常"""
    status = data.get("status", "error")
    error_message = data.get("error_message", "")
    cursor = db.cursor()

    if status == "rate_limited":
        # 429: 记录次数，状态保持 active（冷却由 Worker 本地管理）
        cursor.execute(
            "UPDATE tidal_accounts SET rate_limit_count = rate_limit_count + 1, "
            "error_message = %s, updated_at = NOW() WHERE id = %s",
            (error_message, account_id)
        )
    elif status == "token_expired":
        # Token 过期
        cursor.execute(
            "UPDATE tidal_accounts SET status = 'token_expired', "
            "error_message = %s, updated_at = NOW() WHERE id = %s",
            (error_message, account_id)
        )
    elif status == "suspended":
        # 封禁
        cursor.execute(
            "UPDATE tidal_accounts SET status = 'suspended', "
            "error_message = %s, updated_at = NOW() WHERE id = %s",
            (error_message, account_id)
        )
    else:
        cursor.execute(
            "UPDATE tidal_accounts SET status = %s, error_message = %s, "
            "updated_at = NOW() WHERE id = %s",
            (status, error_message, account_id)
        )

    db.commit()
    return {"message": "账号状态已更新"}


@router.post("/{account_id}/refresh")
def refresh_account_token(account_id: int, data: dict, db=Depends(get_db_dependency)):
    """手动更新账号 Token"""
    cursor = db.cursor()
    cursor.execute(
        "UPDATE tidal_accounts SET access_token = %s, refresh_token = %s, "
        "token_expires_at = %s, status = 'active', error_message = '', updated_at = NOW() "
        "WHERE id = %s",
        (data.get("access_token", ""), data.get("refresh_token", ""),
         data.get("token_expires_at"), account_id)
    )
    db.commit()
    return {"message": "Token 已更新"}
