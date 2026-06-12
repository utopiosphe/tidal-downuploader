"""
TIDAL DASH 下载核心逻辑
复用已验证的 DASH manifest 解析和分段下载
"""
import json
import base64
import xml.etree.ElementTree as ET
import ssl
import requests
import logging
import subprocess
import os
import tempfile

ssl._create_default_https_context = ssl._create_unverified_context
logger = logging.getLogger("worker.downloader")

TIDAL_API_BASE = "https://api.tidal.com/v1"


def build_session(proxy_config: dict) -> requests.Session:
    """构建带代理的 requests session"""
    session = requests.Session()
    session.verify = False

    # 增大连接池以匹配高并发
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    if proxy_config and proxy_config.get("host"):
        protocol = proxy_config.get("protocol", "socks5")
        host = proxy_config["host"]
        username = proxy_config.get("username", "")
        password = proxy_config.get("password", "")

        if protocol == "socks5":
            port = proxy_config.get("socks5_port", 41003)
            proxy_url = f"socks5h://{username}:{password}@{host}:{port}"
        else:
            port = proxy_config.get("http_port", 41002)
            proxy_url = f"http://{username}:{password}@{host}:{port}"

        session.proxies = {"http": proxy_url, "https": proxy_url}

    return session


def get_playback_info(session: requests.Session, track_id: int,
                      access_token: str, quality: str = "LOSSLESS",
                      country_code: str = "NG") -> dict:
    """获取曲目播放信息"""
    url = (f"{TIDAL_API_BASE}/tracks/{track_id}/playbackinfopostpaywall"
           f"?audioquality={quality}&playbackmode=STREAM"
           f"&assetpresentation=FULL&countryCode={country_code}")

    resp = session.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)

    if resp.status_code == 401:
        try:
            err_body = resp.json()
            sub_status = err_body.get("subStatus", 0)
        except Exception:
            sub_status = 0
        # subStatus 11003/6001 = token 真过期
        # subStatus 4005 = Asset not ready (非 token 问题)
        if sub_status == 4005:
            raise TrackNotFoundError(f"Track {track_id} not available for playback (4005)")
        raise TokenExpiredError(f"Token expired for track {track_id} (sub={sub_status})")
    if resp.status_code == 403:
        raise AccountBannedError(f"Account banned or forbidden for track {track_id}")
    if resp.status_code == 404:
        raise TrackNotFoundError(f"Track {track_id} not found")
    if resp.status_code == 429:
        raise RateLimitError(f"Rate limited for track {track_id}")

    resp.raise_for_status()
    return resp.json()


def download_track(session: requests.Session, track_id: int,
                   access_token: str, quality: str = "LOSSLESS",
                   country_code: str = "NG") -> tuple:
    """
    下载单个曲目
    返回: (file_path, codec, actual_quality) - 临时文件路径
    """
    pb = get_playback_info(session, track_id, access_token, quality, country_code)

    actual_quality = pb.get("audioQuality", "UNKNOWN")
    manifest_b64 = pb.get("manifest", "")
    manifest_mime = pb.get("manifestMimeType", "")

    if "dash" in manifest_mime.lower():
        raw_data, codec = _download_dash(session, manifest_b64)
    elif "vnd.tidal.bts" in manifest_mime.lower():
        raw_data, codec = _download_bts(session, manifest_b64)
    else:
        raise DownloadError(f"Unsupported manifest type: {manifest_mime}")

    if raw_data is None:
        raise DownloadError("No data received")

    # 保存到临时文件
    if "flac" in (codec or "").lower():
        raw_ext, final_ext = "mp4", "flac"
    elif "mp4a" in (codec or "").lower():
        raw_ext, final_ext = "m4a", "m4a"
    else:
        raw_ext, final_ext = "mp4", "mp4"

    # 写入临时文件
    tmp_raw = tempfile.NamedTemporaryFile(suffix=f".{raw_ext}", delete=False)
    tmp_raw.write(raw_data)
    tmp_raw.close()

    # ffmpeg 转封装
    if raw_ext != final_ext:
        tmp_final = tmp_raw.name.replace(f".{raw_ext}", f".{final_ext}")
        if _ffmpeg_convert(tmp_raw.name, tmp_final):
            os.unlink(tmp_raw.name)
            return tmp_final, codec, actual_quality
        else:
            return tmp_raw.name, codec, actual_quality
    else:
        return tmp_raw.name, codec, actual_quality


def _download_dash(session: requests.Session, manifest_b64: str) -> tuple:
    """解析 DASH manifest 并下载"""
    xml_str = base64.b64decode(manifest_b64).decode("utf-8")
    root = ET.fromstring(xml_str)
    ns = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}

    for adaptation in root.findall(".//mpd:AdaptationSet", ns):
        for rep in adaptation.findall("mpd:Representation", ns):
            codec = rep.get("codecs", "unknown")
            seg = rep.find("mpd:SegmentTemplate", ns)
            if seg is None:
                continue

            init_url = seg.get("initialization")
            media_tmpl = seg.get("media")
            timeline = seg.find("mpd:SegmentTimeline", ns)

            # 下载 init segment
            all_data = bytearray(session.get(init_url, timeout=60).content)

            if timeline is not None:
                seg_num = 1
                for s in timeline.findall("mpd:S", ns):
                    r_val = s.get("r")
                    repeat = int(r_val) + 1 if r_val is not None else 1
                    for _ in range(repeat):
                        seg_url = media_tmpl.replace("$Number$", str(seg_num))
                        seg_data = session.get(seg_url, timeout=120).content
                        all_data.extend(seg_data)
                        seg_num += 1

            return bytes(all_data), codec

    return None, None


def _download_bts(session: requests.Session, manifest_b64: str) -> tuple:
    """下载 BTS 格式"""
    mf = json.loads(base64.b64decode(manifest_b64).decode("utf-8"))
    urls = mf.get("urls", [])
    codec = mf.get("codecs", "unknown")
    if urls:
        data = session.get(urls[0], timeout=120).content
        return data, codec
    return None, None


def _ffmpeg_convert(input_path: str, output_path: str) -> bool:
    """ffmpeg 转封装"""
    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", input_path, "-c", "copy", output_path],
            capture_output=True, timeout=60
        )
        return result.returncode == 0
    except Exception:
        return False


# 自定义异常
class TokenExpiredError(Exception):
    pass

class AccountBannedError(Exception):
    pass

class TrackNotFoundError(Exception):
    pass

class RateLimitError(Exception):
    pass

class DownloadError(Exception):
    pass
