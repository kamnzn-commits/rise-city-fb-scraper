"""
Rise City Facebook Scraper API - V8.11 🎩 ANTI-MULTI-POSTS + LIVE PROXY GUARD
BASE: V8.10
PATCH MỚI V8.11 (FIX 2 BUGS NGHIÊM TRỌNG):

  BUG 1: Mobile mode parse cộng dồn engagement của nhiều posts
  Case: Video Ngọc Nguyễn live, FB hiển thị feed có 3 posts
    - Post target: 21 likes, 11 cmt, 0 shares
    - Post 2: 8 likes, 4 cmt, 0 shares
    - Post 3: 29 likes, 12 cmt, 1 shares
    Tổng cộng nhầm: 94 likes (sai 4.5 lần)
  
  FIX V8.11:
    - Count icon LIKE trong target_text (sau isolation)
    - Nếu > 2 icon LIKE → có nhiều posts → SKIP mobile engagement
    - Trả 0 (an toàn) thay vì cộng dồn sai

  BUG 2: Proxy fallback lấy peak concurrent viewers cho live replay
  Case: Video Ngọc Nguyễn live actual=3,500 → proxy trả 61,704 (peak live)
  
  FIX V8.11:
    - Detect live replay markers TRƯỚC khi gọi proxy
    - Markers: was_live_broadcast, broadcast_status VOD_READY,
      is_live_streaming, live_video_id, format_detected=video_live, /share/v/
    - Nếu là live replay → SKIP proxy (avoid wrong views)
    - Skip lý do: 'v811_live_replay_skip_proxy'

LOGIC HOÀN CHỈNH V8.11:
1. Desktop mode → views + V8.10 engagement
2. Mobile mode → backup engagement (V8.11 guard chống multi-posts)
3. /videos/ URL fallback → views + smart match
4. Reel Grid → views fallback
5. VN Proxy → CHỈ cho non-live videos (V8.11)

ENV VARS REQUIRED ON RENDER:
- API_SECRET, FB_COOKIES_PATH
- PROXY_HOST, PROXY_PORT, PROXY_USERNAME_BASE, PROXY_PASSWORD
"""
from flask import Flask, request, jsonify, make_response
from playwright.sync_api import sync_playwright
import os
import re
import json
import logging
import time
import random
import gc
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

COOKIES_PATH = os.getenv('FB_COOKIES_PATH', '/etc/secrets/cookies.txt')
API_SECRET = os.getenv('API_SECRET', 'rise-city-secret-2026')

# === V8.2: VN RESIDENTIAL PROXY CONFIG (Webshare) ===
PROXY_HOST = os.getenv('PROXY_HOST', '')
PROXY_PORT = os.getenv('PROXY_PORT', '80')
PROXY_USERNAME_BASE = os.getenv('PROXY_USERNAME_BASE', '')  # ufxgmuzq
PROXY_PASSWORD = os.getenv('PROXY_PASSWORD', '')
PROXY_ENABLED = bool(PROXY_HOST and PROXY_USERNAME_BASE and PROXY_PASSWORD)

# === V8.9: CONCURRENCY LIMIT ENHANCED ===
# V8.4 ban đầu Semaphore(1) - quá strict, gây 503 nhiều
# V8.9 tăng lên Semaphore(2) cho phép 2 request song song
# Render Starter plan có 2GB RAM đủ cho 2 Playwright cùng lúc
SCRAPE_SEMAPHORE = threading.Semaphore(2)
SCRAPE_LOCK_TIMEOUT = 10  # Tăng từ 5s lên 10s đợi semaphore
# === END V8.9 ===


def get_random_vn_proxy():
    """Random rotate qua 10 IP VN: vn-1 → vn-10"""
    if not PROXY_ENABLED:
        return None
    proxy_num = random.randint(1, 10)
    username = f"{PROXY_USERNAME_BASE}-vn-{proxy_num}"
    return {
        'server': f'http://{PROXY_HOST}:{PROXY_PORT}',
        'username': username,
        'password': PROXY_PASSWORD,
        'proxy_id': f'vn-{proxy_num}',  # for debug logging
    }
# === END V8.2 PROXY CONFIG ===

USERNAME_BLACKLIST = {
    'recover', 'help', 'settings', 'privacy', 'home', 'login', 'logout',
    'signup', 'register', 'reg', 'reset', 'support', 'business',
    'marketplace', 'gaming', 'watch', 'reel', 'reels', 'share', 'video',
    'photo', 'permalink', 'profile.php', 'people', 'pages', 'groups',
    'events', 'memories', 'saved', 'notifications', 'messages',
    'friends', 'public', 'media', 'hashtag', 'search', 'browse',
    'lite', 'mobile', 'm', 'www', 'web', 'about', 'policies',
    'terms', 'community', 'safety', 'legal', 'careers', 'ads',
    'developers', 'directory', 'badges', 'feed', 'timeline'
}


@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-API-Key, x-api-key, Authorization'
    response.headers['Access-Control-Max-Age'] = '3600'
    return response


@app.route('/scrape', methods=['OPTIONS'])
@app.route('/health', methods=['OPTIONS'])
@app.route('/', methods=['OPTIONS'])
def handle_options():
    return make_response('', 204)


def decode_unicode_string(s):
    if not s:
        return s
    try:
        return s.encode('utf-8').decode('unicode_escape').encode('latin-1').decode('utf-8')
    except:
        try:
            return bytes(s, 'utf-8').decode('unicode_escape')
        except:
            return s


def decode_html_entities(text):
    if not text:
        return text
    return (text
        .replace('&amp;', '&')
        .replace('&lt;', '<')
        .replace('&gt;', '>')
        .replace('&quot;', '"')
        .replace('&#39;', "'")
        .replace('&apos;', "'"))


def parse_netscape_cookies(cookies_path):
    cookies = []
    if not os.path.exists(cookies_path):
        return cookies
    
    with open(cookies_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split('\t')
            if len(parts) < 7:
                continue
            domain, flag, path, secure, expiration, name, value = parts[:7]
            cookies.append({
                'name': name, 'value': value, 'domain': domain,
                'path': path, 'secure': secure.upper() == 'TRUE',
                'httpOnly': False, 'sameSite': 'Lax',
            })
    return cookies


def parse_vietnamese_number(text):
    if not text:
        return 0
    s = str(text).strip()
    match = re.match(r'([\d.,]+)\s*([KkMmBbTrtr]+)?', s)
    if not match:
        return 0
    num_str = match.group(1)
    suffix = match.group(2)
    
    if ',' in num_str and '.' in num_str:
        num_str = num_str.replace(',', '')
    elif ',' in num_str:
        parts = num_str.split(',')
        if len(parts) == 2 and len(parts[1]) == 3:
            num_str = num_str.replace(',', '')
        else:
            num_str = num_str.replace(',', '.')
    elif '.' in num_str:
        parts = num_str.split('.')
        if len(parts) == 2 and len(parts[1]) == 3:
            num_str = num_str.replace('.', '')
    
    try:
        num = float(num_str)
    except:
        return 0
    
    if suffix:
        suffix = suffix.lower()
        if 'k' in suffix:
            num *= 1000
        elif 'm' in suffix:
            num *= 1000000
        elif 'b' in suffix:
            num *= 1000000000
        elif 'tr' in suffix:
            num *= 1000000
    return int(num)


# ==========================================
# PATCHED: parse_mobile_engagement
# V8.1 original dùng regex thứ tự → SAI khi FB ẩn likes/shares
# V8.1.2 dùng icon codepoint mapping + dual icon set
# ==========================================

def parse_mobile_engagement(innertext, debug_info):
    """
    PATCHED V8.1.2: Parse engagement bằng ICON CODEPOINT, hỗ trợ cả Reel và Video/Live.
    
    FB dùng 2 bộ icon khác nhau:
      REEL:       U+F0378=like  U+F0379=comment  U+F037A=share
      VIDEO/LIVE: U+F0925=like  U+F0926=comment  U+F0927=share
    
    Cũng parse "332 lượt xem" cho video live.
    """
    data = {'likes': 0, 'comments': 0, 'shares': 0, 'mobile_views': 0}
    
    if not innertext:
        return data
    
    # Step 1: Isolate target video (cắt trước related content)
    RELATED_MARKERS = [
        'Watch more reels like this',
        'Còn nhiều nội dung khác',
        'Explore these popular topics',
        'Hãy đăng nhập để khám phá',
        'Tiếp tục dưới tên',
        'Đăng nhập để kết nối',
        'See more reels',
        'Video khác bạn có thể thích',
        'Xem thêm video bạn có',
    ]
    cut_pos = len(innertext)
    matched_marker = None
    for marker in RELATED_MARKERS:
        pos = innertext.find(marker)
        if pos > 0 and pos < cut_pos:
            cut_pos = pos
            matched_marker = marker
    
    target_text = innertext[:cut_pos]
    lines = target_text.split('\n')
    
    debug_info['isolation_marker'] = matched_marker
    debug_info['isolation_length'] = len(target_text)
    
    # === V8.11: GUARD AGAINST MULTI-POSTS FEED ===
    # Bug case (Ngọc Nguyễn live): isolation không cắt được → target_text có
    # nhiều posts → cộng dồn engagement → SAI hoàn toàn.
    # Nếu target_text > 1500 chars VÀ chứa nhiều icon LIKE/CMT → có nhiều posts.
    
    # Count icon sets trong target_text
    REEL_LIKE_CHECK  = '\U000F0378'
    VID_LIKE_CHECK   = '\U000F0925'
    likes_icon_count = target_text.count(REEL_LIKE_CHECK) + target_text.count(VID_LIKE_CHECK)
    
    debug_info['v811_target_length'] = len(target_text)
    debug_info['v811_likes_icon_count'] = likes_icon_count
    
    # Nếu target có > 2 icon LIKE → likely là feed với nhiều posts → skip
    if likes_icon_count > 2:
        debug_info['v811_skip_reason'] = 'multi_posts_detected'
        debug_info['v83_engagement'] = {'likes': 0, 'comments': 0, 'shares': 0}
        logger.warning(f'⚠️ V8.11: Detected {likes_icon_count} posts in feed, skipping mobile engagement')
        return data  # Return zero engagement - safer than wrong values
    
    # Nếu target_text quá dài VÀ không có Reel marker rõ ràng → skip
    if len(target_text) > 1800 and likes_icon_count > 1:
        debug_info['v811_skip_reason'] = 'target_too_long'
        debug_info['v83_engagement'] = {'likes': 0, 'comments': 0, 'shares': 0}
        logger.warning(f'⚠️ V8.11: Target text {len(target_text)} chars too long, skipping')
        return data
    # === END V8.11 GUARD ===
    
    # Step 2: Detect icon set
    REEL_LIKE  = '\U000F0378'
    REEL_CMT   = '\U000F0379'
    REEL_SHARE = '\U000F037A'
    VID_LIKE   = '\U000F0925'
    VID_CMT    = '\U000F0926'
    VID_SHARE  = '\U000F0927'
    
    has_reel_icons = REEL_LIKE in target_text or REEL_CMT in target_text
    has_vid_icons = VID_LIKE in target_text or VID_CMT in target_text
    
    debug_info['format_detected'] = 'video_live' if has_vid_icons else 'reel'
    
    # Step 3a: VIDEO/LIVE format (icon + space + number trên cùng 1 dòng)
    if has_vid_icons:
        for line in lines:
            ls = line.strip()
            if VID_LIKE in ls:
                m = re.search(r'[\d.,]+\s*[KkMmBb]?', ls.split(VID_LIKE)[-1])
                if m:
                    data['likes'] = parse_vietnamese_number(m.group(0))
                    debug_info['v83_like_raw'] = m.group(0)
            elif VID_CMT in ls:
                m = re.search(r'[\d.,]+\s*[KkMmBb]?', ls.split(VID_CMT)[-1])
                if m:
                    data['comments'] = parse_vietnamese_number(m.group(0))
                    debug_info['v83_cmt_raw'] = m.group(0)
            elif VID_SHARE in ls:
                m = re.search(r'[\d.,]+\s*[KkMmBb]?', ls.split(VID_SHARE)[-1])
                if m:
                    data['shares'] = parse_vietnamese_number(m.group(0))
                    debug_info['v83_share_raw'] = m.group(0)
        
        # Parse "332 lượt xem"
        m = re.search(r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*xem', target_text)
        if m:
            data['mobile_views'] = parse_vietnamese_number(m.group(1))
            debug_info['mobile_views_raw'] = m.group(1)
    
    # Step 3b: REEL format (icon trên 1 dòng, số trên dòng tiếp theo)
    if has_reel_icons:
        def find_number_after(start_idx, max_look=2):
            for i in range(start_idx + 1, min(start_idx + 1 + max_look, len(lines))):
                ln = lines[i].strip()
                if not ln:
                    continue
                if re.match(r'^[\d.,]+\s*[KkMmBb]?$', ln) and len(ln) < 15:
                    return ln
                if any(ord(c) > 0xF0000 for c in ln):
                    return None
                if len(ln) > 3 and not ln[0].isdigit():
                    return None
            return None
        
        found_like = found_cmt = found_share = False
        
        for i, line in enumerate(lines):
            for c in line:
                cp = ord(c)
                if cp == ord(REEL_LIKE) and not found_like:
                    found_like = True
                    val = find_number_after(i)
                    reel_likes = parse_vietnamese_number(val) if val else 0
                    data['likes'] = max(data['likes'], reel_likes)
                    debug_info['v83_like_raw'] = val
                elif cp == ord(REEL_CMT) and not found_cmt:
                    found_cmt = True
                    val = find_number_after(i)
                    reel_cmts = parse_vietnamese_number(val) if val else 0
                    data['comments'] = max(data['comments'], reel_cmts)
                    debug_info['v83_cmt_raw'] = val
                elif cp == ord(REEL_SHARE) and not found_share:
                    found_share = True
                    val = find_number_after(i)
                    reel_shares = parse_vietnamese_number(val) if val else 0
                    data['shares'] = max(data['shares'], reel_shares)
                    debug_info['v83_share_raw'] = val
    
    debug_info['v83_engagement'] = {'likes': data['likes'], 'comments': data['comments'], 'shares': data['shares']}
    return data


def search_views_in_text(text):
    """Search Vietnamese view patterns + JSON patterns in any text"""
    candidates = []
    
    patterns = [
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\\u01b0\\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u1ea7n\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*views?\b',
    ]
    
    for pat in patterns:
        matches = re.findall(pat, text, re.IGNORECASE)
        for m in matches[:30]:
            value = parse_vietnamese_number(m)
            if 10 <= value <= 1000000000:
                candidates.append(value)
    
    json_patterns = [
        r'"video_view_count"\s*:\s*(\d+)',
        r'"play_count"\s*:\s*(\d+)',
        r'"viewCount"\s*:\s*(\d+)',
        r'"reels_view_count"\s*:\s*(\d+)',
        r'"organic_view_count"\s*:\s*(\d+)',
    ]
    
    for pat in json_patterns:
        matches = re.findall(pat, text)
        for m in matches:
            try:
                val = int(m)
                if 10 <= val <= 1000000000:
                    candidates.append(val)
            except:
                pass
    
    return max(candidates) if candidates else 0


# ==========================================
# V8.10: EXTRACT ENGAGEMENT FROM HTML JSON
# Mobile mode bị login wall → engagement=0
# Desktop mode có HTML đầy đủ với JSON engagement → cần extract
# ==========================================

def extract_engagement_from_html(html, post_id=None, debug_info=None):
    """
    V8.10: Extract likes/comments/shares từ JSON pattern trong HTML.
    
    FB lưu engagement trong nhiều patterns:
    - "reaction_count":{"count":XXX}
    - "comment_count":{"total_count":XXX}  
    - "share_count":{"count":XXX}
    - "i18n_reaction_count":"XXX"
    - "feedback":{"reaction_count":...}
    
    Strategy:
    1. Tìm post_id position trong HTML
    2. Tìm engagement patterns trong window quanh post_id (5000 chars)
    3. Nếu không tìm được trong window → fallback search toàn HTML
    
    Returns: {'likes': int, 'comments': int, 'shares': int}
    """
    result = {'likes': 0, 'comments': 0, 'shares': 0}
    
    if not html:
        return result
    
    if debug_info is None:
        debug_info = {}
    
    # Find post_id position
    post_id_pos = -1
    if post_id:
        post_id_pos = html.find(f'"{post_id}"')
        debug_info['v810_post_id_pos'] = post_id_pos
    
    # Search window
    if post_id_pos > 0:
        window_start = max(0, post_id_pos - 1000)
        window_end = min(len(html), post_id_pos + 10000)
        search_html = html[window_start:window_end]
        debug_info['v810_search_strategy'] = 'window'
    else:
        search_html = html
        debug_info['v810_search_strategy'] = 'full_html'
    
    # === LIKES patterns ===
    likes_patterns = [
        # Reaction count với object wrapper
        (r'"reaction_count"\s*:\s*\{[^}]*?"count"\s*:\s*(\d+)', 'reaction_count_obj'),
        # I18n reaction count (string format)
        (r'"i18n_reaction_count"\s*:\s*"([\d,\.KkMmBb]+)"', 'i18n_reaction_count'),
        # Top reactions
        (r'"top_reactions"\s*:\s*\{[^}]*?"count"\s*:\s*(\d+)', 'top_reactions'),
        # Reactor count
        (r'"reactor_count"\s*:\s*(\d+)', 'reactor_count'),
        # Like count direct
        (r'"like_count"\s*:\s*(\d+)', 'like_count_direct'),
    ]
    
    likes_candidates = []
    for pattern, name in likes_patterns:
        for m in re.finditer(pattern, search_html):
            val_str = m.group(1)
            val = parse_vietnamese_number(val_str) if isinstance(val_str, str) else int(val_str)
            if val > 0:
                likes_candidates.append((val, name))
    
    if likes_candidates:
        # Lấy giá trị HỢP LÝ NHẤT (không phải max, vì max có thể là page total)
        # Ưu tiên: i18n_reaction_count > reaction_count_obj > top_reactions
        priority_order = ['i18n_reaction_count', 'reaction_count_obj', 'top_reactions', 'reactor_count', 'like_count_direct']
        for preferred in priority_order:
            for val, name in likes_candidates:
                if name == preferred and val < 1000000:  # Sanity check: < 1M
                    result['likes'] = val
                    debug_info['v810_likes_source'] = name
                    debug_info['v810_likes_value'] = val
                    break
            if result['likes'] > 0:
                break
        
        # Fallback: smallest non-zero value (often correct)
        if result['likes'] == 0:
            valid = [v for v, _ in likes_candidates if 0 < v < 1000000]
            if valid:
                result['likes'] = min(valid)
                debug_info['v810_likes_source'] = 'min_valid'
    
    # === COMMENTS patterns ===
    comments_patterns = [
        # Comment count với total
        (r'"comment_count"\s*:\s*\{[^}]*?"total_count"\s*:\s*(\d+)', 'comment_count_total'),
        # Comment count direct
        (r'"comment_count"\s*:\s*(\d+)', 'comment_count_direct'),
        # Total comments
        (r'"total_comment_count"\s*:\s*(\d+)', 'total_comment_count'),
        # i18n comment count
        (r'"i18n_comment_count"\s*:\s*"([\d,\.KkMmBb]+)"', 'i18n_comment_count'),
    ]
    
    comments_candidates = []
    for pattern, name in comments_patterns:
        for m in re.finditer(pattern, search_html):
            val_str = m.group(1)
            val = parse_vietnamese_number(val_str) if not val_str.isdigit() else int(val_str)
            if val > 0 and val < 1000000:  # Sanity check
                comments_candidates.append((val, name))
    
    if comments_candidates:
        priority_order = ['comment_count_total', 'i18n_comment_count', 'comment_count_direct', 'total_comment_count']
        for preferred in priority_order:
            for val, name in comments_candidates:
                if name == preferred:
                    result['comments'] = val
                    debug_info['v810_comments_source'] = name
                    debug_info['v810_comments_value'] = val
                    break
            if result['comments'] > 0:
                break
    
    # === SHARES patterns ===
    shares_patterns = [
        # Share count với object wrapper
        (r'"share_count"\s*:\s*\{[^}]*?"count"\s*:\s*(\d+)', 'share_count_obj'),
        # Share count direct
        (r'"share_count"\s*:\s*(\d+)', 'share_count_direct'),
        # i18n share count
        (r'"i18n_share_count"\s*:\s*"([\d,\.KkMmBb]+)"', 'i18n_share_count'),
        # Reshare count
        (r'"reshare_count"\s*:\s*(\d+)', 'reshare_count'),
    ]
    
    shares_candidates = []
    for pattern, name in shares_patterns:
        for m in re.finditer(pattern, search_html):
            val_str = m.group(1)
            val = parse_vietnamese_number(val_str) if not val_str.isdigit() else int(val_str)
            if 0 <= val < 1000000:  # Sanity check
                shares_candidates.append((val, name))
    
    if shares_candidates:
        priority_order = ['share_count_obj', 'i18n_share_count', 'share_count_direct', 'reshare_count']
        for preferred in priority_order:
            for val, name in shares_candidates:
                if name == preferred:
                    result['shares'] = val
                    debug_info['v810_shares_source'] = name
                    debug_info['v810_shares_value'] = val
                    break
            if result['shares'] > 0 or any(name == priority_order[0] for _, name in shares_candidates):
                break
    
    debug_info['v810_likes_candidates'] = likes_candidates[:10]
    debug_info['v810_comments_candidates'] = comments_candidates[:10]
    debug_info['v810_shares_candidates'] = shares_candidates[:10]
    
    return result


def simulate_human(page):
    try:
        for _ in range(2):
            x = random.randint(100, 1200)
            y = random.randint(100, 600)
            page.mouse.move(x, y)
            time.sleep(random.uniform(0.3, 0.5))
        for offset in [200, 500]:
            page.evaluate(f'window.scrollTo({{top: {offset}, behavior: "smooth"}})')
            time.sleep(random.uniform(0.6, 1.0))
    except:
        pass


# ==========================================
# FINGERPRINTS - 3 different realistic profiles
# ==========================================

FINGERPRINT_IPHONE_15 = {
    'user_agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1',
    'viewport': {'width': 393, 'height': 852},
    'device_scale_factor': 3,
    'is_mobile': True,
    'has_touch': True,
    'extra_headers': {
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.google.com/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }
}

FINGERPRINT_ANDROID_S24 = {
    'user_agent': 'Mozilla/5.0 (Linux; Android 14; SM-S921B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36',
    'viewport': {'width': 384, 'height': 834},
    'device_scale_factor': 2.75,
    'is_mobile': True,
    'has_touch': True,
    'extra_headers': {
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Referer': 'https://www.google.com/search?q=facebook+reel',
        'sec-ch-ua': '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Upgrade-Insecure-Requests': '1',
    }
}

FINGERPRINT_DESKTOP_CHROME_VN = {
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'viewport': {'width': 1366, 'height': 768},
    'device_scale_factor': 1,
    'is_mobile': False,
    'has_touch': False,
    'extra_headers': {
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Referer': 'https://www.google.com.vn/',
        'sec-ch-ua': '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }
}


def try_anonymous_with_fingerprint(browser, url, fingerprint, name, debug_info):
    """
    Try to scrape views WITHOUT cookies but with realistic fingerprint.
    The key insight: Different fingerprints might bypass FB's anti-bot.
    """
    try:
        debug_info[f'{name}_attempted'] = True
        
        context = browser.new_context(
            user_agent=fingerprint['user_agent'],
            viewport=fingerprint['viewport'],
            device_scale_factor=fingerprint['device_scale_factor'],
            is_mobile=fingerprint['is_mobile'],
            has_touch=fingerprint['has_touch'],
            locale='vi-VN',
            timezone_id='Asia/Ho_Chi_Minh',
            extra_http_headers=fingerprint['extra_headers'],
        )
        
        # NO cookies - anonymous
        page = context.new_page()
        
        # Anti-detection scripts
        page.add_init_script("""
            // Override webdriver detection
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en'] });
            
            // Fake chrome runtime
            window.chrome = { runtime: {} };
            
            // Hide playwright trace
            const originalQuery = window.navigator.permissions?.query;
            if (originalQuery) {
                window.navigator.permissions.query = (params) => (
                    params.name === 'notifications'
                        ? Promise.resolve({ state: Notification.permission })
                        : originalQuery(params)
                );
            }
            
            // Pretend to have screen
            Object.defineProperty(screen, 'availWidth', { get: () => 1366 });
            Object.defineProperty(screen, 'availHeight', { get: () => 768 });
        """)
        
        # Capture all responses
        captured_responses = []
        def handle_response(response):
            try:
                if response.status == 200:
                    url_lower = response.url.lower()
                    if any(k in url_lower for k in ['graphql', 'reel', 'video', 'fb_dtsg', 'jsmods']):
                        try:
                            body = response.text()
                            if body and len(body) < 3000000:
                                captured_responses.append(body)
                        except:
                            pass
            except:
                pass
        page.on('response', handle_response)
        
        logger.info(f'{name} navigating to: {url}')
        response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
        
        # Random wait like real user (3-7s)
        time.sleep(random.uniform(3, 6))
        
        # Try to dismiss login overlay if appears (without dismissing video)
        try:
            page.evaluate("""
                () => {
                    const dialogs = document.querySelectorAll('[role="dialog"]');
                    dialogs.forEach(d => {
                        const text = d.textContent || '';
                        if (text.includes('\u0110\u0103ng nh\u1eadp') || text.includes('Log in')) {
                            d.style.display = 'none';
                        }
                    });
                    const overlays = document.querySelectorAll('[data-testid*="login"], [aria-label*="\u0110\u0103ng nh\u1eadp"]');
                    overlays.forEach(o => o.style.display = 'none');
                }
            """)
            time.sleep(1)
        except:
            pass
        
        # Scroll to trigger view counter render
        try:
            page.evaluate('window.scrollTo({top: 300, behavior: "smooth"})')
            time.sleep(2)
            page.evaluate('window.scrollTo({top: 600, behavior: "smooth"})')
            time.sleep(2)
        except:
            pass
        
        # Get all data
        html = page.content()
        innertext = ''
        try:
            innertext = page.evaluate('document.body.innerText || ""')
        except:
            pass
        
        debug_info[f'{name}_html_length'] = len(html)
        debug_info[f'{name}_innertext_length'] = len(innertext)
        debug_info[f'{name}_innertext_preview'] = innertext[:300]
        debug_info[f'{name}_network_count'] = len(captured_responses)
        debug_info[f'{name}_url_after_redirect'] = page.url
        
        # Check if redirected to login
        if 'login' in page.url.lower() or '\u0110\u0103ng nh\u1eadp v\u00e0o Facebook' in innertext[:200]:
            debug_info[f'{name}_blocked'] = True
            context.close()
            return 0
        
        # Search views in all sources
        all_text = '\n'.join([html, innertext] + captured_responses)
        views = search_views_in_text(all_text)
        
        debug_info[f'{name}_views_found'] = views
        debug_info[f'{name}_html_keywords'] = {
            'has_luot_xem': 'l\u01b0\u1ee3t xem' in html,
            'has_video_view_count': 'video_view_count' in html,
            'has_play_count': 'play_count' in html,
        }
        
        context.close()
        return views
    except Exception as e:
        logger.warning(f'{name} mode failed: {e}')
        debug_info[f'{name}_error'] = str(e)[:200]
        return 0


def try_mbasic_for_views(browser, url, cookies, debug_info):
    """
    NEW MODE: mbasic.facebook.com - text-only mobile site.
    Sometimes shows view counter that desktop/mobile hides.
    """
    try:
        debug_info['mbasic_attempted'] = True
        
        # Convert URL to mbasic
        mbasic_url = url.replace('www.facebook.com', 'mbasic.facebook.com')
        if 'mbasic.facebook.com' not in mbasic_url:
            mbasic_url = mbasic_url.replace('facebook.com', 'mbasic.facebook.com')
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Linux; Android 7.0; SM-G930V) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36',
            viewport={'width': 360, 'height': 640},
            locale='vi-VN',
            timezone_id='Asia/Ho_Chi_Minh',
        )
        
        context.add_cookies(cookies)
        page = context.new_page()
        
        logger.info(f'mbasic navigating to: {mbasic_url}')
        response = page.goto(mbasic_url, wait_until='domcontentloaded', timeout=30000)
        time.sleep(4)
        
        html = page.content()
        innertext = ''
        try:
            innertext = page.evaluate('document.body.innerText || ""')
        except:
            pass
        
        debug_info['mbasic_html_length'] = len(html)
        debug_info['mbasic_innertext_length'] = len(innertext)
        debug_info['mbasic_innertext_preview'] = innertext[:500]
        
        all_text = html + '\n' + innertext
        views = search_views_in_text(all_text)
        debug_info['mbasic_views_found'] = views
        
        context.close()
        return views
    except Exception as e:
        logger.warning(f'mbasic mode failed: {e}')
        debug_info['mbasic_error'] = str(e)[:200]
        return 0


def extract_profile_url_from_html(html, debug=None):
    """
    V8.3.1: Extract profile URL của owner reel từ HTML JSON.
    Improved với 12+ patterns và debug logging.
    
    Args:
        html: Full HTML từ FB
        debug: Optional dict để ghi lại debug info
    
    Returns: (profile_url, username) or (None, None)
    """
    if debug is None:
        debug = {}
    
    debug['html_size'] = len(html)
    debug['patterns_tried'] = []
    debug['matches_found'] = []
    
    # === STRATEGY 1: JSON patterns (most reliable when present) ===
    json_patterns = [
        ('owning_profile_url', r'"owning_profile"\s*:\s*\{[^}]*?"url"\s*:\s*"(https?:\\?/\\?/[^"]+facebook\.com\\?/[^"]+?)"'),
        ('creation_story_actor', r'"creation_story"\s*:\s*\{[^}]*?"actors"\s*:\s*\[\s*\{[^}]*?"url"\s*:\s*"(https?:\\?/\\?/[^"]+facebook\.com\\?/[^"]+?)"'),
        ('video_owner_url', r'"video_owner"\s*:\s*\{[^}]*?"(?:profile_url|url)"\s*:\s*"(https?:\\?/\\?/[^"]+?)"'),
        ('actor_url', r'"actor"\s*:\s*\{[^}]*?"url"\s*:\s*"(https?:\\?/\\?/[^"]+facebook\.com\\?/[^"]+?)"'),
        ('actors_url', r'"actors"\s*:\s*\[\s*\{[^}]*?"url"\s*:\s*"(https?:\\?/\\?/[^"]+facebook\.com\\?/[^"]+?)"'),
        ('owner_url', r'"owner"\s*:\s*\{[^}]*?"url"\s*:\s*"(https?:\\?/\\?/[^"]+facebook\.com\\?/[^"]+?)"'),
        ('profile_url_field', r'"profile_url"\s*:\s*"(https?:\\?/\\?/[^"]+facebook\.com\\?/[^"]+?)"'),
        ('page_url_field', r'"page_url"\s*:\s*"(https?:\\?/\\?/[^"]+facebook\.com\\?/[^"]+?)"'),
        ('vanity_url', r'"vanity"\s*:\s*"([a-zA-Z0-9.]+)"'),
        ('username_field', r'"username"\s*:\s*"([a-zA-Z0-9.]+)"'),
    ]
    
    for name, pattern in json_patterns:
        debug['patterns_tried'].append(name)
        match = re.search(pattern, html)
        if match:
            raw = match.group(1).replace('\\/', '/')
            debug['matches_found'].append({'pattern': name, 'raw': raw[:200]})
            
            # If pattern returns just username (vanity/username field)
            if name in ('vanity_url', 'username_field'):
                if raw and raw.lower() not in USERNAME_BLACKLIST:
                    return f'https://www.facebook.com/{raw}', raw
            else:
                # Extract username from URL
                user_match = re.search(r'facebook\.com/([^/?#]+)', raw)
                if user_match:
                    username = user_match.group(1)
                    if username and username.lower() not in USERNAME_BLACKLIST:
                        return raw, username
    
    # === STRATEGY 2: profile.php?id= ===
    debug['patterns_tried'].append('profile_php')
    pid_match = re.search(r'facebook\.com\\?/profile\.php\?id=(\d+)', html)
    if pid_match:
        pid = pid_match.group(1)
        debug['matches_found'].append({'pattern': 'profile_php', 'raw': pid})
        return f'https://www.facebook.com/profile.php?id={pid}', f'profile.php?id={pid}'
    
    # === STRATEGY 3: og:url meta tag ===
    debug['patterns_tried'].append('og_url')
    og_match = re.search(r'<meta\s+property="og:url"\s+content="([^"]+)"', html)
    if og_match:
        og_url = og_match.group(1)
        debug['matches_found'].append({'pattern': 'og_url', 'raw': og_url[:200]})
        # Extract username from og_url (might be /reel/<id> or /<username>/...)
        user_match = re.search(r'facebook\.com/([^/?#]+)', og_url)
        if user_match:
            username = user_match.group(1)
            if username.lower() not in USERNAME_BLACKLIST:
                return f'https://www.facebook.com/{username}', username
    
    # === STRATEGY 4: canonical URL ===
    debug['patterns_tried'].append('canonical')
    canon_match = re.search(r'<link\s+rel="canonical"\s+href="([^"]+)"', html)
    if canon_match:
        canon_url = canon_match.group(1)
        debug['matches_found'].append({'pattern': 'canonical', 'raw': canon_url[:200]})
    
    # === STRATEGY 5: Username từ thumbnail URL fbcdn pattern ===
    # FB CDN URL có thể chứa hint về owner
    
    # === STRATEGY 6: Find any /<username>/posts/ or /<username>/videos/ ===
    debug['patterns_tried'].append('username_in_path')
    # Tìm 5 candidate đầu tiên
    candidates = re.findall(r'facebook\.com\\?/([a-zA-Z0-9.]+)\\?/(?:posts|videos|reels|photos)\\?/', html[:200000])
    debug['username_candidates'] = list(set(candidates))[:10]
    
    for cand in candidates:
        if cand.lower() not in USERNAME_BLACKLIST and len(cand) > 2:
            return f'https://www.facebook.com/{cand}', cand
    
    return None, None


def extract_profile_url_from_dom(page, debug=None):
    """
    V8.3.2: Extract profile URL via JavaScript trong browser DOM.
    PRIORITY ORDER:
        1. og:url meta tag (most reliable for FB reel pages)
        2. Top scored <a> link with avatar/image
        3. profile.php?id= as last resort
    
    Returns: (profile_url, username) or (None, None)
    """
    if debug is None:
        debug = {}
    
    try:
        result = page.evaluate(f"""
            () => {{
                const blacklist = {list(USERNAME_BLACKLIST)};
                const blacklistSet = new Set(blacklist.map(s => s.toLowerCase()));
                
                // Strategy A: og:url meta tag
                const ogUrl = document.querySelector('meta[property="og:url"]');
                let ogUrlContent = ogUrl ? ogUrl.getAttribute('content') : null;
                
                // Strategy B: All <a> links pointing to facebook.com
                const allLinks = document.querySelectorAll('a[href]');
                const candidates = [];
                
                for (const link of allLinks) {{
                    const href = link.getAttribute('href') || '';
                    
                    // Match /username (no slashes after)
                    let match = href.match(/^(?:https?:\\/\\/(?:www\\.|m\\.|web\\.)?facebook\\.com)?\\/([a-zA-Z0-9.]+)(?:\\/|\\?|$|#)/);
                    if (match) {{
                        const username = match[1];
                        if (!blacklistSet.has(username.toLowerCase()) && username.length > 2) {{
                            const text = (link.textContent || '').trim();
                            const hasImg = link.querySelector('img') !== null;
                            const hasAvatar = link.querySelector('image, svg image, [role="img"]') !== null;
                            
                            candidates.push({{
                                username: username,
                                href: href,
                                text: text.substring(0, 100),
                                has_img: hasImg,
                                has_avatar: hasAvatar,
                                aria_label: link.getAttribute('aria-label') || ''
                            }});
                        }}
                    }}
                }}
                
                // Strategy C: profile.php?id=
                const pidLinks = [];
                for (const link of allLinks) {{
                    const href = link.getAttribute('href') || '';
                    const m = href.match(/profile\\.php\\?id=(\\d+)/);
                    if (m) {{
                        pidLinks.push({{
                            id: m[1],
                            href: href,
                            text: (link.textContent || '').trim().substring(0, 100)
                        }});
                    }}
                }}
                
                return {{
                    og_url: ogUrlContent,
                    candidates: candidates.slice(0, 30),
                    profile_php_ids: pidLinks.slice(0, 10),
                }};
            }}
        """)
        
        debug['dom_og_url'] = result.get('og_url')
        debug['dom_candidates_count'] = len(result.get('candidates', []))
        debug['dom_profile_php_count'] = len(result.get('profile_php_ids', []))
        debug['dom_top_candidates'] = result.get('candidates', [])[:10]
        
        # ===== PRIORITY 1: Parse og:url =====
        # FB embed reel/post URL như:
        #   https://www.facebook.com/the.mobifone/videos/<slug>/<post_id>/
        #   https://www.facebook.com/the.mobifone/posts/<id>
        # Username = path component đầu tiên sau facebook.com/
        og_url = result.get('og_url') or ''
        if og_url:
            # Extract username from og:url path
            og_match = re.search(r'facebook\.com/([^/?#]+)', og_url)
            if og_match:
                og_username = og_match.group(1)
                # Skip if it's a system path (reel, share, watch, etc.)
                if og_username.lower() not in USERNAME_BLACKLIST and len(og_username) > 2:
                    debug['dom_chosen_method'] = 'og_url'
                    debug['dom_chosen_username'] = og_username
                    return f'https://www.facebook.com/{og_username}', og_username
        
        # ===== PRIORITY 2: Scored candidates with avatar/image =====
        candidates = result.get('candidates', [])
        scored = []
        for c in candidates:
            score = 0
            if c.get('has_avatar'):
                score += 10
            if c.get('has_img'):
                score += 5
            if c.get('text') and len(c.get('text')) > 1:
                score += 3
            # Prefer shorter, simpler usernames
            if '.' in c.get('username', ''):
                score += 2
            scored.append((score, c))
        
        scored.sort(key=lambda x: x[0], reverse=True)
        
        # Only take if score is meaningful (>5, must have image/avatar)
        if scored and scored[0][0] > 5:
            best = scored[0][1]
            username = best['username']
            debug['dom_chosen_method'] = 'scored_link'
            debug['dom_chosen_username'] = username
            debug['dom_chosen_score'] = scored[0][0]
            return f'https://www.facebook.com/{username}', username
        
        # ===== PRIORITY 3: profile.php?id= (last resort) =====
        if result.get('profile_php_ids'):
            pid = result['profile_php_ids'][0]['id']
            debug['dom_chosen_method'] = 'profile_php_fallback'
            debug['dom_chosen_username'] = f'profile.php?id={pid}'
            return f'https://www.facebook.com/profile.php?id={pid}', f'profile.php?id={pid}'
        
        debug['dom_chosen_method'] = 'none'
        return None, None
    except Exception as e:
        debug['dom_error'] = str(e)[:300]
        return None, None


def parse_view_count_string(s):
    """
    Parse "1.2K", "185", "1,5K", "2,8 triệu" → integer.
    Uses round() to avoid float precision issues (4.1 * 1000000 = 4099999.999).
    """
    if not s:
        return 0
    s = s.strip().replace(',', '.')
    
    # "X tri[ệe]u" (million)
    m = re.match(r'^([\d.]+)\s*tri[ệe]u', s, re.IGNORECASE)
    if m:
        try:
            return round(float(m.group(1)) * 1_000_000)
        except:
            return 0
    
    # "X K" or "X k"
    m = re.match(r'^([\d.]+)\s*[KkN]', s)
    if m:
        try:
            return round(float(m.group(1)) * 1_000)
        except:
            return 0
    
    # "X M" (rare)
    m = re.match(r'^([\d.]+)\s*M', s)
    if m:
        try:
            return round(float(m.group(1)) * 1_000_000)
        except:
            return 0
    
    # Plain digits
    digits = re.sub(r'[^\d]', '', s)
    if digits:
        try:
            return int(digits)
        except:
            return 0
    
    return 0


def try_reel_grid_for_views(browser, profile_url, target_post_id, result):
    """
    V8.3 REEL GRID: Navigate sang profile reels grid để lấy view count.
    FB hiển thị views ở grid (👁️ 185) nhưng KHÔNG ở single reel page.
    
    Args:
        browser: Playwright browser instance (no proxy)
        profile_url: URL profile của owner (e.g. https://www.facebook.com/the.mobifone)
        target_post_id: post_id cần tìm view (từ step 1)
        result: dict to update debug info
    
    Returns:
        int: view count hoặc 0
    """
    if not profile_url or not target_post_id:
        result['debug']['grid_skip_reason'] = 'no_profile_url_or_post_id'
        return 0
    
    # Build reels grid URL
    if 'profile.php?id=' in profile_url:
        grid_url = profile_url + '&sk=reels'
    else:
        # Strip trailing slash, add /reels/
        clean_url = profile_url.rstrip('/')
        grid_url = clean_url + '/reels/'
    
    result['debug']['grid_url_attempted'] = grid_url
    
    try:
        # Load cookies for grid scraping (engagement context)
        cookies = parse_netscape_cookies(COOKIES_PATH)
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1366, 'height': 1500},  # Tall to load more reels
            locale='vi-VN',
            timezone_id='Asia/Ho_Chi_Minh',
            extra_http_headers={
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            }
        )
        if cookies:
            context.add_cookies(cookies)
        
        page = context.new_page()
        
        try:
            page.goto(grid_url, wait_until='domcontentloaded', timeout=30000)
            page.wait_for_timeout(3500)  # Wait for reels grid to render
        except Exception as e:
            logger.warning(f'Grid: page.goto failed: {e}')
            result['debug']['grid_goto_error'] = str(e)[:200]
        
        html = page.content()
        result['debug']['grid_html_length'] = len(html)
        
        # Strategy 1: Search for reels grid data in JSON (preferred)
        # FB embed reels list with: {"id":"<post_id>","play_count":<int>}
        target_views = 0
        
        # Pattern A: video_view_count near post_id
        for pat in [
            r'"id"\s*:\s*"' + re.escape(target_post_id) + r'"[^}]*?"play_count"\s*:\s*(\d+)',
            r'"id"\s*:\s*"' + re.escape(target_post_id) + r'"[^}]*?"video_view_count"\s*:\s*(\d+)',
            r'"play_count"\s*:\s*(\d+)[^}]*?"id"\s*:\s*"' + re.escape(target_post_id) + r'"',
            r'"video_view_count"\s*:\s*(\d+)[^}]*?"id"\s*:\s*"' + re.escape(target_post_id) + r'"',
        ]:
            m = re.search(pat, html)
            if m:
                target_views = int(m.group(1))
                result['debug']['grid_match_method'] = 'json_pattern'
                break
        
        # Strategy 2: Use DOM to find views via JS
        if target_views == 0:
            try:
                dom_data = page.evaluate("""
                    (targetPostId) => {
                        const results = [];
                        // Find all reel links containing the post_id
                        const allLinks = document.querySelectorAll('a[href*="/reel/"]');
                        for (const link of allLinks) {
                            const href = link.getAttribute('href') || '';
                            // Look at the parent container
                            let container = link;
                            for (let i = 0; i < 8; i++) {
                                if (!container.parentElement) break;
                                container = container.parentElement;
                                const text = (container.innerText || '').trim();
                                // Match patterns like "👁️ 185" or just "185" near eye icon
                                // Also match Vietnamese number formats: "1,5K", "2,8 triệu"
                                if (text && text.length < 200) {
                                    const matches = text.match(/[\\d.,]+\\s*(?:triệu|tri[eệ]u|K|k|N|M)?/g);
                                    if (matches) {
                                        results.push({
                                            href: href,
                                            container_text: text.substring(0, 150),
                                            view_strings: matches
                                        });
                                        break;
                                    }
                                }
                            }
                        }
                        // Also try to get all text contains the post_id
                        return {
                            reel_links: results.slice(0, 20),
                            full_text_length: document.body ? document.body.innerText.length : 0,
                        };
                    }
                """, target_post_id)
                
                result['debug']['grid_dom_links_count'] = len(dom_data.get('reel_links', []))
                result['debug']['grid_full_text_length'] = dom_data.get('full_text_length', 0)
                
                # Find link matching target_post_id
                for link_data in dom_data.get('reel_links', []):
                    if target_post_id in link_data.get('href', ''):
                        view_strings = link_data.get('view_strings', [])
                        # Get the largest number found
                        for vs in view_strings:
                            v = parse_view_count_string(vs)
                            if v > target_views:
                                target_views = v
                        if target_views > 0:
                            result['debug']['grid_match_method'] = 'dom_match'
                            result['debug']['grid_match_text'] = link_data.get('container_text', '')[:100]
                            break
            except Exception as e:
                result['debug']['grid_dom_error'] = str(e)[:200]
        
        # Strategy 3: Page innertext fallback
        if target_views == 0:
            try:
                innertext = page.evaluate('document.body ? document.body.innerText : ""')
                result['debug']['grid_innertext_length'] = len(innertext)
                
                # Save sample for debug
                if target_post_id[:6] in innertext:
                    idx = innertext.find(target_post_id[:6])
                    result['debug']['grid_innertext_around_postid'] = innertext[max(0, idx-100):idx+200]
            except Exception:
                pass
        
        result['debug']['grid_target_post_id'] = target_post_id
        result['debug']['grid_views_found'] = target_views
        
        context.close()
        return target_views
    except Exception as e:
        logger.exception(f'Reel grid retry failed: {e}')
        result['debug']['grid_error'] = str(e)[:300]
        return 0


def try_vn_proxy_for_views(p, url, cookies, result):
    """
    V8.2 SMART ROUTING: Retry scrape với VN residential proxy 
    để fix views=0 cho video FB ẩn server-side cho datacenter IP.
    
    - Random rotate qua 10 IP VN: vn-1 → vn-10
    - Chỉ chạy desktop_cookies mode (đỡ tốn bandwidth)
    - Tìm views từ HTML, network responses, mobile innertext
    
    NOTE V8.2.1: Nhận playwright instance `p` từ caller để tránh
    "Sync API inside asyncio loop" error trên Render.
    """
    proxy_config = get_random_vn_proxy()
    if not proxy_config:
        logger.warning('VN Proxy: not configured')
        return 0
    
    proxy_id = proxy_config.pop('proxy_id', 'unknown')
    result['debug']['proxy_id_used'] = proxy_id
    
    proxy_browser = None
    try:
        # Launch SEPARATE browser instance WITH PROXY
        # (reuse the same `p` playwright instance to avoid asyncio conflict)
        proxy_browser = p.chromium.launch(
            headless=True,
            proxy=proxy_config,  # KEY: dùng VN proxy
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
                '--no-first-run',
                '--disable-infobars',
            ]
        )
        
        context = proxy_browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1366, 'height': 768},
            locale='vi-VN',
            timezone_id='Asia/Ho_Chi_Minh',
            extra_http_headers={
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            }
        )
        context.add_cookies(cookies)
        
        page = context.new_page()
        
        # Track network responses for view counts
        network_views = []
        
        def handle_response(response):
            try:
                if 'graphql' in response.url or 'video' in response.url:
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '')
                        if 'json' in content_type or 'javascript' in content_type:
                            body = response.text()
                            if body:
                                found = search_views_in_text(body)
                                if found > 0:
                                    network_views.append(found)
            except Exception:
                pass
        
        page.on('response', handle_response)
        
        try:
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            page.wait_for_timeout(3000)
        except Exception as e:
            logger.warning(f'VN Proxy: page.goto failed: {e}')
        
        html = page.content()
        
        try:
            innertext = page.evaluate('document.body ? document.body.innerText : ""')
        except Exception:
            innertext = ''
        
        result['debug']['proxy_html_length'] = len(html)
        result['debug']['proxy_innertext_length'] = len(innertext)
        
        # Search views in HTML + innertext + network
        all_text = html + '\n' + innertext
        html_views = search_views_in_text(all_text)
        net_views = max(network_views) if network_views else 0
        
        result['debug']['proxy_html_views'] = html_views
        result['debug']['proxy_network_views'] = net_views
        
        final_proxy_views = max(html_views, net_views)
        
        context.close()
        proxy_browser.close()
        
        return final_proxy_views
    except Exception as e:
        logger.exception(f'VN Proxy retry failed: {e}')
        result['debug']['proxy_error'] = str(e)[:300]
        try:
            if proxy_browser:
                proxy_browser.close()
        except Exception:
            pass
        return 0


def scrape_with_playwright(url):
    cookies = parse_netscape_cookies(COOKIES_PATH)
    if not cookies:
        return {'success': False, 'error': 'No cookies loaded'}
    
    result = {
        'success': False,
        'data': {
            'views': 0, 'likes': 0, 'comments': 0, 'shares': 0,
            'caption': '', 'thumbnail': '', 'username': '', 'post_id': None,
            'video_url': '', 'reactions_breakdown': {},
        },
        'debug': {
            'final_url': '', 'page_title': '',
            'cookies_count': len(cookies),
            'extracted_data': {},
            'mode_used': '',
            'tried_modes': [],
            'view_sources': {},
        }
    }
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-gpu',
                    '--no-first-run',
                    '--disable-infobars',
                ]
            )
            
            # === ATTEMPT 1: Desktop with cookies (engagement + metadata) ===
            views_1 = try_desktop_with_cookies(browser, url, cookies, result)
            result['debug']['view_sources']['desktop_cookies'] = views_1
            gc.collect()  # V8.4: Force GC after each mode
            
            # === ATTEMPT 2: iPhone 15 anonymous (NEW) ===
            views_2 = try_anonymous_with_fingerprint(
                browser, url, FINGERPRINT_IPHONE_15, 'iphone15', result['debug']
            )
            result['debug']['view_sources']['iphone15_anon'] = views_2
            gc.collect()  # V8.4: Force GC
            
            # === V8.4: SKIPPED Android S24 + mbasic (ít hiệu quả, save 30s) ===
            views_3 = 0
            views_4 = 0
            result['debug']['view_sources']['android_anon'] = 'skipped_v84'
            result['debug']['view_sources']['mbasic_cookies'] = 'skipped_v84'
            
            # === ATTEMPT 5: Mobile m.facebook.com (engagement backup) ===
            try_mobile_mode(browser, url, cookies, result)
            gc.collect()  # V8.4: Force GC
            
            # COMBINE: Take MAX views from all sources
            final_views = max(views_1, views_2, views_3, views_4)
            if final_views > 0:
                result['data']['views'] = final_views
            
            # === V8.3 REEL GRID: Nếu views=0, scrape profile reels grid ===
            # FB ẩn views ở single reel page nhưng HIỆN ở profile reels grid (👁️ 185)
            result['debug']['grid_attempted'] = False
            result['debug']['grid_views'] = 0
            
            if result['data']['views'] == 0:
                # V8.3.1: Try DOM-extracted URL first (already obtained in desktop_with_cookies)
                profile_url = result['debug'].get('dom_profile_url')
                owner_username = result['debug'].get('dom_profile_username')
                source = 'dom' if profile_url else None
                
                # Fallback to HTML regex extraction
                if not profile_url:
                    desktop_html = result['debug'].get('html_full_for_grid', '')
                    html_extract_debug = {}
                    profile_url, owner_username = extract_profile_url_from_html(
                        desktop_html, html_extract_debug
                    )
                    result['debug']['html_profile_extraction'] = html_extract_debug
                    if profile_url:
                        source = 'html'
                
                result['debug']['grid_profile_url'] = profile_url
                result['debug']['grid_owner_username'] = owner_username
                result['debug']['grid_profile_source'] = source
                
                target_post_id = result['data'].get('post_id')
                
                if profile_url and target_post_id:
                    logger.info(f'🎩 Views=0, thử Reel Grid: {profile_url} (source: {source})')
                    grid_views = try_reel_grid_for_views(browser, profile_url, target_post_id, result)
                    result['debug']['grid_attempted'] = True
                    result['debug']['grid_views'] = grid_views
                    
                    if grid_views > 0:
                        result['data']['views'] = grid_views
                        logger.info(f'✅ Reel Grid fix views=0! Got {grid_views} views')
                else:
                    result['debug']['grid_skip_reason'] = 'no_profile_url_or_post_id'
            else:
                result['debug']['grid_skipped_reason'] = 'views_already_found'
            # === END V8.3 REEL GRID ===
            
            # === V8.5 NEW: TRY /videos/ DIRECT URL (cho Profile cá nhân) ===
            # Profile cá nhân (không phải Page) thường lưu video ở /videos/[id]
            # thay vì /reel/[id]. Try fetch URL này để lấy views.
            result['debug']['videos_url_attempted'] = False
            result['debug']['videos_url_views'] = 0
            
            if result['data']['views'] == 0:
                profile_url_v85 = result['debug'].get('dom_profile_url') or result['debug'].get('grid_profile_url')
                target_post_id_v85 = result['data'].get('post_id')
                
                if profile_url_v85 and target_post_id_v85:
                    # Try multiple URL patterns
                    urls_to_try = [
                        f"{profile_url_v85.rstrip('/')}/videos/{target_post_id_v85}",
                        f"{profile_url_v85.rstrip('/')}/videos/{target_post_id_v85}/",
                    ]
                    
                    for v85_url in urls_to_try:
                        try:
                            logger.info(f'🎩 V8.5: Try /videos/ URL: {v85_url}')
                            result['debug']['videos_url_attempted'] = True
                            result['debug']['videos_url'] = v85_url
                            
                            v85_context = browser.new_context(
                                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                                viewport={'width': 1366, 'height': 900},
                                locale='vi-VN',
                                timezone_id='Asia/Ho_Chi_Minh',
                            )
                            v85_context.add_cookies(cookies)
                            v85_page = v85_context.new_page()
                            
                            try:
                                v85_page.goto(v85_url, wait_until='domcontentloaded', timeout=20000)
                                v85_page.wait_for_timeout(3000)
                                
                                v85_html = v85_page.content()
                                result['debug']['videos_url_html_length'] = len(v85_html)
                                
                                # === V8.6 NEW: DETECT LIVE REPLAY ===
                                # Live replay videos có markers khác. play_count = peak concurrent
                                # viewers (lúc live), KHÔNG phải replay views.
                                live_markers = [
                                    '"was_live_broadcast":true',
                                    '"broadcast_status":"VOD_READY"',
                                    '"broadcast_status":"LIVE_STOPPED"',
                                    '"is_live_streaming"',
                                    '"live_video_id"',
                                    '"liveBroadcastId"',
                                    '/share/v/',  # URL pattern cho live replay
                                ]
                                is_live_replay = any(marker in v85_html for marker in live_markers)
                                result['debug']['v86_is_live_replay'] = is_live_replay
                                result['debug']['v86_live_markers_found'] = [m for m in live_markers if m in v85_html]
                                
                                # Try multiple patterns to find views
                                v85_views = 0
                                
                                if is_live_replay:
                                    # === V8.8 SMART MATCH: play_count gần post_id mới đúng ===
                                    # KEY INSIGHT từ V8.7 debug:
                                    # - "play_count" GẦN post_id = views thật (UI hiển thị)
                                    # - "play_count" XA post_id = peak live viewers (sai)
                                    # - "video_view_count" = 3-second views (số nhỏ hơn, không phải UI)
                                    logger.info(f'🎩 V8.8: LIVE REPLAY - smart proximity match')
                                    
                                    # 1. Tìm vị trí post_id
                                    post_id_for_match = result['data'].get('post_id', '')
                                    post_id_pos = -1
                                    if post_id_for_match:
                                        post_id_pos = v85_html.find(f'"{post_id_for_match}"')
                                    
                                    # 2. Patterns ưu tiên cho LIVE REPLAY
                                    # play_count_reduced > play_count > video_view_count
                                    # (play_count_reduced = số UI hiển thị, ưu tiên nhất)
                                    patterns_with_names = [
                                        (r'"play_count_reduced"\s*:\s*"(\d+)"', 'play_count_reduced'),
                                        (r'"play_count"\s*:\s*(\d+)', 'play_count'),
                                        (r'"video_view_count"\s*:\s*(\d+)', 'video_view_count'),
                                        (r'"total_view_count"\s*:\s*(\d+)', 'total_view_count'),
                                        (r'"post_view_count"\s*:\s*(\d+)', 'post_view_count'),
                                    ]
                                    
                                    # 3. Tìm tất cả candidates với position
                                    all_candidates = []
                                    for pattern, field_name in patterns_with_names:
                                        for match in re.finditer(pattern, v85_html):
                                            value = int(match.group(1))
                                            position = match.start()
                                            distance = abs(position - post_id_pos) if post_id_pos > 0 else 999999999
                                            
                                            ctx_pre = v85_html[max(0, position-100):position]
                                            ctx_post = v85_html[position:min(len(v85_html), position+150)]
                                            
                                            all_candidates.append({
                                                'field': field_name,
                                                'value': value,
                                                'position': position,
                                                'distance': distance,
                                                'context_pre': ctx_pre[-100:],
                                                'context_post': ctx_post[:150],
                                            })
                                    
                                    result['debug']['v88_total_candidates'] = len(all_candidates)
                                    result['debug']['v88_post_id_position'] = post_id_pos
                                    
                                    # 4. SMART MATCH ALGORITHM:
                                    # Step 1: Tìm "feedback" object chứa post_id
                                    # FB lưu: ...id:"<post_id>"},...play_count:XXX,..."id":"ZmVlZGJhY2s6<base64>"...
                                    # base64 của "feedback:<post_id>" → có chứa post_id
                                    
                                    final_views = 0
                                    final_source = 'unknown'
                                    
                                    if post_id_pos > 0:
                                        # Tìm trong window [post_id_pos, post_id_pos + 1000]
                                        # Đây là sau post_id, nơi FB lưu play_count cho video này
                                        window_start = post_id_pos
                                        window_end = post_id_pos + 1500  # 1500 chars sau post_id
                                        
                                        # Lọc candidates trong window
                                        in_window = [c for c in all_candidates 
                                                    if window_start <= c['position'] <= window_end]
                                        
                                        # Sort by position (gần post_id trước)
                                        in_window.sort(key=lambda x: x['position'])
                                        
                                        result['debug']['v88_in_window_count'] = len(in_window)
                                        result['debug']['v88_in_window_top5'] = in_window[:5]
                                        
                                        # Ưu tiên play_count_reduced (chính xác UI)
                                        for cand in in_window:
                                            if cand['field'] == 'play_count_reduced':
                                                final_views = cand['value']
                                                final_source = 'window_play_count_reduced'
                                                logger.info(f'✅ V8.8 WINDOW play_count_reduced: {final_views}')
                                                break
                                        
                                        # Fallback: play_count trong window
                                        if final_views == 0:
                                            for cand in in_window:
                                                if cand['field'] == 'play_count':
                                                    final_views = cand['value']
                                                    final_source = 'window_play_count'
                                                    logger.info(f'✅ V8.8 WINDOW play_count: {final_views}')
                                                    break
                                        
                                        # Fallback: video_view_count trong window
                                        if final_views == 0:
                                            for cand in in_window:
                                                if cand['field'] == 'video_view_count':
                                                    final_views = cand['value']
                                                    final_source = 'window_video_view_count'
                                                    logger.info(f'✅ V8.8 WINDOW video_view_count: {final_views}')
                                                    break
                                    
                                    # 5. NẾU KHÔNG TÌM ĐƯỢC TRONG WINDOW → smart fallback
                                    if final_views == 0:
                                        # Lấy candidates SẮP XẾP THEO DISTANCE
                                        all_candidates.sort(key=lambda x: x['distance'])
                                        
                                        # Loại bỏ candidates quá xa (> 50000 chars - chắc chắn là video khác)
                                        nearby = [c for c in all_candidates if c['distance'] < 50000]
                                        
                                        # Ưu tiên play_count_reduced
                                        for cand in nearby:
                                            if cand['field'] == 'play_count_reduced':
                                                final_views = cand['value']
                                                final_source = 'nearby_play_count_reduced'
                                                break
                                        
                                        if final_views == 0:
                                            for cand in nearby:
                                                if cand['field'] == 'play_count':
                                                    final_views = cand['value']
                                                    final_source = 'nearby_play_count'
                                                    break
                                        
                                        if final_views == 0:
                                            for cand in nearby:
                                                if cand['field'] == 'video_view_count':
                                                    final_views = cand['value']
                                                    final_source = 'nearby_video_view_count'
                                                    break
                                    
                                    # 6. UI fallback: tìm "X lượt xem" gần post_id nhất
                                    if final_views == 0 and post_id_pos > 0:
                                        ui_pattern = r'(\d+(?:[.,]\d+)?(?:\s*(?:K|M|nghìn|triệu))?)\s*l[uượ]+t\s*xem'
                                        ui_matches = []
                                        for match in re.finditer(ui_pattern, v85_html, re.IGNORECASE):
                                            raw = match.group(1)
                                            parsed = parse_view_count_string(raw)
                                            pos = match.start()
                                            distance = abs(pos - post_id_pos)
                                            ui_matches.append({
                                                'parsed': parsed, 
                                                'position': pos,
                                                'distance': distance,
                                                'raw': raw
                                            })
                                        if ui_matches:
                                            ui_matches.sort(key=lambda x: x['distance'])
                                            final_views = ui_matches[0]['parsed']
                                            final_source = 'ui_luot_xem_nearest'
                                    
                                    v85_views = final_views
                                    result['debug']['v88_final_views'] = final_views
                                    result['debug']['v88_final_source'] = final_source
                                    result['debug']['videos_url_match_pattern'] = 'v88_smart_window'
                                    result['debug']['v86_field_used'] = final_source
                                    
                                    if final_views > 0:
                                        logger.info(f'✅ V8.8 FINAL: {final_views} views via {final_source}')
                                else:
                                    # === V8.5 LOGIC: Reel thường - dùng play_count ===
                                    for pattern in [
                                        r'"play_count"\s*:\s*(\d+)',
                                        r'"video_view_count"\s*:\s*(\d+)',
                                        r'"viewCount"\s*:\s*(\d+)',
                                        r'"viewer_count"\s*:\s*(\d+)',
                                    ]:
                                        matches = re.findall(pattern, v85_html)
                                        if matches:
                                            v85_views = max(int(m) for m in matches)
                                            result['debug']['videos_url_match_pattern'] = pattern
                                            break
                                    
                                    # Pattern 2: Vietnamese "lượt xem" or "views" near number
                                    if v85_views == 0:
                                        vn_patterns = [
                                            r'(\d+(?:[.,]\d+)?(?:\s*(?:K|M|nghìn|triệu))?)\s*l[uượ]+t\s*xem',
                                            r'(\d+(?:[.,]\d+)?(?:\s*(?:K|M))?)\s*views?',
                                        ]
                                        for vn_pat in vn_patterns:
                                            vn_matches = re.findall(vn_pat, v85_html, re.IGNORECASE)
                                            if vn_matches:
                                                for vn_m in vn_matches:
                                                    parsed = parse_view_count_string(vn_m)
                                                    if parsed > v85_views:
                                                        v85_views = parsed
                                                if v85_views > 0:
                                                    result['debug']['videos_url_match_pattern'] = vn_pat
                                                    break
                                
                                if v85_views > 0:
                                    result['debug']['videos_url_views'] = v85_views
                                    result['data']['views'] = v85_views
                                    logger.info(f'✅ V8.5 /videos/ URL fix views=0! Got {v85_views} views from {v85_url}')
                                    v85_page.close()
                                    v85_context.close()
                                    break  # Stop trying other URL patterns
                            except Exception as goto_err:
                                logger.warning(f'V8.5 /videos/ URL goto failed: {goto_err}')
                                result['debug']['videos_url_error'] = str(goto_err)[:200]
                            
                            v85_page.close()
                            v85_context.close()
                            
                        except Exception as v85_err:
                            logger.warning(f'V8.5 /videos/ URL exception: {v85_err}')
                            result['debug']['videos_url_exception'] = str(v85_err)[:200]
                            continue
            # === END V8.5 /videos/ URL FALLBACK ===
            
            # === V8.11 SMART ROUTING: Skip proxy cho live replay (avoid wrong views) ===
            # Bug case (Ngọc Nguyễn live): proxy lấy peak concurrent viewers (61K)
            # thay vì replay views (3.5K). Nếu là live replay → KHÔNG dùng proxy.
            result['debug']['proxy_used'] = False
            result['debug']['proxy_views'] = 0
            
            # V8.11: Detect live replay markers
            is_live_replay_v811 = False
            try:
                if html and len(html) > 1000:
                    live_markers_v811 = [
                        '"was_live_broadcast":true',
                        '"broadcast_status":"VOD_READY"',
                        '"broadcast_status":"LIVE_STOPPED"',
                        '"is_live_streaming"',
                        '"live_video_id"',
                        '"liveBroadcastId"',
                    ]
                    is_live_replay_v811 = any(m in html for m in live_markers_v811)
                
                # Fallback: detect via final_url
                if not is_live_replay_v811:
                    final_url_check = result.get('debug', {}).get('iphone15_url_after_redirect', '')
                    if '/share/v/' in final_url_check or '/share/v/' in url:
                        is_live_replay_v811 = True
                
                # Fallback: detect via format_detected
                if result.get('debug', {}).get('format_detected') == 'video_live':
                    is_live_replay_v811 = True
            except:
                pass
            
            result['debug']['v811_is_live_replay_global'] = is_live_replay_v811
            
            if (result['data']['views'] == 0 and PROXY_ENABLED and not is_live_replay_v811):
                logger.info('🎩 Views=0, retry với VN residential proxy...')
                proxy_views = try_vn_proxy_for_views(p, url, cookies, result)
                result['debug']['proxy_used'] = True
                result['debug']['proxy_views'] = proxy_views
                
                if proxy_views > 0:
                    result['data']['views'] = proxy_views
                    logger.info(f'✅ VN Proxy fix views=0! Got {proxy_views} views')
            elif is_live_replay_v811 and result['data']['views'] == 0:
                logger.warning('⚠️ V8.11: SKIP proxy cho live replay (avoid peak viewer bug)')
                result['debug']['proxy_skipped_reason'] = 'v811_live_replay_skip_proxy'
            elif result['data']['views'] == 0 and not PROXY_ENABLED:
                result['debug']['proxy_used'] = False
                result['debug']['proxy_note'] = 'Views=0 but proxy not configured'
            else:
                result['debug']['proxy_skipped_reason'] = 'views_already_found'
            # === END V8.11 SMART ROUTING ===
            
            browser.close()
            
            if (result['data']['views'] > 0 or 
                result['data']['likes'] > 0 or
                result['data']['caption']):
                result['success'] = True
    except Exception as e:
        logger.exception('Scrape failed')
        result['error'] = str(e)
        result['error_type'] = type(e).__name__
    
    # V8.3: Remove cached HTML before returning (save bandwidth)
    if 'html_full_for_grid' in result.get('debug', {}):
        del result['debug']['html_full_for_grid']
    
    return result


def try_desktop_with_cookies(browser, url, cookies, result):
    """Mode 1: Desktop with cookies for engagement + metadata"""
    try:
        result['debug']['tried_modes'].append('desktop_cookies')
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1366, 'height': 768},
            locale='vi-VN',
            timezone_id='Asia/Ho_Chi_Minh',
            extra_http_headers={
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            }
        )
        
        context.add_cookies(cookies)
        page = context.new_page()
        
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en'] });
        """)
        
        response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
        time.sleep(7)
        simulate_human(page)
        time.sleep(2)
        
        final_url = page.url
        page_title = page.title()
        result['debug']['final_url'] = final_url
        result['debug']['page_title'] = page_title
        
        if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
            result['error'] = 'Redirected to login'
            context.close()
            return 0
        
        html = page.content()
        result['debug']['html_length'] = len(html)
        
        # V8.3: Cache HTML for reel grid profile extraction (max 500KB)
        result['debug']['html_full_for_grid'] = html[:500000] if len(html) > 500000 else html
        
        views = search_views_in_text(html)
        
        extracted = extract_metadata_from_html(html)
        result['debug']['extracted_data'] = extracted
        
        dom_data = extract_username_from_dom(page)
        
        # V8.3.1: Also extract profile URL via DOM (for reel grid scraping)
        dom_profile_debug = {}
        dom_profile_url, dom_profile_username = extract_profile_url_from_dom(page, dom_profile_debug)
        result['debug']['dom_profile_extraction'] = dom_profile_debug
        if dom_profile_url:
            result['debug']['dom_profile_url'] = dom_profile_url
            result['debug']['dom_profile_username'] = dom_profile_username
        
        result['debug']['mode_used'] = 'desktop_cookies'
        
        raw_caption = extracted.get('caption', '')
        if raw_caption:
            decoded = decode_unicode_string(raw_caption)
            result['data']['caption'] = decoded[:5000]
        
        raw_thumbnail = extracted.get('thumbnail', '')
        if raw_thumbnail:
            result['data']['thumbnail'] = decode_html_entities(raw_thumbnail)
        
        result['data']['username'] = dom_data.get('username', '')
        result['data']['post_id'] = extracted.get('post_id')
        result['data']['video_url'] = final_url
        
        # === V8.10: Extract engagement từ HTML JSON (fix mobile login wall) ===
        post_id_for_eng = extracted.get('post_id')
        v810_engagement = extract_engagement_from_html(html, post_id_for_eng, result['debug'])
        
        # Chỉ overwrite nếu V8.10 tìm được giá trị > 0
        # (ưu tiên data từ mobile mode nếu có, V8.10 là fallback)
        if v810_engagement['likes'] > 0:
            result['data']['likes'] = v810_engagement['likes']
            logger.info(f'✅ V8.10 desktop likes: {v810_engagement["likes"]}')
        if v810_engagement['comments'] > 0:
            result['data']['comments'] = v810_engagement['comments']
            logger.info(f'✅ V8.10 desktop comments: {v810_engagement["comments"]}')
        if v810_engagement['shares'] > 0:
            result['data']['shares'] = v810_engagement['shares']
            logger.info(f'✅ V8.10 desktop shares: {v810_engagement["shares"]}')
        
        result['debug']['v810_desktop_engagement'] = v810_engagement
        # === END V8.10 ===
        
        context.close()
        return views
    except Exception as e:
        logger.warning(f'Desktop mode failed: {e}')
        return 0


# ==========================================
# PATCHED: try_mobile_mode
# V8.1.2: Always set engagement + use mobile_views from video/live
# ==========================================

def try_mobile_mode(browser, url, cookies, result):
    """Mode 5: Mobile m.facebook.com for engagement (PATCHED V8.1.2)"""
    try:
        result['debug']['tried_modes'].append('mobile_cookies')
        
        mobile_url = url.replace('www.facebook.com', 'm.facebook.com')
        if 'm.facebook.com' not in mobile_url:
            mobile_url = mobile_url.replace('facebook.com', 'm.facebook.com')
        
        context = browser.new_context(
            user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1',
            viewport={'width': 414, 'height': 896},
            locale='vi-VN',
            timezone_id='Asia/Ho_Chi_Minh',
            extra_http_headers={
                'Accept-Language': 'vi-VN,vi;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
        )
        
        context.add_cookies(cookies)
        page = context.new_page()
        
        response = page.goto(mobile_url, wait_until='domcontentloaded', timeout=45000)
        time.sleep(random.uniform(5, 6))
        
        for offset in [200, 400]:
            try:
                page.evaluate(f'window.scrollTo(0, {offset})')
                time.sleep(random.uniform(0.8, 1.2))
            except:
                pass
        time.sleep(2)
        
        mobile_innertext = ''
        try:
            mobile_innertext = page.evaluate('document.body.innerText || ""')
            result['debug']['mobile_innertext_length'] = len(mobile_innertext)
            result['debug']['mobile_innertext_sample'] = mobile_innertext[:1000]
        except:
            pass
        
        # PATCHED: parse with icon-mapping + dual icon set
        engagement = parse_mobile_engagement(mobile_innertext, result['debug'])
        
        # Always set (even 0 is correct for hidden metrics)
        result['data']['likes'] = engagement['likes']
        result['data']['comments'] = engagement['comments']
        result['data']['shares'] = engagement['shares']
        
        # Mobile views: from "lượt xem" (video/live) or search_views_in_text
        mobile_views = engagement.get('mobile_views', 0)
        if mobile_views == 0 and mobile_innertext:
            mobile_views = search_views_in_text(mobile_innertext)
        if mobile_views > 0:
            result['debug']['view_sources']['mobile_cookies'] = mobile_views
            if mobile_views > result['data']['views']:
                result['data']['views'] = mobile_views
        
        context.close()
    except Exception as e:
        logger.warning(f'Mobile mode failed: {e}')


def extract_username_from_dom(page):
    data = {}
    try:
        js_results = page.evaluate("""
            () => {
                const results = [];
                const profileLinks = document.querySelectorAll('a[href*="/"]');
                for (const link of profileLinks) {
                    const href = link.getAttribute('href') || '';
                    const match = href.match(/^https?:\\/\\/(?:www\\.|m\\.)?facebook\\.com\\/([a-zA-Z0-9.]+)(?:\\/|$|\\?)/);
                    if (match) {
                        const text = (link.textContent || '').trim();
                        if (text && text.length > 1 && text.length < 100) {
                            results.push({username: match[1], displayName: text});
                        }
                    }
                }
                return results.slice(0, 20);
            }
        """)
        
        if js_results:
            for m in js_results:
                username = m.get('username', '').lower()
                if username and username not in USERNAME_BLACKLIST:
                    data['username'] = m['username']
                    break
    except:
        pass
    
    return data


def extract_metadata_from_html(html):
    data = {}
    
    for pat in [
        r'"video_id"\s*:\s*"(\d+)"',
        r'"top_level_post_id"\s*:\s*"(\d+)"',
    ]:
        m = re.search(pat, html)
        if m:
            data['post_id'] = m.group(1)
            break
    
    caption_candidates = []
    
    m = re.search(r'<meta\s+property="og:description"\s+content="([^"]+)"', html)
    if m:
        caption_candidates.append(m.group(1))
    
    for pat in [
        r'"message"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
        r'"text"\s*:\s*"((?:[^"\\]|\\.)+)"\s*,\s*"is_explicit_locale"',
        r'"description"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
    ]:
        m = re.search(pat, html)
        if m:
            caption_candidates.append(m.group(1))
    
    if caption_candidates:
        caption_candidates.sort(key=len, reverse=True)
        data['caption'] = caption_candidates[0]
    
    for pat in [
        r'<meta\s+property="og:image"\s+content="([^"]+)"',
        r'"first_frame_thumbnail"\s*:\s*"([^"]+)"',
    ]:
        m = re.search(pat, html)
        if m:
            data['thumbnail'] = m.group(1)[:1000]
            break
    
    return data


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'ok',
        'service': 'Rise City Facebook Scraper \U0001f3a9',
        'version': '8.11-anti-multi-posts',
    })


@app.route('/health', methods=['GET'])
def health():
    cookies_exist = os.path.exists(COOKIES_PATH)
    cookies_count = len(parse_netscape_cookies(COOKIES_PATH)) if cookies_exist else 0
    
    chromium_ok = False
    chromium_path = None
    try:
        with sync_playwright() as p:
            chromium_path = p.chromium.executable_path
            chromium_ok = os.path.exists(chromium_path) if chromium_path else False
    except Exception as e:
        chromium_path = f'Error: {e}'
    
    return jsonify({
        'status': 'ok',
        'cookies_loaded': cookies_exist,
        'cookies_count': cookies_count,
        'chromium_ok': chromium_ok,
        'chromium_path': chromium_path,
        'version': '8.11-anti-multi-posts',
        'proxy_enabled': PROXY_ENABLED,
        'proxy_host': PROXY_HOST if PROXY_ENABLED else None,
        'proxy_country': 'VN' if PROXY_ENABLED else None,
        'proxy_pool_size': 10 if PROXY_ENABLED else 0,
        'concurrency_limit': 2,
        'modes_count': 3,
    })


@app.route('/scrape', methods=['POST'])
def scrape_endpoint():
    """
    V8.9: Always return JSON. Use semaphore (2 concurrent).
    Support X-Bypass-Lock header for retry requests.
    Return data_quality assessment.
    """
    try:
        # Auth check
        api_key = request.headers.get('X-API-Key') or request.headers.get('x-api-key')
        if api_key != API_SECRET:
            return jsonify({'error': 'Unauthorized', 'success': False}), 401
        
        # Parse request
        data = request.get_json(silent=True) or {}
        url = data.get('url', '')
        
        if not url:
            return jsonify({'error': 'Missing url', 'success': False}), 400
        
        # V8.9: Check bypass lock header (cho retry requests)
        bypass_lock = request.headers.get('X-Bypass-Lock', '').lower() in ('true', '1', 'yes')
        
        # V8.9: Concurrency limit (semaphore = 2)
        if not bypass_lock:
            acquired = SCRAPE_SEMAPHORE.acquire(timeout=SCRAPE_LOCK_TIMEOUT)
            if not acquired:
                logger.warning(f'⚠️ Semaphore timeout, refusing request: {url}')
                return jsonify({
                    'success': False,
                    'error': 'Server busy. 2 scrapes in progress. Retry in 30s.',
                    'retry_after': 30,
                    'can_bypass': True,  # Frontend có thể thử với X-Bypass-Lock
                }), 503
        else:
            logger.info(f'🔓 V8.9: Bypass lock enabled for: {url}')
        
        try:
            logger.info(f'🎩 V8.9 Scraping: {url} (bypass={bypass_lock})')
            start_time = time.time()
            result = scrape_with_playwright(url)
            elapsed = time.time() - start_time
            
            # Add timing info
            if isinstance(result, dict):
                result.setdefault('debug', {})['scrape_time_seconds'] = round(elapsed, 2)
                
                # V8.9: DATA QUALITY ASSESSMENT
                # Kiểm tra data có hoàn chỉnh không
                video_data = result.get('data', {})
                views_v = video_data.get('views', 0)
                likes_v = video_data.get('likes', 0)
                comments_v = video_data.get('comments', 0)
                shares_v = video_data.get('shares', 0)
                thumbnail_v = video_data.get('thumbnail', '')
                
                quality_issues = []
                
                # Check 1: Views > 0 nhưng tất cả engagement = 0 (bất thường)
                if views_v > 100 and likes_v == 0 and comments_v == 0 and shares_v == 0:
                    quality_issues.append('zero_engagement_with_views')
                
                # Check 2: Không có thumbnail
                if not thumbnail_v or len(thumbnail_v) < 20:
                    quality_issues.append('missing_thumbnail')
                
                # Check 3: Views = 0 (có thể FB chưa update hoặc video private)
                if views_v == 0:
                    quality_issues.append('zero_views')
                
                # Đánh giá quality
                if not quality_issues:
                    data_quality = 'complete'
                elif len(quality_issues) == 1 and 'missing_thumbnail' in quality_issues:
                    data_quality = 'partial'  # Chỉ thiếu thumbnail (không quan trọng)
                elif 'zero_views' in quality_issues:
                    data_quality = 'incomplete_no_views'
                elif 'zero_engagement_with_views' in quality_issues:
                    data_quality = 'incomplete_no_engagement'
                else:
                    data_quality = 'partial'
                
                result['data_quality'] = data_quality
                result['quality_issues'] = quality_issues
                result['should_retry'] = data_quality.startswith('incomplete')
                
                logger.info(f'✅ V8.9 Done in {elapsed:.1f}s: {url}, quality={data_quality}')
            
            return jsonify(result)
        finally:
            # Force GC + release semaphore (chỉ release nếu đã acquire)
            gc.collect()
            if not bypass_lock:
                SCRAPE_SEMAPHORE.release()
            
    except Exception as e:
        # V8.9: NEVER return HTML 502, always JSON
        logger.exception(f'❌ V8.9 Endpoint error: {e}')
        return jsonify({
            'success': False,
            'error': f'Internal server error: {type(e).__name__}: {str(e)}',
            'data_quality': 'error',
            'should_retry': True,
            'data': {
                'views': 0, 'likes': 0, 'comments': 0, 'shares': 0,
                'caption': '', 'thumbnail': '', 'username': '', 'post_id': None,
            },
        }), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
