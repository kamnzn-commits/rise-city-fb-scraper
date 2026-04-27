"""
Rise City Facebook Scraper API - V8.4 🎩
ĐÀO SÂU GỐC RỄ - PARSE JSON TỪ HTML SOURCE CODE

TRIỆT ĐỂ BỎ:
❌ innertext parsing
❌ icon codepoint mapping  
❌ DOM walker
❌ mobile m.facebook.com engagement
❌ Mọi thứ phụ thuộc giao diện

THAY BẰNG:
✅ Parse JSON embedded trong <script> tags (server-side data)
✅ Match target video bằng post_id/video_id
✅ Extract engagement từ JSON object chính xác
✅ Proximity matching: chỉ lấy data WITHIN 5000 chars của target post_id

FB embed data trong HTML dạng:
- <script type="application/json" data-sjs>...</script>
- require("RelayPrefetchedStreamCache").next(...)
- __comet_data__ / handleWithCustomApplyEach

Trong các JSON blocks, engagement nằm trong:
- "reaction_count":{"count":N}
- "comment_rendering_instance":{"comments":{"total_count":N}}
- "share_count":{"count":N}  
- "video_view_count":N / "play_count":N

Strategy:
1. Desktop Chrome + cookies → lấy 1.4MB HTML
2. Parse TẤT CẢ JSON từ HTML source
3. Tìm post_id → extract engagement trong proximity
4. Fallback: nếu JSON parse fail → regex patterns trên raw HTML
"""
from flask import Flask, request, jsonify, make_response
from playwright.sync_api import sync_playwright
import os
import re
import json
import logging
import time
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

COOKIES_PATH = os.getenv('FB_COOKIES_PATH', '/etc/secrets/cookies.txt')
API_SECRET = os.getenv('API_SECRET', 'rise-city-secret-2026')

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
        .replace('&amp;', '&').replace('&lt;', '<')
        .replace('&gt;', '>').replace('&quot;', '"')
        .replace('&#39;', "'").replace('&apos;', "'"))


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


def parse_number(text):
    """Parse number from various formats: 43, 1.5K, 1,500, etc."""
    if text is None:
        return 0
    if isinstance(text, (int, float)):
        return int(text)
    s = str(text).strip()
    if not s:
        return 0
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
        sl = suffix.lower()
        if 'k' in sl: num *= 1000
        elif 'm' in sl: num *= 1000000
        elif 'b' in sl: num *= 1000000000
        elif 'tr' in sl: num *= 1000000
    return int(num)


# ==========================================
# CORE V8.4: DEEP JSON EXTRACTION FROM HTML
# ==========================================

def extract_post_id_from_url(url):
    """Extract reel/video ID from URL"""
    patterns = [
        r'/reel/(\d+)',
        r'/videos/(\d+)',
        r'video\.php\?v=(\d+)',
        r'/watch/\?v=(\d+)',
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


def deep_extract_from_html(html, target_post_id, debug_info):
    """
    V8.4 CORE: Extract engagement data from FB HTML source code.
    
    Strategy:
    1. Find target post_id position in HTML
    2. Extract data WITHIN proximity (±5000 chars) of post_id
    3. Use JSON patterns to find exact values
    4. Also search entire HTML for og: meta tags (caption, thumbnail)
    """
    data = {
        'views': 0, 'likes': 0, 'comments': 0, 'shares': 0,
        'caption': '', 'thumbnail': '', 'username': '', 'post_id': None,
    }
    
    debug_info['v84_method'] = 'deep_json'
    debug_info['v84_html_length'] = len(html)
    
    # === STEP 1: Find target post_id in HTML ===
    post_id = target_post_id
    if not post_id:
        # Try to find video_id from HTML
        for pat in [
            r'"video_id"\s*:\s*"(\d+)"',
            r'"post_id"\s*:\s*"(\d+)"',
            r'"top_level_post_id"\s*:\s*"(\d+)"',
        ]:
            m = re.search(pat, html)
            if m:
                post_id = m.group(1)
                break
    
    data['post_id'] = post_id
    debug_info['v84_post_id'] = post_id
    
    # === STEP 2: Extract METADATA (og: tags - reliable, always present) ===
    m = re.search(r'<meta\s+property="og:description"\s+content="([^"]+)"', html)
    if m:
        data['caption'] = m.group(1)
    
    # Longer caption from JSON
    caption_pats = [
        r'"message"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
        r'"text"\s*:\s*"((?:[^"\\]|\\.)+)"\s*,\s*"is_explicit_locale"',
    ]
    for pat in caption_pats:
        m = re.search(pat, html)
        if m and len(m.group(1)) > len(data.get('caption', '')):
            data['caption'] = m.group(1)
    
    m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
    if m:
        data['thumbnail'] = decode_html_entities(m.group(1))
    
    # === STEP 3: PROXIMITY-BASED ENGAGEMENT EXTRACTION ===
    # Find ALL positions of post_id in HTML, extract data near each
    if post_id:
        positions = [m.start() for m in re.finditer(re.escape(post_id), html)]
        debug_info['v84_post_id_positions'] = len(positions)
        
        # For each position, search ±5000 chars for engagement data
        engagement_candidates = []
        
        for pos in positions[:20]:  # limit to first 20 occurrences
            start = max(0, pos - 5000)
            end = min(len(html), pos + 5000)
            block = html[start:end]
            
            candidate = extract_engagement_from_block(block)
            if candidate.get('likes', 0) > 0 or candidate.get('comments', 0) > 0:
                engagement_candidates.append(candidate)
        
        debug_info['v84_engagement_candidates'] = len(engagement_candidates)
        
        # Pick the BEST candidate (most complete data)
        if engagement_candidates:
            best = max(engagement_candidates, 
                       key=lambda c: (c.get('likes', 0) > 0) + (c.get('comments', 0) > 0) + (c.get('shares', 0) > 0))
            data['likes'] = best.get('likes', 0)
            data['comments'] = best.get('comments', 0)
            data['shares'] = best.get('shares', 0)
            debug_info['v84_engagement_source'] = 'proximity'
    
    # === STEP 4: VIEW COUNT - search entire HTML ===
    # Views are sometimes in different JSON block than engagement
    view_patterns = [
        (r'"video_view_count"\s*:\s*(\d+)', 'video_view_count'),
        (r'"play_count"\s*:\s*(\d+)', 'play_count'),
        (r'"viewCount"\s*:\s*(\d+)', 'viewCount'),
        (r'"reels_view_count"\s*:\s*(\d+)', 'reels_view_count'),
    ]
    
    view_candidates = []
    for pat, name in view_patterns:
        matches = re.findall(pat, html)
        for m in matches:
            val = int(m)
            if 10 <= val <= 1000000000:
                # PROXIMITY CHECK: only accept if near target post_id
                if post_id:
                    match_obj = re.search(pat, html)
                    if match_obj:
                        match_pos = match_obj.start()
                        # Find nearest post_id occurrence
                        min_distance = float('inf')
                        for pid_pos in [m2.start() for m2 in re.finditer(re.escape(post_id), html)]:
                            dist = abs(match_pos - pid_pos)
                            if dist < min_distance:
                                min_distance = dist
                        if min_distance < 5000:
                            view_candidates.append({'value': val, 'pattern': name, 'distance': min_distance})
                else:
                    view_candidates.append({'value': val, 'pattern': name, 'distance': 0})
    
    debug_info['v84_view_candidates'] = view_candidates[:10]
    
    if view_candidates:
        # Take the one closest to post_id
        best_view = min(view_candidates, key=lambda v: v['distance'])
        data['views'] = best_view['value']
        debug_info['v84_views_source'] = best_view['pattern']
    
    # === STEP 5: USERNAME from HTML source ===
    username_pats = [
        r'"name"\s*:\s*"([^"]+)"\s*,\s*"(?:url|profile_url|id)"',
        r'"owner"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"',
        r'"actor"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"',
    ]
    for pat in username_pats:
        m = re.search(pat, html)
        if m:
            name = m.group(1)
            if len(name) > 1 and len(name) < 100 and name.lower() not in USERNAME_BLACKLIST:
                data['username'] = name
                break
    
    # === STEP 6: FALLBACK - if proximity extraction found nothing ===
    if data['likes'] == 0 and data['comments'] == 0:
        debug_info['v84_fallback'] = True
        fallback = extract_engagement_global(html)
        if fallback.get('likes', 0) > 0:
            data['likes'] = fallback['likes']
            data['comments'] = fallback.get('comments', 0)
            data['shares'] = fallback.get('shares', 0)
            debug_info['v84_engagement_source'] = 'global_fallback'
    
    return data


def extract_engagement_from_block(block):
    """Extract engagement from a JSON block (±5000 chars around post_id)"""
    result = {'likes': 0, 'comments': 0, 'shares': 0}
    
    # Likes/reactions
    likes_pats = [
        r'"reaction_count"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
        r'"reactors"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
        r'"unified_reactors"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
        r'"reaction_count"\s*:\s*(\d+)',
        r'"i18n_reaction_count"\s*:\s*"(\d+)',
    ]
    for pat in likes_pats:
        m = re.search(pat, block)
        if m:
            val = int(m.group(1))
            if val >= 0:
                result['likes'] = val
                break
    
    # Comments
    comments_pats = [
        r'"total_comment_count"\s*:\s*(\d+)',
        r'"comment_count"\s*:\s*\{\s*"total_count"\s*:\s*(\d+)',
        r'"comments"\s*:\s*\{\s*"total_count"\s*:\s*(\d+)',
        r'"total_count"\s*:\s*(\d+)\s*,\s*"[^"]*comment',
    ]
    for pat in comments_pats:
        m = re.search(pat, block)
        if m:
            val = int(m.group(1))
            if val >= 0:
                result['comments'] = val
                break
    
    # Shares
    shares_pats = [
        r'"share_count"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
        r'"reshare_count"\s*:\s*(\d+)',
        r'"share_count"\s*:\s*(\d+)',
    ]
    for pat in shares_pats:
        m = re.search(pat, block)
        if m:
            val = int(m.group(1))
            if val >= 0:
                result['shares'] = val
                break
    
    return result


def extract_engagement_global(html):
    """Global fallback: find FIRST occurrence of engagement patterns in entire HTML"""
    result = {'likes': 0, 'comments': 0, 'shares': 0}
    
    m = re.search(r'"reaction_count"\s*:\s*\{\s*"count"\s*:\s*(\d+)', html)
    if m:
        result['likes'] = int(m.group(1))
    
    m = re.search(r'"total_comment_count"\s*:\s*(\d+)', html)
    if m:
        result['comments'] = int(m.group(1))
    
    m = re.search(r'"share_count"\s*:\s*\{\s*"count"\s*:\s*(\d+)', html)
    if m:
        result['shares'] = int(m.group(1))
    
    return result


# ==========================================
# SCRAPE ORCHESTRATOR
# ==========================================

def simulate_human(page):
    try:
        for _ in range(2):
            page.mouse.move(random.randint(100, 1200), random.randint(100, 600))
            time.sleep(random.uniform(0.3, 0.5))
        page.evaluate('window.scrollTo({top: 300, behavior: "smooth"})')
        time.sleep(1)
    except:
        pass


def scrape_with_playwright(url):
    cookies = parse_netscape_cookies(COOKIES_PATH)
    if not cookies:
        return {'success': False, 'error': 'No cookies loaded'}
    
    # Pre-extract post_id from URL
    target_post_id = extract_post_id_from_url(url)
    
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
            'version': '8.4-deep-source',
            'target_post_id_from_url': target_post_id,
        }
    }
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox', '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-gpu', '--no-first-run',
                ]
            )
            
            # SINGLE MODE: Desktop Chrome + cookies → deep HTML parse
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
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
            
            logger.info(f'Navigating to: {url}')
            response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
            time.sleep(8)
            simulate_human(page)
            time.sleep(2)
            
            final_url = page.url
            page_title = page.title()
            result['debug']['final_url'] = final_url
            result['debug']['page_title'] = page_title
            
            if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
                result['error'] = 'Redirected to login - cookies invalid'
                browser.close()
                return result
            
            # Get full HTML source
            html = page.content()
            result['debug']['html_length'] = len(html)
            
            # If target_post_id not from URL, try from redirect URL
            if not target_post_id:
                target_post_id = extract_post_id_from_url(final_url)
                result['debug']['target_post_id_from_redirect'] = target_post_id
            
            # === V8.4 DEEP EXTRACTION ===
            extracted = deep_extract_from_html(html, target_post_id, result['debug'])
            
            result['data']['views'] = extracted['views']
            result['data']['likes'] = extracted['likes']
            result['data']['comments'] = extracted['comments']
            result['data']['shares'] = extracted['shares']
            result['data']['post_id'] = extracted['post_id']
            result['data']['video_url'] = final_url
            
            # Caption decode
            raw_caption = extracted.get('caption', '')
            if raw_caption:
                result['data']['caption'] = decode_unicode_string(raw_caption)[:5000]
            
            result['data']['thumbnail'] = extracted.get('thumbnail', '')
            result['data']['username'] = extracted.get('username', '')
            
            if (result['data']['views'] > 0 or
                result['data']['likes'] > 0 or
                result['data']['comments'] > 0 or
                result['data']['caption']):
                result['success'] = True
            
            context.close()
            browser.close()
    except Exception as e:
        logger.exception('Scrape failed')
        result['error'] = str(e)
        result['error_type'] = type(e).__name__
    
    return result


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'ok',
        'service': 'Rise City Facebook Scraper \U0001f3a9',
        'version': '8.4-deep-source',
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
        'version': '8.4-deep-source',
    })


@app.route('/scrape', methods=['POST'])
def scrape_endpoint():
    api_key = request.headers.get('X-API-Key') or request.headers.get('x-api-key')
    if api_key != API_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json(silent=True) or {}
    url = data.get('url', '')
    if not url:
        return jsonify({'error': 'Missing url'}), 400
    logger.info(f'Scraping: {url}')
    result = scrape_with_playwright(url)
    return jsonify(result)


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
