"""
Rise City Facebook Scraper API - V9.0 🎩
FINAL - TRIỆT ĐỂ 1 PHÁT ĂN LUÔN

SAU 17 VERSIONS, SỰ THẬT:
- FB KHÔNG GỬI likes/shares/views cho datacenter IP (Render Singapore)
- HTML chỉ có: caption, thumbnail, post_id, total_comment_count
- Network responses: KHÔNG có reaction_count, share_count, video_view_count
- Đây là giới hạn phía Facebook, không phải bug code

V9.0 STRATEGY:
- Render: CHỈ lấy METADATA (caption, thumbnail, post_id, username)
- Engagement (likes, shares, views, comments): để Edge Function gọi Apify
- Render KHÔNG CỐ lấy engagement nữa → nhanh, ổn định, không sai

SIMPLE. CLEAN. ĐÚNG.
"""
from flask import Flask, request, jsonify, make_response
from playwright.sync_api import sync_playwright
import os
import re
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


def extract_post_id_from_url(url):
    for pat in [r'/reel/(\d+)', r'/videos/(\d+)', r'video\.php\?v=(\d+)', r'/watch/\?v=(\d+)']:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


def extract_metadata_from_html(html, target_post_id):
    """Extract ONLY metadata from HTML. No engagement."""
    data = {
        'caption': '', 'thumbnail': '', 'post_id': None, 'username': '',
    }
    
    # Post ID
    if target_post_id:
        data['post_id'] = target_post_id
    else:
        for pat in [r'"video_id"\s*:\s*"(\d+)"', r'"post_id"\s*:\s*"(\d+)"', r'"top_level_post_id"\s*:\s*"(\d+)"']:
            m = re.search(pat, html)
            if m:
                data['post_id'] = m.group(1)
                break
    
    # Caption - take longest
    caption_candidates = []
    m = re.search(r'<meta\s+property="og:description"\s+content="([^"]+)"', html)
    if m:
        caption_candidates.append(m.group(1))
    for pat in [
        r'"message"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
        r'"text"\s*:\s*"((?:[^"\\]|\\.)+)"\s*,\s*"is_explicit_locale"',
    ]:
        m = re.search(pat, html)
        if m:
            caption_candidates.append(m.group(1))
    if caption_candidates:
        caption_candidates.sort(key=len, reverse=True)
        raw = caption_candidates[0]
        data['caption'] = decode_unicode_string(raw)[:5000]
    
    # Thumbnail
    m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
    if m:
        data['thumbnail'] = decode_html_entities(m.group(1))
    
    # Username from DOM source
    for pat in [
        r'"name"\s*:\s*"([^"]+)"\s*,\s*"(?:url|profile_url)"',
        r'"owner"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"',
    ]:
        m = re.search(pat, html)
        if m:
            name = m.group(1)
            if 1 < len(name) < 100 and name.lower() not in USERNAME_BLACKLIST:
                data['username'] = name
                break
    
    return data


def scrape_with_playwright(url):
    cookies = parse_netscape_cookies(COOKIES_PATH)
    if not cookies:
        return {'success': False, 'error': 'No cookies loaded'}
    
    target_post_id = extract_post_id_from_url(url)
    
    result = {
        'success': False,
        'data': {
            'caption': '', 'thumbnail': '', 'username': '', 'post_id': None,
            'video_url': '',
        },
        'debug': {
            'cookies_count': len(cookies),
            'version': '9.0-metadata-only',
            'note': 'V9.0: Render only extracts metadata. Engagement (likes/shares/views) must come from Apify via Edge Function.',
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
            
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                viewport={'width': 1366, 'height': 768},
                locale='vi-VN',
                timezone_id='Asia/Ho_Chi_Minh',
                extra_http_headers={
                    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                }
            )
            context.add_cookies(cookies)
            page = context.new_page()
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            """)
            
            logger.info(f'Navigating to: {url}')
            page.goto(url, wait_until='domcontentloaded', timeout=45000)
            time.sleep(6)
            
            final_url = page.url
            result['debug']['final_url'] = final_url
            result['debug']['page_title'] = page.title()
            
            if 'login' in final_url.lower():
                result['error'] = 'Redirected to login'
                browser.close()
                return result
            
            html = page.content()
            result['debug']['html_length'] = len(html)
            
            if not target_post_id:
                target_post_id = extract_post_id_from_url(final_url)
            
            metadata = extract_metadata_from_html(html, target_post_id)
            
            result['data']['caption'] = metadata['caption']
            result['data']['thumbnail'] = metadata['thumbnail']
            result['data']['username'] = metadata['username']
            result['data']['post_id'] = metadata['post_id']
            result['data']['video_url'] = final_url
            
            if metadata['caption'] or metadata['thumbnail'] or metadata['post_id']:
                result['success'] = True
            
            context.close()
            browser.close()
    except Exception as e:
        logger.exception('Scrape failed')
        result['error'] = str(e)
    
    return result


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'ok',
        'service': 'Rise City Facebook Scraper \U0001f3a9',
        'version': '9.0-metadata-only',
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
        'version': '9.0-metadata-only',
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
