"""
Rise City Facebook Scraper API - V8.5 🎩
CAPTURE NETWORK GRAPHQL RESPONSES

ROOT CAUSE (confirmed by debug-html):
- HTML static KHÔNG có reaction_count, share_count, video_view_count
- FB load engagement data via ASYNC GraphQL streaming (RelayPrefetchedStreamCache)
- HTML chỉ có: total_comment_count, metadata (caption, thumbnail, post_id)
- Likes, shares, views được gửi qua NETWORK RESPONSES sau page render

V8.5 STRATEGY:
1. page.on('response') → capture ALL GraphQL network responses
2. HTML parse → metadata (caption, thumbnail, post_id)
3. total_comment_count từ HTML (có sẵn)
4. Network responses → search reaction_count, share_count, video_view_count
5. Proximity match: chỉ accept data WITHIN 5000 chars of target post_id
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


def parse_number(val):
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip()
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


def extract_post_id_from_url(url):
    for pat in [r'/reel/(\d+)', r'/videos/(\d+)', r'video\.php\?v=(\d+)', r'/watch/\?v=(\d+)']:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return None


# ==========================================
# METADATA FROM HTML (always works)
# ==========================================

def extract_metadata_from_html(html):
    data = {'caption': '', 'thumbnail': '', 'post_id': None, 'username': ''}
    
    # Post ID
    for pat in [r'"video_id"\s*:\s*"(\d+)"', r'"post_id"\s*:\s*"(\d+)"', r'"top_level_post_id"\s*:\s*"(\d+)"']:
        m = re.search(pat, html)
        if m:
            data['post_id'] = m.group(1)
            break
    
    # Caption
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
        data['caption'] = caption_candidates[0]
    
    # Thumbnail
    m = re.search(r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
    if m:
        data['thumbnail'] = decode_html_entities(m.group(1))
    
    # Username
    for pat in [
        r'"name"\s*:\s*"([^"]+)"\s*,\s*"(?:url|profile_url)"',
        r'"owner"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"',
    ]:
        m = re.search(pat, html)
        if m:
            name = m.group(1)
            if len(name) > 1 and len(name) < 100 and name.lower() not in USERNAME_BLACKLIST:
                data['username'] = name
                break
    
    return data


# ==========================================
# ENGAGEMENT FROM HTML (total_comment_count only)
# ==========================================

def extract_comments_from_html(html, target_post_id):
    """HTML có total_comment_count - proximity match với post_id"""
    if not target_post_id:
        return 0
    
    pattern = r'"total_comment_count"\s*:\s*(\d+)'
    matches = list(re.finditer(pattern, html))
    
    if not matches:
        return 0
    
    # Find closest match to target post_id
    post_positions = [m.start() for m in re.finditer(re.escape(target_post_id), html)]
    if not post_positions:
        # Fallback: return first match
        return int(matches[0].group(1))
    
    best_val = 0
    best_dist = float('inf')
    for m in matches:
        val = int(m.group(1))
        for pp in post_positions:
            dist = abs(m.start() - pp)
            if dist < best_dist:
                best_dist = dist
                best_val = val
    
    return best_val


# ==========================================
# ENGAGEMENT FROM NETWORK RESPONSES (likes, shares, views)
# ==========================================

def extract_engagement_from_network(captured_bodies, target_post_id, debug_info):
    """
    V8.5 CORE: Search captured GraphQL network responses for engagement data.
    FB sends engagement via streaming JSON responses.
    """
    result = {'likes': 0, 'shares': 0, 'views': 0, 'comments_net': 0}
    
    debug_info['v85_network_bodies'] = len(captured_bodies)
    debug_info['v85_network_total_size'] = sum(len(b) for b in captured_bodies)
    
    likes_candidates = []
    shares_candidates = []
    views_candidates = []
    comments_candidates = []
    
    for body in captured_bodies:
        # Skip small or non-JSON bodies
        if len(body) < 50:
            continue
        
        # Check if this body contains target post_id
        has_target = target_post_id and target_post_id in body
        
        # LIKES patterns
        for pat in [
            r'"reaction_count"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
            r'"reactors"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
            r'"i18n_reaction_count"\s*:\s*"(\d+)',
            r'"reaction_count"\s*:\s*(\d+)',
        ]:
            for m in re.finditer(pat, body):
                val = int(m.group(1))
                if has_target:
                    # Proximity check within this body
                    pid_pos = body.find(target_post_id)
                    dist = abs(m.start() - pid_pos) if pid_pos >= 0 else 99999
                    likes_candidates.append({'value': val, 'distance': dist, 'in_target_body': True})
                else:
                    likes_candidates.append({'value': val, 'distance': 99999, 'in_target_body': False})
        
        # SHARES patterns
        for pat in [
            r'"share_count"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
            r'"reshare_count"\s*:\s*(\d+)',
        ]:
            for m in re.finditer(pat, body):
                val = int(m.group(1))
                if has_target:
                    pid_pos = body.find(target_post_id)
                    dist = abs(m.start() - pid_pos) if pid_pos >= 0 else 99999
                    shares_candidates.append({'value': val, 'distance': dist, 'in_target_body': True})
                else:
                    shares_candidates.append({'value': val, 'distance': 99999, 'in_target_body': False})
        
        # VIEWS patterns
        for pat in [
            r'"video_view_count"\s*:\s*(\d+)',
            r'"play_count"\s*:\s*(\d+)',
            r'"viewCount"\s*:\s*(\d+)',
            r'"reels_view_count"\s*:\s*(\d+)',
        ]:
            for m in re.finditer(pat, body):
                val = int(m.group(1))
                if 10 <= val <= 1000000000:
                    if has_target:
                        pid_pos = body.find(target_post_id)
                        dist = abs(m.start() - pid_pos) if pid_pos >= 0 else 99999
                        views_candidates.append({'value': val, 'distance': dist, 'in_target_body': True})
                    else:
                        views_candidates.append({'value': val, 'distance': 99999, 'in_target_body': False})
        
        # COMMENTS from network too
        for pat in [r'"total_comment_count"\s*:\s*(\d+)']:
            for m in re.finditer(pat, body):
                val = int(m.group(1))
                if has_target:
                    pid_pos = body.find(target_post_id)
                    dist = abs(m.start() - pid_pos) if pid_pos >= 0 else 99999
                    comments_candidates.append({'value': val, 'distance': dist, 'in_target_body': True})
    
    # Pick best candidates: prefer in_target_body=True, then closest distance
    def pick_best(candidates):
        if not candidates:
            return 0
        # First try candidates in target body
        target_cands = [c for c in candidates if c['in_target_body']]
        if target_cands:
            # Pick closest to post_id
            best = min(target_cands, key=lambda c: c['distance'])
            if best['distance'] < 5000:
                return best['value']
        return 0
    
    result['likes'] = pick_best(likes_candidates)
    result['shares'] = pick_best(shares_candidates)
    result['views'] = pick_best(views_candidates)
    result['comments_net'] = pick_best(comments_candidates)
    
    debug_info['v85_likes_candidates'] = len(likes_candidates)
    debug_info['v85_shares_candidates'] = len(shares_candidates)
    debug_info['v85_views_candidates'] = len(views_candidates)
    debug_info['v85_comments_candidates'] = len(comments_candidates)
    
    # Log first few for debugging
    debug_info['v85_likes_samples'] = [{'val': c['value'], 'dist': c['distance'], 'target': c['in_target_body']} for c in likes_candidates[:5]]
    debug_info['v85_views_samples'] = [{'val': c['value'], 'dist': c['distance'], 'target': c['in_target_body']} for c in views_candidates[:5]]
    
    return result


# ==========================================
# SCRAPE ORCHESTRATOR
# ==========================================

def scrape_with_playwright(url):
    cookies = parse_netscape_cookies(COOKIES_PATH)
    if not cookies:
        return {'success': False, 'error': 'No cookies loaded'}
    
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
            'version': '8.5-network-capture',
            'target_post_id_from_url': target_post_id,
        }
    }
    
    # Capture network responses
    captured_bodies = []
    
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
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                }
            )
            context.add_cookies(cookies)
            page = context.new_page()
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en'] });
            """)
            
            # CAPTURE ALL NETWORK RESPONSES
            def handle_response(resp):
                try:
                    if resp.status == 200:
                        url_lower = resp.url.lower()
                        # Capture GraphQL, API, and any JSON responses
                        if any(k in url_lower for k in [
                            'graphql', '/api/', 'ajax', 'relay', 
                            'stream', 'comet', 'reels'
                        ]):
                            try:
                                body = resp.text()
                                if body and len(body) < 5000000:
                                    captured_bodies.append(body)
                            except:
                                pass
                except:
                    pass
            
            page.on('response', handle_response)
            
            logger.info(f'Navigating to: {url}')
            response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
            
            # Wait for async streams to arrive
            time.sleep(10)
            
            # Scroll to trigger more data loading
            try:
                page.evaluate('window.scrollTo({top: 300, behavior: "smooth"})')
                time.sleep(3)
                page.evaluate('window.scrollTo({top: 0, behavior: "smooth"})')
                time.sleep(2)
            except:
                pass
            
            final_url = page.url
            page_title = page.title()
            result['debug']['final_url'] = final_url
            result['debug']['page_title'] = page_title
            
            if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
                result['error'] = 'Redirected to login - cookies invalid'
                browser.close()
                return result
            
            # Get HTML for metadata
            html = page.content()
            result['debug']['html_length'] = len(html)
            
            # Resolve post_id
            if not target_post_id:
                target_post_id = extract_post_id_from_url(final_url)
            if not target_post_id:
                for pat in [r'"video_id"\s*:\s*"(\d+)"', r'"post_id"\s*:\s*"(\d+)"']:
                    m = re.search(pat, html)
                    if m:
                        target_post_id = m.group(1)
                        break
            
            result['debug']['target_post_id'] = target_post_id
            
            # === METADATA FROM HTML ===
            metadata = extract_metadata_from_html(html)
            result['data']['post_id'] = metadata['post_id'] or target_post_id
            result['data']['video_url'] = final_url
            
            raw_caption = metadata.get('caption', '')
            if raw_caption:
                result['data']['caption'] = decode_unicode_string(raw_caption)[:5000]
            result['data']['thumbnail'] = metadata.get('thumbnail', '')
            result['data']['username'] = metadata.get('username', '')
            
            # === COMMENTS FROM HTML (reliable) ===
            html_comments = extract_comments_from_html(html, target_post_id)
            
            # === ENGAGEMENT FROM NETWORK (likes, shares, views) ===
            # Also search HTML as additional "body" (it contains some streamed data)
            all_bodies = captured_bodies + [html]
            net_engagement = extract_engagement_from_network(all_bodies, target_post_id, result['debug'])
            
            # === COMBINE ===
            result['data']['likes'] = net_engagement['likes']
            result['data']['comments'] = max(html_comments, net_engagement['comments_net'])
            result['data']['shares'] = net_engagement['shares']
            result['data']['views'] = net_engagement['views']
            
            result['debug']['html_comments'] = html_comments
            result['debug']['net_comments'] = net_engagement['comments_net']
            
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
        'version': '8.5-network-capture',
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
        'version': '8.5-network-capture',
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
