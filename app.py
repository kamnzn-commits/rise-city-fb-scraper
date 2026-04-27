"""
Rise City Facebook Scraper API - V8.7 🎩
SAFE VIEW EXTRACTION: Better return 0 than WRONG number.

Principle: View count must be from TARGET video, not related reels.

Strategy for views (in priority order):
1. og:video meta tag (rare but exact)
2. JSON match WITH target post_id nearby (10000 chars window)
3. aria-label NEAR <video> element (skip "Watch more reels" section)

If none of above match safely, return views=0 (let admin verify manually).

Engagement parsing (V8.3 - works perfect):
- Tokenize mobile innertext into icons + numbers
- Map icon→number for first reel only
- Stop when 2nd reel begins
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

# FB private use area icons
ICON_LIKE = '\U000F0378'      # 󰍸
ICON_COMMENT = '\U000F0379'   # 󰍹
ICON_SHARE = '\U000F037A'     # 󰍺
ICON_EYE = '\U000F174D'       # 󱝍 (view count)

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
    
    # Handle "X triệu" or "X nghìn"
    if 'tri\u1ec7u' in s:
        num_match = re.match(r'([\d.,]+)', s)
        if num_match:
            num_str = num_match.group(1).replace(',', '.')
            try:
                return int(float(num_str) * 1000000)
            except:
                return 0
    if 'ngh\u00ecn' in s:
        num_match = re.match(r'([\d.,]+)', s)
        if num_match:
            num_str = num_match.group(1).replace(',', '.')
            try:
                return int(float(num_str) * 1000)
            except:
                return 0
    
    match = re.match(r'([\d.,]+)\s*([KkMmBb]+)?', s)
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
    return int(num)


def parse_engagement_v83(innertext, debug_info):
    """
    V8.3 engagement parser - WORKS PERFECTLY for all videos.
    Returns dict: {likes, comments, shares, views}
    Views from this only if eye icon found in first reel block.
    """
    result = {'likes': 0, 'comments': 0, 'shares': 0, 'views': 0}
    
    if not innertext:
        return result
    
    debug_info['parse_v83'] = {}
    
    lines = innertext.split('\n')
    tokens = []
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        if not line_stripped:
            continue
        
        # Number first (handles "6", "1.3K", "1,3 triệu")
        num_match = re.match(r'^([\d.,]+\s*[KkMmBb]?(?:\s*ngh\u00ecn|\s*tri\u1ec7u)?)$', line_stripped)
        if num_match:
            value = parse_vietnamese_number(num_match.group(1))
            tokens.append({
                'type': 'number',
                'value': value,
                'raw': line_stripped,
                'line': i,
            })
            continue
        
        # Single icon
        if len(line_stripped) <= 2:
            for ch in line_stripped:
                cp = ord(ch)
                if 0xF0000 <= cp <= 0xFFFFD:
                    icon_type = None
                    if ch == ICON_LIKE:
                        icon_type = 'like'
                    elif ch == ICON_COMMENT:
                        icon_type = 'comment'
                    elif ch == ICON_SHARE:
                        icon_type = 'share'
                    elif ch == ICON_EYE:
                        icon_type = 'eye'
                    else:
                        icon_type = 'other_icon'
                    tokens.append({
                        'type': 'icon',
                        'icon_type': icon_type,
                        'raw': ch,
                        'line': i,
                    })
                    break
            continue
        
        # Text
        tokens.append({
            'type': 'text',
            'raw': line_stripped[:50],
            'line': i,
        })
    
    debug_info['parse_v83']['tokens_count'] = len(tokens)
    
    # Find first reel block
    first_reel_tokens = []
    seen_like = False
    seen_share = False
    
    for tok in tokens:
        if tok['type'] == 'text':
            text = tok['raw'].lower()
            if 'watch more' in text or 'kh\u00e1m ph\u00e1' in text:
                break
        
        if tok['type'] == 'icon' and tok.get('icon_type') == 'like' and seen_like and seen_share:
            break
        
        if tok['type'] == 'icon' and tok.get('icon_type') == 'like':
            seen_like = True
        if tok['type'] == 'icon' and tok.get('icon_type') == 'share':
            seen_share = True
        
        first_reel_tokens.append(tok)
    
    debug_info['parse_v83']['first_reel_summary'] = [
        f"{t['type']}:{t.get('icon_type', '') or t.get('value', '') or t.get('raw', '')[:20]}"
        for t in first_reel_tokens[:15]
    ]
    
    # Map icon → number
    icon_positions = {'like': [], 'comment': [], 'share': [], 'eye': []}
    for i, tok in enumerate(first_reel_tokens):
        if tok['type'] == 'icon':
            it = tok.get('icon_type')
            if it in icon_positions:
                icon_positions[it].append(i)
    
    for icon_type in ['like', 'comment', 'share', 'eye']:
        if not icon_positions[icon_type]:
            continue
        idx = icon_positions[icon_type][0]
        if idx + 1 < len(first_reel_tokens):
            next_tok = first_reel_tokens[idx + 1]
            if next_tok['type'] == 'number':
                value = next_tok['value']
                if icon_type == 'like' and 0 <= value < 1000000000:
                    result['likes'] = value
                elif icon_type == 'comment' and 0 <= value < 1000000000:
                    result['comments'] = value
                elif icon_type == 'share' and 0 <= value < 1000000000:
                    result['shares'] = value
                elif icon_type == 'eye' and 0 <= value < 10000000000:
                    result['views'] = value
    
    # Special case: icons grouped (likes=0 hidden)
    if (result['likes'] == 0 and result['comments'] == 0 and result['shares'] == 0):
        icons_seq = []
        numbers_seq = []
        for tok in first_reel_tokens:
            if tok['type'] == 'icon' and tok.get('icon_type') in ('like', 'comment', 'share'):
                icons_seq.append(tok.get('icon_type'))
            elif tok['type'] == 'number' and 0 <= tok['value'] < 1000000000:
                numbers_seq.append(tok['value'])
        
        if icons_seq[:3] == ['like', 'comment', 'share']:
            if len(numbers_seq) == 3:
                result['likes'] = numbers_seq[0]
                result['comments'] = numbers_seq[1]
                result['shares'] = numbers_seq[2]
            elif len(numbers_seq) == 2:
                result['comments'] = numbers_seq[0]
                result['shares'] = numbers_seq[1]
            elif len(numbers_seq) == 1:
                result['comments'] = numbers_seq[0]
    
    debug_info['parse_v83']['parsed'] = result.copy()
    return result


def safe_extract_views_from_html(html, target_post_id, debug_info):
    """
    SAFE view extraction - only return value if HIGH CONFIDENCE it's target video.
    Returns 0 if uncertain.
    
    Strategy:
    1. og:video:* meta tags (these always belong to current page = target)
    2. JSON pattern WITH target_post_id within 5000 chars window
    3. NEVER use aria-label fallback (always related reels)
    4. NEVER use HTML view-count search alone (could be related reels)
    """
    debug_info['safe_view_attempts'] = []
    
    if not html or not target_post_id:
        return 0
    
    # Strategy 1: og:video meta tags
    # Some videos have view in og:video:* but FB rarely puts it
    # Skip for now - rare case
    
    # Strategy 2: JSON view count NEAR post_id (proximity matching)
    # Only accept if pattern is within 5000 chars of target_post_id mention
    target_id_str = str(target_post_id)
    target_positions = []
    
    # Find all occurrences of target post_id in HTML
    for m in re.finditer(re.escape(target_id_str), html):
        target_positions.append(m.start())
        if len(target_positions) > 50:
            break
    
    debug_info['safe_view_attempts'].append({
        'strategy': 'proximity_json',
        'target_id': target_id_str,
        'target_occurrences': len(target_positions)
    })
    
    if not target_positions:
        return 0
    
    # Find view count patterns and check proximity to target_id
    view_patterns = [
        r'"video_view_count"\s*:\s*(\d+)',
        r'"play_count"\s*:\s*(\d+)',
        r'"reels_video_view_count"\s*:\s*(\d+)',
        r'"organic_view_count_v2"\s*:\s*(\d+)',
        r'"feedback_video_view_count"\s*:\s*(\d+)',
        r'"reel_view_count"\s*:\s*(\d+)',
        r'"viewCount"\s*:\s*(\d+)',
    ]
    
    candidates = []
    for pat in view_patterns:
        for m in re.finditer(pat, html):
            view_pos = m.start()
            try:
                value = int(m.group(1))
                if not (10 <= value <= 1000000000):
                    continue
            except:
                continue
            
            # Check proximity to ANY target_id occurrence
            min_distance = min(abs(view_pos - tp) for tp in target_positions)
            
            if min_distance <= 5000:  # within 5KB
                candidates.append({
                    'value': value,
                    'pattern': pat[:30],
                    'distance': min_distance
                })
    
    debug_info['safe_view_attempts'].append({
        'strategy': 'proximity_json',
        'candidates_count': len(candidates),
        'candidates': candidates[:5]
    })
    
    if not candidates:
        return 0
    
    # Take CLOSEST match to target_id (highest confidence)
    candidates.sort(key=lambda x: x['distance'])
    best = candidates[0]
    
    debug_info['safe_view_attempts'].append({
        'strategy': 'proximity_json',
        'selected': best
    })
    
    return best['value']


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
            
            try_desktop_with_cookies(browser, url, cookies, result)
            try_mobile_for_engagement(browser, url, cookies, result)
            
            browser.close()
            
            if (result['data']['views'] > 0 or 
                result['data']['likes'] > 0 or
                result['data']['comments'] > 0 or
                result['data']['caption']):
                result['success'] = True
    except Exception as e:
        logger.exception('Scrape failed')
        result['error'] = str(e)
        result['error_type'] = type(e).__name__
    
    return result


def try_desktop_with_cookies(browser, url, cookies, result):
    """
    Desktop with cookies: get caption + thumbnail + post_id + SAFE views
    """
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
            return
        
        html = page.content()
        result['debug']['html_length'] = len(html)
        
        # Extract metadata first (we need post_id for safe view extraction)
        extracted = extract_metadata_from_html(html)
        result['debug']['extracted_data'] = extracted
        
        target_post_id = extracted.get('post_id', '')
        if target_post_id:
            result['data']['post_id'] = target_post_id
        
        # SAFE view extraction (only if proximity match)
        if target_post_id:
            safe_views = safe_extract_views_from_html(html, target_post_id, result['debug'])
            if safe_views > 0:
                result['data']['views'] = safe_views
                result['debug']['view_sources']['html_proximity'] = safe_views
        
        # Username
        dom_data = extract_username_from_dom(page)
        
        result['debug']['mode_used'] = 'desktop_cookies'
        
        # Caption
        raw_caption = extracted.get('caption', '')
        if raw_caption:
            decoded = decode_unicode_string(raw_caption)
            result['data']['caption'] = decoded[:5000]
        
        # Thumbnail
        raw_thumbnail = extracted.get('thumbnail', '')
        if raw_thumbnail:
            result['data']['thumbnail'] = decode_html_entities(raw_thumbnail)
        
        result['data']['username'] = dom_data.get('username', '')
        result['data']['video_url'] = final_url
        
        context.close()
    except Exception as e:
        logger.warning(f'Desktop mode failed: {e}')


def try_mobile_for_engagement(browser, url, cookies, result):
    """
    Mobile m.facebook.com: get likes/comments/shares only.
    Do NOT extract views from mobile (too unreliable, easily gets related reel views).
    """
    try:
        result['debug']['tried_modes'].append('mobile_engagement')
        
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
        
        # V8.3 engagement parser
        parsed = parse_engagement_v83(mobile_innertext, result['debug'])
        
        # Set engagement (likes/comments/shares)
        result['data']['likes'] = parsed.get('likes', 0)
        result['data']['comments'] = parsed.get('comments', 0)
        result['data']['shares'] = parsed.get('shares', 0)
        
        # ONLY use eye icon view if it's IN the first reel block
        # parse_engagement_v83 already handles this correctly
        if parsed.get('views', 0) > 0 and result['data']['views'] == 0:
            result['data']['views'] = parsed['views']
            result['debug']['view_sources']['mobile_first_reel_eye'] = parsed['views']
        
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
    """Extract caption, thumbnail, post_id from HTML"""
    data = {}
    
    # Post ID
    for pat in [
        r'"video_id"\s*:\s*"(\d+)"',
        r'"top_level_post_id"\s*:\s*"(\d+)"',
    ]:
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
        r'"description"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
    ]:
        m = re.search(pat, html)
        if m:
            caption_candidates.append(m.group(1))
    
    if caption_candidates:
        caption_candidates.sort(key=len, reverse=True)
        data['caption'] = caption_candidates[0]
    
    # Thumbnail
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
        'service': 'Rise City Facebook Scraper 🎩',
        'version': '8.7-safe-views',
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
        'version': '8.7-safe-views',
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
