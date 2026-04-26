"""
Rise City Facebook Scraper API - V8.2 🎩
ROOT CAUSE FIX: FB render engagement icons differently based on count.

Discovery:
- Video with HIGH engagement (>10): "icon\n99\nicon\n50\nicon\n2"
- Video with LOW engagement (<10): "icon\nicon\n6\nicon\n11" (icons grouped first, numbers after)
- Some videos: Numbers before icons
- Live indicator (eye icon 󱝍) shows view count separately

Solution: Multiple parsing strategies, smart fallback, take first valid match.

Strategies:
1. Strategy ICON_NUMBER: icon → number (V7.8 logic, works for normal videos)
2. Strategy ICON_GROUPED: icons grouped, numbers grouped (NEW - low engagement)
3. Strategy NUMBER_ICON: number → icon (NEW - reversed order)
4. Strategy EYE_ICON_VIEW: eye icon 󱝍 → view count (NEW for view extraction)
5. Strategy FALLBACK: Find any 3 numbers in mobile innertext that look like engagement
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


def parse_mobile_engagement_v82(innertext, debug_info):
    """
    V8.2: Multiple parsing strategies for FB rendering variations.
    Returns engagement dict + view dict.
    """
    result = {'likes': 0, 'comments': 0, 'shares': 0, 'views': 0}
    
    if not innertext:
        return result
    
    debug_info['parse_strategies_tried'] = []
    
    # Get only the FIRST video block (not the entire reels feed)
    # Look for first sequence of icon→...→author pattern
    # The first reel should be at the start before "Watch more reels"
    first_block = innertext
    if 'Watch more reels' in innertext:
        first_block = innertext.split('Watch more reels')[0]
    elif 'Hãy đăng nhập' in innertext:
        first_block = innertext.split('Hãy đăng nhập')[0]
    
    debug_info['first_block_length'] = len(first_block)
    debug_info['first_block_sample'] = first_block[:600]
    
    # ============================================
    # STRATEGY EYE_ICON: Find 󱝍 (eye) → view count
    # The eye icon often shows view count for reels
    # ============================================
    eye_icon = '\U000F174D'  # 󱝍 unicode
    eye_pattern = rf'{re.escape(eye_icon)}\s*\n?\s*([\d.,]+\s*[KkMmBb]?(?:\s*tri\u1ec7u)?)'
    eye_matches = re.findall(eye_pattern, first_block)
    if eye_matches:
        for match in eye_matches[:5]:
            view_value = parse_vietnamese_number(match)
            # Eye icon usually for VIEWS not engagement, so accept reasonable view counts
            if 100 <= view_value <= 1000000000:
                if view_value > result['views']:
                    result['views'] = view_value
                    debug_info['parse_strategies_tried'].append(f'eye_icon_view:{view_value}')
    
    # ============================================
    # STRATEGY ICON_NUMBER (V7.8): icon → number directly
    # Works for: "󰍸\n99\n󰍹\n50\n󰍺\n2"
    # ============================================
    pattern_v1 = r'[\U000F0000-\U000FFFFD]\s*\n\s*([\d.,]+\s*[KkMmBb]?)\s*\n'
    matches_v1 = re.findall(pattern_v1, first_block)
    debug_info['strategy_v1_matches'] = matches_v1[:5]
    
    if len(matches_v1) >= 3:
        # First 3 matches are usually likes, comments, shares
        result['likes'] = parse_vietnamese_number(matches_v1[0])
        result['comments'] = parse_vietnamese_number(matches_v1[1])
        result['shares'] = parse_vietnamese_number(matches_v1[2])
        debug_info['parse_strategies_tried'].append(f'v1_icon_number:L{result["likes"]}/C{result["comments"]}/S{result["shares"]}')
        return result
    
    # ============================================
    # STRATEGY ICON_GROUPED: icons grouped, numbers separate
    # Works for: "󰍸\n󰍹\n6\n󰍺\n11" (some icons appear without numbers)
    # ============================================
    # Find sequence: icon, optional newline, icon, optional newline, NUMBER, icon, NUMBER
    # Or any combination where icons are grouped before numbers
    
    # Split into lines and analyze
    lines = first_block.split('\n')
    debug_info['total_lines'] = len(lines)
    
    # Find first 5 numbers in order they appear
    numbers_found = []
    for i, line in enumerate(lines):
        line_clean = line.strip()
        if not line_clean:
            continue
        # Check if line is a number (with optional K/M suffix)
        num_match = re.match(r'^([\d.,]+\s*[KkMmBb]?(?:\s*tri\u1ec7u)?)$', line_clean)
        if num_match:
            value = parse_vietnamese_number(num_match.group(1))
            if 0 <= value <= 1000000000:
                numbers_found.append({
                    'value': value,
                    'line_idx': i,
                    'raw': line_clean
                })
                if len(numbers_found) >= 10:
                    break
    
    debug_info['numbers_found'] = [n['raw'] for n in numbers_found[:10]]
    
    # Strategy v2: Take first 3 numbers as engagement (most common case for low-engagement)
    # Filter out numbers that are too large (likely view counts, not engagement)
    engagement_candidates = [n for n in numbers_found if n['value'] < 1000000]
    
    if len(engagement_candidates) >= 3 and result['likes'] == 0:
        result['likes'] = engagement_candidates[0]['value']
        result['comments'] = engagement_candidates[1]['value']
        result['shares'] = engagement_candidates[2]['value']
        debug_info['parse_strategies_tried'].append(f'v2_grouped:L{result["likes"]}/C{result["comments"]}/S{result["shares"]}')
    elif len(engagement_candidates) == 2 and result['likes'] == 0:
        result['likes'] = engagement_candidates[0]['value']
        result['comments'] = engagement_candidates[1]['value']
        debug_info['parse_strategies_tried'].append(f'v2_grouped_2nums')
    elif len(engagement_candidates) == 1 and result['likes'] == 0:
        result['likes'] = engagement_candidates[0]['value']
        debug_info['parse_strategies_tried'].append(f'v2_grouped_1num')
    
    # ============================================
    # STRATEGY VIEW_FROM_LARGE_NUMBER:
    # If found a number with K/M suffix (like 1.3K, 17K), it's likely VIEWS
    # ============================================
    if result['views'] == 0:
        for n in numbers_found:
            # Numbers like 1.3K, 17K, 1.5M are usually views
            if 'K' in n['raw'].upper() or 'M' in n['raw'].upper() or 'tri\u1ec7u' in n['raw']:
                if n['value'] >= 1000:  # at least 1K
                    if n['value'] > result['views']:
                        result['views'] = n['value']
                        debug_info['parse_strategies_tried'].append(f'large_number_view:{n["value"]}')
    
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


def search_views_in_text(text):
    """Search Vietnamese view patterns + JSON patterns"""
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
        r'"post_view_count"\s*:\s*(\d+)',
        r'"feedback_video_view_count"\s*:\s*(\d+)',
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
            'parse_v82_debug': {},
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
            
            views_desktop = try_desktop_with_cookies(browser, url, cookies, result)
            result['debug']['view_sources']['desktop_cookies'] = views_desktop
            
            try_mobile_mode_v82(browser, url, cookies, result)
            
            browser.close()
            
            if (result['data']['views'] > 0 or 
                result['data']['likes'] > 0 or
                result['data']['caption']):
                result['success'] = True
    except Exception as e:
        logger.exception('Scrape failed')
        result['error'] = str(e)
        result['error_type'] = type(e).__name__
    
    return result


def try_desktop_with_cookies(browser, url, cookies, result):
    """Desktop with cookies for engagement + metadata"""
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
        
        views = search_views_in_text(html)
        
        extracted = extract_metadata_from_html(html)
        result['debug']['extracted_data'] = extracted
        
        dom_data = extract_username_from_dom(page)
        
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
        
        context.close()
        return views
    except Exception as e:
        logger.warning(f'Desktop mode failed: {e}')
        return 0


def try_mobile_mode_v82(browser, url, cookies, result):
    """V8.2 Mobile mode with smart parsing"""
    try:
        result['debug']['tried_modes'].append('mobile_v82')
        
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
            result['debug']['mobile_innertext_sample'] = mobile_innertext[:1500]
        except:
            pass
        
        # V8.2: Use new flexible parser
        parsed = parse_mobile_engagement_v82(mobile_innertext, result['debug']['parse_v82_debug'])
        
        if parsed.get('likes', 0) > 0:
            result['data']['likes'] = parsed['likes']
        if parsed.get('comments', 0) > 0:
            result['data']['comments'] = parsed['comments']
        if parsed.get('shares', 0) > 0:
            result['data']['shares'] = parsed['shares']
        if parsed.get('views', 0) > 0:
            result['data']['views'] = parsed['views']
            result['debug']['view_sources']['mobile_v82'] = parsed['views']
        
        # Also search HTML for views (some videos have view in HTML)
        if result['data']['views'] == 0:
            mobile_html = page.content()
            html_views = search_views_in_text(mobile_html)
            if html_views > 0:
                result['data']['views'] = html_views
                result['debug']['view_sources']['mobile_html'] = html_views
        
        context.close()
    except Exception as e:
        logger.warning(f'Mobile v82 mode failed: {e}')


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
        'service': 'Rise City Facebook Scraper 🎩',
        'version': '8.2-flexible-parser',
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
        'version': '8.2-flexible-parser',
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
