"""
Rise City Facebook Scraper API - V8.0 🎩
DUAL MODE STRATEGY:

Mode A (Desktop with cookies): Lấy engagement metadata
  - likes, comments, shares (mobile innertext - V7.8 logic)
  - caption full, thumbnail, post_id

Mode B (Desktop WITHOUT cookies - anonymous): Lấy views
  - FB hiển thị view cho user ẩn danh
  - Cookies của account phụ đôi khi làm FB ẩn view

Mode C (Mobile m.facebook.com): Backup engagement
  
Lấy MAX views, MAX likes/comments/shares từ tất cả modes.
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


def parse_mobile_engagement(innertext, debug_info):
    """Parse engagement from mobile innertext (V7.8 - works)"""
    data = {'likes': 0, 'comments': 0, 'shares': 0}
    
    if not innertext:
        return data
    
    icon_number_pattern = r'[\U000F0000-\U000FFFFD]\s*\n\s*([\d.,]+\s*[KkMmBb]?)\s*\n'
    matches = re.findall(icon_number_pattern, innertext)
    
    debug_info['icon_pattern_matches'] = matches[:10]
    
    if len(matches) >= 3:
        data['likes'] = parse_vietnamese_number(matches[0])
        data['comments'] = parse_vietnamese_number(matches[1])
        data['shares'] = parse_vietnamese_number(matches[2])
    elif len(matches) == 2:
        data['likes'] = parse_vietnamese_number(matches[0])
        data['comments'] = parse_vietnamese_number(matches[1])
    elif len(matches) == 1:
        data['likes'] = parse_vietnamese_number(matches[0])
    
    return data


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
    except Exception as e:
        logger.warning(f'Human sim failed: {e}')


def search_views_in_text(text):
    """Search Vietnamese view patterns in any text"""
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
    
    # Also try JSON patterns
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


def scrape_anonymous_for_views(browser, url, debug_info):
    """
    Mode B: Open URL WITHOUT cookies to get views.
    FB shows view counter to anonymous users for some videos.
    """
    try:
        debug_info['anonymous_attempted'] = True
        
        # Anonymous context - NO cookies, fresh browser fingerprint
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
        
        # NO cookies added - anonymous mode
        page = context.new_page()
        
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en'] });
        """)
        
        logger.info(f'Anonymous mode navigating to: {url}')
        
        # Capture network for view counter (some FB endpoints expose it)
        captured_anon = []
        def handle_response(response):
            try:
                if response.status == 200:
                    url_lower = response.url.lower()
                    if 'graphql' in url_lower or '/video' in url_lower or 'reel' in url_lower:
                        try:
                            body = response.text()
                            if body and len(body) < 3000000:
                                captured_anon.append(body)
                        except:
                            pass
            except:
                pass
        page.on('response', handle_response)
        
        response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
        time.sleep(7)
        
        # Try to dismiss login popup if appears
        try:
            page.evaluate("""
                () => {
                    // Close any dialog/popup
                    const closeButtons = document.querySelectorAll('[aria-label*="Close"], [aria-label*="Đóng"]');
                    closeButtons.forEach(btn => btn.click());
                }
            """)
            time.sleep(1)
        except:
            pass
        
        # Scroll
        try:
            page.evaluate('window.scrollTo(0, 400)')
            time.sleep(2)
        except:
            pass
        
        # Get HTML and innertext
        html = page.content()
        innertext = ''
        try:
            innertext = page.evaluate('document.body.innerText || ""')
        except:
            pass
        
        debug_info['anonymous_html_length'] = len(html)
        debug_info['anonymous_innertext_length'] = len(innertext)
        debug_info['anonymous_innertext_sample'] = innertext[:1500]
        debug_info['anonymous_network_count'] = len(captured_anon)
        debug_info['anonymous_html_keywords'] = {
            'has_luot_xem': 'l\u01b0\u1ee3t xem' in html,
            'has_video_view_count': 'video_view_count' in html,
            'has_play_count': 'play_count' in html,
        }
        
        # Search views in HTML, innertext, and captured network responses
        all_text_sources = [html, innertext] + captured_anon
        combined_text = '\n'.join(all_text_sources)
        
        views = search_views_in_text(combined_text)
        debug_info['anonymous_views_found'] = views
        
        context.close()
        return views
    except Exception as e:
        logger.warning(f'Anonymous mode failed: {e}')
        debug_info['anonymous_error'] = str(e)[:200]
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
            'html_search_results': {},
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
                    '--window-size=1366,768',
                ]
            )
            
            # === MODE A: Desktop with cookies (engagement + metadata) ===
            views_cookies = try_desktop_mode(browser, url, cookies, result)
            result['debug']['view_sources']['cookies'] = views_cookies
            
            # === MODE B: ANONYMOUS (NEW - to get hidden views) ===
            anonymous_views = scrape_anonymous_for_views(browser, url, result['debug'])
            result['debug']['view_sources']['anonymous'] = anonymous_views
            
            # === MODE C: Mobile m.facebook.com (engagement backup) ===
            try_mobile_mode(browser, url, cookies, result)
            
            browser.close()
            
            # COMBINE: Take MAX of all view sources
            final_views = max(views_cookies, anonymous_views)
            if final_views > 0:
                result['data']['views'] = final_views
            
            if (result['data']['views'] > 0 or 
                result['data']['likes'] > 0 or
                result['data']['caption']):
                result['success'] = True
    except Exception as e:
        logger.exception('Scrape failed')
        result['error'] = str(e)
        result['error_type'] = type(e).__name__
    
    return result


def try_desktop_mode(browser, url, cookies, result):
    """Mode A: Desktop with cookies - engagement + metadata. Returns views (may be 0)."""
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
        
        logger.info(f'Desktop+cookies navigating to: {url}')
        response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
        time.sleep(7)
        simulate_human(page)
        time.sleep(2)
        
        final_url = page.url
        page_title = page.title()
        result['debug']['final_url'] = final_url
        result['debug']['page_title'] = page_title
        
        if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
            result['error'] = 'Redirected to login - cookies invalid'
            context.close()
            return 0
        
        html = page.content()
        result['debug']['html_length'] = len(html)
        
        # Try to find views
        views = search_views_in_text(html)
        
        # Metadata
        extracted = extract_metadata_from_html(html)
        result['debug']['extracted_data'] = extracted
        
        # Username
        dom_data = extract_username_from_dom(page)
        
        result['debug']['html_search_results'] = {
            'has_luot_xem': 'l\u01b0\u1ee3t xem' in html,
            'has_video_view_count': 'video_view_count' in html,
            'has_play_count': 'play_count' in html,
        }
        
        result['debug']['mode_used'] = 'desktop_cookies'
        
        # Caption full
        raw_caption = extracted.get('caption', '')
        if raw_caption:
            decoded = decode_unicode_string(raw_caption)
            result['data']['caption'] = decoded[:5000]
        
        # Thumbnail decode
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


def try_mobile_mode(browser, url, cookies, result):
    """Mode C: Mobile m.facebook.com for engagement"""
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
        
        logger.info(f'Mobile+cookies navigating to: {mobile_url}')
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
        
        engagement = parse_mobile_engagement(mobile_innertext, result['debug'])
        
        if engagement.get('likes', 0) > 0:
            result['data']['likes'] = engagement['likes']
        if engagement.get('comments', 0) > 0:
            result['data']['comments'] = engagement['comments']
        if engagement.get('shares', 0) > 0:
            result['data']['shares'] = engagement['shares']
        
        # Mobile innertext might also have views
        if mobile_innertext:
            mobile_views = search_views_in_text(mobile_innertext)
            if mobile_views > 0:
                result['debug']['view_sources']['mobile'] = mobile_views
        
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
    
    # Post ID
    for pat in [
        r'"video_id"\s*:\s*"(\d+)"',
        r'"top_level_post_id"\s*:\s*"(\d+)"',
    ]:
        m = re.search(pat, html)
        if m:
            data['post_id'] = m.group(1)
            break
    
    # Caption - take longest candidate
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
        'version': '8.0-dual-mode',
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
        'version': '8.0-dual-mode',
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
