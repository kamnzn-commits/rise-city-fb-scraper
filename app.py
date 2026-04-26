"""
Rise City Facebook Scraper API - V7.9 🎩
Fix triệt để vấn đề video không có view count:

1. Caption: tăng giới hạn 500 → 5000 ký tự, thêm fallback patterns
2. View extraction (3 strategy mới):
   - Strategy A: Search HTML "lượt xem" (V7.3 - cũ)  
   - Strategy B: Search mobile innertext (V7.7 - cũ)
   - Strategy C: Capture GraphQL responses chứa play_count
   - Strategy D: Search JSON nested cho video_view_count
   - Strategy E: Wait dài + scroll trigger force JS load
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

# Global cache for captured GraphQL responses
captured_responses = []


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
    """Decode HTML entities like &amp; → &"""
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
        for _ in range(3):
            x = random.randint(100, 1200)
            y = random.randint(100, 600)
            page.mouse.move(x, y)
            time.sleep(random.uniform(0.3, 0.8))
        for offset in [200, 400, 600, 300]:
            page.evaluate(f'window.scrollTo({{top: {offset}, behavior: "smooth"}})')
            time.sleep(random.uniform(0.8, 1.5))
    except Exception as e:
        logger.warning(f'Human sim failed: {e}')


def scrape_with_playwright(url):
    global captured_responses
    captured_responses = []
    
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
            'view_extraction_attempts': [],
            'view_strategies_tried': [],
            'mode_used': '',
            'tried_modes': [],
            'html_search_results': {},
            'graphql_views_found': [],
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
            
            try_desktop_mode(browser, url, cookies, result)
            try_mobile_mode(browser, url, cookies, result)
            
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


def try_desktop_mode(browser, url, cookies, result):
    """Desktop mode - get views (multi-strategy) + metadata"""
    global captured_responses
    
    try:
        result['debug']['tried_modes'].append('desktop')
        
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
        
        # Capture GraphQL responses (Strategy C)
        def handle_response(response):
            try:
                url_lower = response.url.lower()
                if 'graphql' in url_lower or '/api/graphql' in url_lower or 'video' in url_lower:
                    if response.status == 200:
                        try:
                            body = response.text()
                            if body and len(body) < 5000000:
                                captured_responses.append(body)
                        except:
                            pass
            except:
                pass
        
        page.on('response', handle_response)
        
        logger.info(f'Desktop navigating to: {url}')
        response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
        
        # Wait LONGER (Strategy E)
        time.sleep(10)
        simulate_human(page)
        time.sleep(3)
        
        # Try to play video to trigger view-related JS
        try:
            page.evaluate('''
                const video = document.querySelector('video');
                if (video) {
                    video.muted = true;
                    video.play().catch(() => {});
                }
            ''')
            time.sleep(3)
        except:
            pass
        
        final_url = page.url
        page_title = page.title()
        result['debug']['final_url'] = final_url
        result['debug']['page_title'] = page_title
        result['debug']['response_status'] = response.status if response else None
        result['debug']['network_captures'] = len(captured_responses)
        
        if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
            result['error'] = 'Redirected to login - cookies invalid'
            context.close()
            return
        
        html = page.content()
        result['debug']['html_length'] = len(html)
        
        # === STRATEGY A: HTML "lượt xem" pattern (V7.3) ===
        html_views, view_attempts = search_views_in_html(html)
        result['debug']['view_extraction_attempts'] = view_attempts
        if html_views > 0:
            result['debug']['view_strategies_tried'].append('A_html_luot_xem')
        
        # === STRATEGY C: GraphQL captured responses ===
        graphql_views = 0
        graphql_found = []
        view_patterns_json = [
            r'"video_view_count"\s*:\s*(\d+)',
            r'"play_count"\s*:\s*(\d+)',
            r'"viewCount"\s*:\s*(\d+)',
            r'"reels_view_count"\s*:\s*(\d+)',
            r'"organic_view_count"\s*:\s*(\d+)',
            r'"unified_view_count_renderer"[^}]*"count"\s*:\s*(\d+)',
            r'"playbackVideoMetadata"[^}]*"viewCount"\s*:\s*(\d+)',
        ]
        
        for resp_body in captured_responses:
            for pat in view_patterns_json:
                matches = re.findall(pat, resp_body)
                for m in matches:
                    try:
                        val = int(m)
                        if 10 <= val <= 1000000000:
                            graphql_found.append({'pattern': pat[:30], 'value': val})
                            if val > graphql_views:
                                graphql_views = val
                    except:
                        pass
        
        result['debug']['graphql_views_found'] = graphql_found[:20]
        if graphql_views > 0:
            result['debug']['view_strategies_tried'].append('C_graphql')
        
        # === STRATEGY D: Search HTML JSON deep patterns ===
        html_json_views = 0
        for pat in view_patterns_json:
            matches = re.findall(pat, html)
            for m in matches:
                try:
                    val = int(m)
                    if 10 <= val <= 1000000000:
                        if val > html_json_views:
                            html_json_views = val
                except:
                    pass
        
        if html_json_views > 0:
            result['debug']['view_strategies_tried'].append('D_html_json')
            result['debug']['html_json_views'] = html_json_views
        
        # === COMBINE: take MAX from all strategies ===
        final_views = max(html_views, graphql_views, html_json_views)
        
        # Metadata extraction (with caption fix)
        extracted = extract_metadata_from_html(html)
        result['debug']['extracted_data'] = extracted
        
        # Username
        dom_data = extract_username_from_dom(page)
        
        result['debug']['html_search_results'] = {
            'has_luot_xem': 'l\u01b0\u1ee3t xem' in html,
            'has_nguoi_khac': 'ng\u01b0\u1eddi kh\u00e1c' in html,
            'has_binh_luan': 'b\u00ecnh lu\u1eadn' in html,
            'has_luot_chia_se': 'l\u01b0\u1ee3t chia s\u1ebb' in html,
            'has_video_view_count': 'video_view_count' in html,
            'has_play_count': 'play_count' in html,
        }
        
        result['debug']['mode_used'] = 'desktop'
        
        if final_views > 0:
            result['data']['views'] = final_views
        
        # Caption with FIX: increase limit + decode properly + fallback
        raw_caption = extracted.get('caption', '')
        if raw_caption:
            decoded = decode_unicode_string(raw_caption)
            # Increase limit to 5000 chars
            result['data']['caption'] = decoded[:5000]
        
        # Thumbnail with HTML entity decode
        raw_thumbnail = extracted.get('thumbnail', '')
        if raw_thumbnail:
            result['data']['thumbnail'] = decode_html_entities(raw_thumbnail)
        
        result['data']['username'] = dom_data.get('username', '')
        result['data']['post_id'] = extracted.get('post_id')
        result['data']['video_url'] = final_url
        
        context.close()
    except Exception as e:
        logger.warning(f'Desktop mode failed: {e}')


def try_mobile_mode(browser, url, cookies, result):
    """Mobile m.facebook.com for engagement"""
    try:
        result['debug']['tried_modes'].append('mobile')
        
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
        
        logger.info(f'Mobile navigating to: {mobile_url}')
        response = page.goto(mobile_url, wait_until='domcontentloaded', timeout=45000)
        time.sleep(random.uniform(5, 7))
        
        for offset in [200, 400, 600]:
            try:
                page.evaluate(f'window.scrollTo(0, {offset})')
                time.sleep(random.uniform(1, 2))
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
        
        # Engagement from mobile (V7.8 logic)
        engagement = parse_mobile_engagement(mobile_innertext, result['debug'])
        
        if engagement.get('likes', 0) > 0:
            result['data']['likes'] = engagement['likes']
            result['debug']['mode_used'] = 'desktop+mobile'
        if engagement.get('comments', 0) > 0:
            result['data']['comments'] = engagement['comments']
        if engagement.get('shares', 0) > 0:
            result['data']['shares'] = engagement['shares']
        
        # Try to find views in mobile too (NEW Strategy B+)
        if result['data']['views'] == 0:
            mobile_html = page.content()
            
            # Search for "X lượt xem" in mobile HTML
            view_patterns_mobile = [
                r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*xem',
                r'"play_count"\s*:\s*(\d+)',
                r'"video_view_count"\s*:\s*(\d+)',
            ]
            
            mobile_view_candidates = []
            for pat in view_patterns_mobile:
                matches = re.findall(pat, mobile_html, re.IGNORECASE)
                for m in matches[:20]:
                    if pat.startswith('([\\d'):
                        val = parse_vietnamese_number(m)
                    else:
                        try:
                            val = int(m)
                        except:
                            continue
                    if 10 <= val <= 1000000000:
                        mobile_view_candidates.append(val)
            
            if mobile_view_candidates:
                result['data']['views'] = max(mobile_view_candidates)
                result['debug']['view_strategies_tried'].append('B+_mobile_html')
        
        context.close()
    except Exception as e:
        logger.warning(f'Mobile mode failed: {e}')


def search_views_in_html(html):
    """V7.3 strategy"""
    view_candidates = []
    view_attempts = []
    
    view_patterns = [
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\\u01b0\\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u1ea7n\s*xem',
    ]
    
    for pat in view_patterns:
        matches = re.findall(pat, html, re.IGNORECASE)
        for m in matches[:20]:
            value = parse_vietnamese_number(m)
            if 10 <= value <= 1000000000:
                view_candidates.append(value)
                view_attempts.append({'match': str(m)[:30], 'value': value})
    
    final_views = max(view_candidates) if view_candidates else 0
    return final_views, view_attempts


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
    """Extract caption (with fixes), thumbnail, post_id"""
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
    
    # CAPTION - multiple strategies for full text
    caption_candidates = []
    
    # Strategy 1: og:description (clean, simple)
    m = re.search(r'<meta\s+property="og:description"\s+content="([^"]+)"', html)
    if m:
        caption_candidates.append(m.group(1))
    
    # Strategy 2: dangerouslySetInnerHTML message text (full caption)
    for pat in [
        r'"message"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
        r'"text"\s*:\s*"((?:[^"\\]|\\.)+)"\s*,\s*"is_explicit_locale"',
        r'"description"\s*:\s*\{\s*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
        r'"creation_story"[^}]*"message"[^}]*"text"\s*:\s*"((?:[^"\\]|\\.)+)"',
    ]:
        m = re.search(pat, html)
        if m:
            caption_candidates.append(m.group(1))
    
    # Pick the LONGEST caption candidate (usually the full one)
    if caption_candidates:
        # Sort by length, take longest
        caption_candidates.sort(key=len, reverse=True)
        data['caption'] = caption_candidates[0]
    
    # THUMBNAIL
    for pat in [
        r'<meta\s+property="og:image"\s+content="([^"]+)"',
        r'"first_frame_thumbnail"\s*:\s*"([^"]+)"',
        r'"image"\s*:\s*\{\s*"uri"\s*:\s*"([^"]+)"',
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
        'version': '7.9-multi-strategy',
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
        'version': '7.9-multi-strategy',
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
