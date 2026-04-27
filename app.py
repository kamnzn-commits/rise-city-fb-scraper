"""
Rise City Facebook Scraper API - V8.3 🎩
ROOT CAUSE FIX - ICON MAPPING BY CODEPOINT:

V8.1-V8.2 BUG:
- Regex lấy [icon, number] theo thứ tự xuất hiện → SAI
- Khi likes=0 hoặc shares=0, FB ẨN số → regex lấy nhầm số của icon khác

V8.3 FIX:
- Map theo Unicode codepoint chính xác:
  U+F0378 (󰍸) = LIKE icon
  U+F0379 (󰍹) = COMMENT icon
  U+F037A (󰍺) = SHARE icon
  U+F174D (󱝍) = EYE/VIEW icon
- Tìm ICON → số NGAY SAU nó (1-2 dòng)
- Nếu gặp ICON khác trước khi gặp số → metric = 0 (FB ẩn)
- Chỉ lấy FIRST occurrence (target reel, không related)
- Kết hợp isolation (V8.2) để chặn related reels

5 ATTEMPTS PER REQUEST (giữ nguyên):
1. Desktop Chrome + cookies (metadata)
2. iPhone 15 Pro anonymous (views)
3. Android S24 anonymous (views)
4. mbasic.facebook.com (views)
5. Mobile m.facebook.com (engagement with icon-mapping fix)
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

# Facebook PUA icon codepoints
ICON_LIKE  = '\U000F0378'  # 󰍸
ICON_CMT   = '\U000F0379'  # 󰍹
ICON_SHARE = '\U000F037A'  # 󰍺
ICON_EYE   = '\U000F174D'  # 󱝍

RELATED_MARKERS = [
    'Watch more reels like this',
    'C\u00f2n nhi\u1ec1u n\u1ed9i dung kh\u00e1c',
    'Explore these popular topics',
    'H\u00e3y \u0111\u0103ng nh\u1eadp \u0111\u1ec3 kh\u00e1m ph\u00e1',
    'Ti\u1ebfp t\u1ee5c d\u01b0\u1edbi t\u00ean',
    '\u0110\u0103ng nh\u1eadp \u0111\u1ec3 k\u1ebft n\u1ed1i',
    'See more reels',
]


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


def isolate_target_innertext(innertext):
    """Cắt innertext chỉ giữ phần TARGET reel, bỏ related"""
    if not innertext:
        return ''
    cut_pos = len(innertext)
    for marker in RELATED_MARKERS:
        pos = innertext.find(marker)
        if pos > 0 and pos < cut_pos:
            cut_pos = pos
    return innertext[:cut_pos]


def find_number_after_line(lines, start_idx, max_look=2):
    """
    V8.3 CORE: Tìm số NGAY SAU dòng start_idx (1-2 dòng tiếp).
    Nếu gặp icon khác hoặc text dài trước khi gặp số → return None (metric=0).
    """
    for i in range(start_idx + 1, min(start_idx + 1 + max_look, len(lines))):
        line = lines[i].strip()
        if not line:
            continue
        # Check if line IS a number
        if re.match(r'^[\d.,]+\s*[KkMmBb]?$', line):
            return line
        # Check if line contains ANY PUA icon → stop (another metric starts)
        has_icon = any(ord(c) > 0xF0000 for c in line)
        if has_icon:
            return None
        # Check if line is text content (username, caption, etc) → stop
        if len(line) > 3 and not line[0].isdigit():
            return None
    return None


def parse_mobile_engagement_v83(innertext, debug_info):
    """
    V8.3: Parse engagement bằng icon codepoint mapping.
    
    FB mobile reel innertext structure:
    - LIKE_ICON  (nếu likes>0 → số ngay sau, nếu likes=0 → icon tiếp theo ngay)
    - CMT_ICON   (nếu comments>0 → số ngay sau)
    - SHARE_ICON (nếu shares>0 → số ngay sau)
    - Username
    - Caption
    - ...related reels
    
    Logic: Tìm FIRST occurrence của mỗi icon trong isolated text,
    check số ngay sau. Nếu không có số → metric = 0.
    """
    data = {'likes': 0, 'comments': 0, 'shares': 0}
    
    if not innertext:
        return data
    
    # Isolate target reel
    target_text = isolate_target_innertext(innertext)
    lines = target_text.split('\n')
    
    debug_info['v83_isolated_length'] = len(target_text)
    
    found_like = False
    found_cmt = False
    found_share = False
    
    for i, line in enumerate(lines):
        for c in line:
            cp = ord(c)
            if cp == ord(ICON_LIKE) and not found_like:
                found_like = True
                val = find_number_after_line(lines, i)
                data['likes'] = parse_vietnamese_number(val) if val else 0
                debug_info['v83_like_raw'] = val
            elif cp == ord(ICON_CMT) and not found_cmt:
                found_cmt = True
                val = find_number_after_line(lines, i)
                data['comments'] = parse_vietnamese_number(val) if val else 0
                debug_info['v83_cmt_raw'] = val
            elif cp == ord(ICON_SHARE) and not found_share:
                found_share = True
                val = find_number_after_line(lines, i)
                data['shares'] = parse_vietnamese_number(val) if val else 0
                debug_info['v83_share_raw'] = val
    
    debug_info['v83_engagement'] = data
    return data


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
# FINGERPRINTS
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
        'Referer': 'https://www.google.com/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
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
        'sec-ch-ua': '"Google Chrome";v="124", "Chromium";v="124"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
    }
}


def try_anonymous_with_fingerprint(browser, url, fingerprint, name, debug_info):
    """Anonymous mode for views"""
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
        page = context.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en'] });
            window.chrome = { runtime: {} };
        """)
        
        captured_responses = []
        def handle_response(response):
            try:
                if response.status == 200:
                    url_lower = response.url.lower()
                    if any(k in url_lower for k in ['graphql', 'reel', 'video']):
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
        page.goto(url, wait_until='domcontentloaded', timeout=45000)
        time.sleep(random.uniform(3, 6))
        
        try:
            page.evaluate("""() => {
                document.querySelectorAll('[role="dialog"]').forEach(d => {
                    if ((d.textContent||'').includes('Đăng nhập') || (d.textContent||'').includes('Log in'))
                        d.style.display = 'none';
                });
            }""")
            time.sleep(1)
        except:
            pass
        
        try:
            page.evaluate('window.scrollTo({top: 400, behavior: "smooth"})')
            time.sleep(3)
        except:
            pass
        
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
        
        if 'login' in page.url.lower() or '\u0110\u0103ng nh\u1eadp v\u00e0o Facebook' in innertext[:200]:
            debug_info[f'{name}_blocked'] = True
            context.close()
            return 0
        
        all_text = '\n'.join([html, innertext] + captured_responses)
        views = search_views_in_text(all_text)
        debug_info[f'{name}_views_found'] = views
        
        context.close()
        return views
    except Exception as e:
        logger.warning(f'{name} mode failed: {e}')
        debug_info[f'{name}_error'] = str(e)[:200]
        return 0


def try_mbasic_for_views(browser, url, cookies, debug_info):
    """mbasic.facebook.com text-only"""
    try:
        debug_info['mbasic_attempted'] = True
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
        page.goto(mbasic_url, wait_until='domcontentloaded', timeout=30000)
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
        
        views = search_views_in_text(html + '\n' + innertext)
        debug_info['mbasic_views_found'] = views
        
        context.close()
        return views
    except Exception as e:
        logger.warning(f'mbasic failed: {e}')
        debug_info['mbasic_error'] = str(e)[:200]
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
            'version': '8.3-icon-mapping',
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
                    '--disable-gpu', '--no-first-run', '--disable-infobars',
                ]
            )
            
            views_1 = try_desktop_with_cookies(browser, url, cookies, result)
            result['debug']['view_sources']['desktop_cookies'] = views_1
            
            views_2 = try_anonymous_with_fingerprint(
                browser, url, FINGERPRINT_IPHONE_15, 'iphone15', result['debug']
            )
            result['debug']['view_sources']['iphone15_anon'] = views_2
            
            views_3 = try_anonymous_with_fingerprint(
                browser, url, FINGERPRINT_ANDROID_S24, 'android', result['debug']
            )
            result['debug']['view_sources']['android_anon'] = views_3
            
            views_4 = try_mbasic_for_views(browser, url, cookies, result['debug'])
            result['debug']['view_sources']['mbasic_cookies'] = views_4
            
            try_mobile_mode(browser, url, cookies, result)
            
            browser.close()
            
            final_views = max(views_1, views_2, views_3, views_4)
            if final_views > 0:
                result['data']['views'] = final_views
            
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
    """Mode 1: Desktop with cookies for metadata"""
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
        
        page.goto(url, wait_until='domcontentloaded', timeout=45000)
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


def try_mobile_mode(browser, url, cookies, result):
    """Mode 5: Mobile with V8.3 icon-mapping engagement"""
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
        
        page.goto(mobile_url, wait_until='domcontentloaded', timeout=45000)
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
        
        # V8.3: Parse engagement with icon-mapping
        engagement = parse_mobile_engagement_v83(mobile_innertext, result['debug'])
        
        # Always update (even if 0, that's correct for hidden metrics)
        result['data']['likes'] = engagement['likes']
        result['data']['comments'] = engagement['comments']
        result['data']['shares'] = engagement['shares']
        
        # Also search views in mobile innertext
        if mobile_innertext:
            target_text = isolate_target_innertext(mobile_innertext)
            mobile_views = search_views_in_text(target_text)
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
        'version': '8.3-icon-mapping',
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
        'version': '8.3-icon-mapping',
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
