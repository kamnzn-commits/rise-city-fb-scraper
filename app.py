"""
Rise City Facebook Scraper API - V7.8 🎩
KEY INSIGHT: Mobile FB hiển thị icon + số:
  󰍸 (icon thích) → 96 likes
  󰍹 (icon comment) → 28 comments
  󰍺 (icon share) → 2 shares

V7.8 parse PATTERN ICON-NUMBER thay vì keyword "X bình luận"
- Lấy 3 số đầu tiên xuất hiện sau icon FB ở đầu mobile innertext
- Đó chính là engagement của VIDEO ĐẦU TIÊN (target)
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

# FB icon characters (Private Use Area in Unicode)
# 󰍸 = like icon, 󰍹 = comment icon, 󰍺 = share icon
ICON_LIKE = '\udb80\udf78'      # 󰍸 (U+F0378)
ICON_COMMENT = '\udb80\udf79'   # 󰍹 (U+F0379) 
ICON_SHARE = '\udb80\udf7a'     # 󰍺 (U+F037A)


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
    """
    KEY INSIGHT V7.8: Mobile FB has format:
    
    󰍸     <- like icon
    96    <- like count
    󰍹     <- comment icon
    28    <- comment count
    󰍺     <- share icon
    2     <- share count (or empty if 0)
    Trần Xuân Hậu
    
    Strategy:
    1. Look for icon → number pattern using Unicode Private Use Area
    2. First triplet usually = target video engagement
    3. Verify by checking name comes after
    """
    data = {'likes': 0, 'comments': 0, 'shares': 0}
    debug_attempts = []
    
    if not innertext:
        return data
    
    # Strategy 1: Match icon followed by number on next line(s)
    # Use [\uF0000-\uFFFFF] for Private Use Area chars (FB icons)
    # Pattern: ICON \n NUMBER \n
    
    # Generic pattern: any FB icon char then digits
    # FB icons are in Supplementary Private Use Area (U+F0000 to U+FFFFD)
    icon_number_pattern = r'[\U000F0000-\U000FFFFD]\s*\n\s*([\d.,]+\s*[KkMmBb]?)\s*\n'
    matches = re.findall(icon_number_pattern, innertext)
    
    debug_info['icon_pattern_matches'] = matches[:10]
    debug_attempts.append({'strategy': 'icon_number', 'matches': matches[:10]})
    
    # If we got at least 3 matches, first 3 = likes, comments, shares
    # (in mobile FB order: like, comment, share)
    if len(matches) >= 3:
        like_val = parse_vietnamese_number(matches[0])
        comment_val = parse_vietnamese_number(matches[1])
        share_val = parse_vietnamese_number(matches[2])
        
        # Sanity check: likes should be largest, shares smallest typically
        # But we trust the order from FB UI
        if like_val > 0:
            data['likes'] = like_val
        if comment_val >= 0:
            data['comments'] = comment_val
        if share_val >= 0:
            data['shares'] = share_val
        
        debug_attempts.append({
            'strategy': 'first_3_icons',
            'likes': like_val,
            'comments': comment_val,
            'shares': share_val
        })
    elif len(matches) == 2:
        # No share number (share = 0, hidden in UI)
        like_val = parse_vietnamese_number(matches[0])
        comment_val = parse_vietnamese_number(matches[1])
        if like_val > 0:
            data['likes'] = like_val
        if comment_val >= 0:
            data['comments'] = comment_val
        debug_attempts.append({
            'strategy': '2_icons',
            'likes': like_val,
            'comments': comment_val,
        })
    elif len(matches) == 1:
        # Only likes, comments=0, shares=0
        like_val = parse_vietnamese_number(matches[0])
        if like_val > 0:
            data['likes'] = like_val
        debug_attempts.append({'strategy': '1_icon', 'likes': like_val})
    
    # Strategy 2 (fallback): If no icon-number match, try simple line-by-line
    # Look for lines that are pure numbers near the start
    if data['likes'] == 0:
        lines = innertext.split('\n')
        pure_numbers = []
        for i, line in enumerate(lines[:30]):  # First 30 lines only
            stripped = line.strip()
            if re.match(r'^[\d.,]+\s*[KkMmBb]?$', stripped):
                value = parse_vietnamese_number(stripped)
                if 0 <= value <= 100000000:
                    pure_numbers.append({'line': i, 'value': value})
        
        debug_attempts.append({'strategy': 'pure_numbers', 'found': pure_numbers[:10]})
        
        # First 3 pure numbers = likes, comments, shares
        if len(pure_numbers) >= 3:
            data['likes'] = pure_numbers[0]['value']
            data['comments'] = pure_numbers[1]['value']
            data['shares'] = pure_numbers[2]['value']
        elif len(pure_numbers) == 2:
            data['likes'] = pure_numbers[0]['value']
            data['comments'] = pure_numbers[1]['value']
        elif len(pure_numbers) == 1:
            data['likes'] = pure_numbers[0]['value']
    
    debug_info['mobile_parse_attempts'] = debug_attempts
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
            'mode_used': '',
            'tried_modes': [],
            'html_search_results': {},
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
            
            # Always run desktop for views + metadata
            try_desktop_mode(browser, url, cookies, result)
            
            # ALWAYS run mobile for engagement (V7.8 strategy)
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
    """Desktop for views + metadata"""
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
        
        logger.info(f'Desktop navigating to: {url}')
        response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
        time.sleep(8)
        simulate_human(page)
        
        final_url = page.url
        page_title = page.title()
        result['debug']['final_url'] = final_url
        result['debug']['page_title'] = page_title
        result['debug']['response_status'] = response.status if response else None
        
        if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
            result['error'] = 'Redirected to login - cookies invalid'
            context.close()
            return
        
        html = page.content()
        result['debug']['html_length'] = len(html)
        
        # Views (V7.3 strategy - works!)
        html_views, view_attempts = search_views_in_html(html)
        result['debug']['view_extraction_attempts'] = view_attempts
        
        # Metadata
        extracted = extract_metadata_from_html(html)
        result['debug']['extracted_data'] = extracted
        
        # Username
        dom_data = extract_username_from_dom(page)
        
        result['debug']['html_search_results'] = {
            'has_luot_xem': 'l\u01b0\u1ee3t xem' in html,
            'has_nguoi_khac': 'ng\u01b0\u1eddi kh\u00e1c' in html,
            'has_binh_luan': 'b\u00ecnh lu\u1eadn' in html,
            'has_luot_chia_se': 'l\u01b0\u1ee3t chia s\u1ebb' in html,
        }
        
        result['debug']['mode_used'] = 'desktop'
        
        if html_views > 0:
            result['data']['views'] = html_views
        result['data']['caption'] = decode_unicode_string(extracted.get('caption', ''))[:500]
        result['data']['thumbnail'] = extracted.get('thumbnail', '')
        result['data']['username'] = dom_data.get('username', '')
        result['data']['post_id'] = extracted.get('post_id')
        result['data']['video_url'] = final_url
        
        context.close()
    except Exception as e:
        logger.warning(f'Desktop mode failed: {e}')


def try_mobile_mode(browser, url, cookies, result):
    """Mobile m.facebook.com for engagement (V7.8 KEY)"""
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
        
        # Get mobile innertext
        mobile_innertext = ''
        try:
            mobile_innertext = page.evaluate('document.body.innerText || ""')
            result['debug']['mobile_innertext_length'] = len(mobile_innertext)
            result['debug']['mobile_innertext_sample'] = mobile_innertext[:1000]
        except:
            pass
        
        # PARSE WITH NEW V7.8 STRATEGY
        engagement = parse_mobile_engagement(mobile_innertext, result['debug'])
        
        # Update result
        if engagement.get('likes', 0) > 0:
            result['data']['likes'] = engagement['likes']
            result['debug']['mode_used'] = 'desktop+mobile'
        if engagement.get('comments', 0) > 0:
            result['data']['comments'] = engagement['comments']
        if engagement.get('shares', 0) > 0:
            result['data']['shares'] = engagement['shares']
        
        context.close()
    except Exception as e:
        logger.warning(f'Mobile mode failed: {e}')


def search_views_in_html(html):
    """V7.3 strategy - DON'T CHANGE"""
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
            if 10 <= value <= 100000000:
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
    data = {}
    patterns = {
        'post_id': [
            r'"video_id[":\s]*"(\d+)"',
            r'"top_level_post_id[":\s]*"(\d+)"',
        ],
        'caption': [
            r'"message[":\s]*\{[^}]*"text[":\s]*"([^"]+)"',
            r'<meta\s+property="og:description"\s+content="([^"]+)"',
        ],
        'thumbnail': [
            r'<meta\s+property="og:image"\s+content="([^"]+)"',
            r'"first_frame_thumbnail[":\s]*"([^"]+)"',
        ]
    }
    
    for field, pats in patterns.items():
        for pat in pats:
            match = re.search(pat, html)
            if match:
                value = match.group(1)
                cleaned = value.replace('\\u003C', '<').replace('\\/', '/').replace('\\u0026', '&')
                if cleaned:
                    data[field] = cleaned[:500]
                    break
    return data


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'ok',
        'service': 'Rise City Facebook Scraper 🎩',
        'version': '7.8-icon-parse',
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
        'version': '7.8-icon-parse',
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
