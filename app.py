"""
Rise City Facebook Scraper API - V7.7 🎩
LAST ATTEMPT - Human simulation:
1. Mouse movement (giả lập người thật)
2. Random delays (không đều)
3. Mobile m.facebook.com fallback nếu desktop fail
4. Wait for selector (đợi engagement DOM load)
5. Click vào reel để trigger full UI
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


def simulate_human(page):
    """Simulate human mouse/keyboard activity"""
    try:
        # Random mouse moves
        for _ in range(3):
            x = random.randint(100, 1200)
            y = random.randint(100, 600)
            page.mouse.move(x, y)
            time.sleep(random.uniform(0.3, 0.8))
        
        # Random scroll (smooth)
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
            'engagement_attempts': [],
            'mode_used': '',
            'innertext_length': 0,
            'innertext_sample': '',
            'html_search_results': {},
            'tried_modes': [],
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
            
            # === MODE 1: Desktop with full simulation ===
            success = try_desktop_mode(browser, url, cookies, result)
            
            if not success or (result['data']['likes'] == 0 and result['data']['comments'] == 0):
                # === MODE 2: Mobile fallback ===
                logger.info('Desktop mode insufficient, trying mobile')
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
    """Try desktop mode with human simulation"""
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
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        context.add_cookies(cookies)
        page = context.new_page()
        
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en'] });
            window.chrome = { runtime: {} };
        """)
        
        logger.info(f'Desktop mode navigating to: {url}')
        response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
        
        # Initial wait
        time.sleep(random.uniform(3, 5))
        
        # Simulate human
        simulate_human(page)
        
        # Try wait for engagement element
        try:
            page.wait_for_selector('text=/(lượt xem|bình luận|người khác)/i', timeout=10000)
            logger.info('Engagement text found via wait_for_selector')
        except:
            logger.info('wait_for_selector timeout')
        
        # Final wait
        time.sleep(random.uniform(2, 4))
        
        final_url = page.url
        page_title = page.title()
        result['debug']['final_url'] = final_url
        result['debug']['page_title'] = page_title
        result['debug']['response_status'] = response.status if response else None
        
        if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
            result['error'] = 'Redirected to login - cookies invalid'
            context.close()
            return False
        
        html = page.content()
        result['debug']['html_length'] = len(html)
        
        # Capture innertext for debug
        try:
            innertext = page.evaluate('document.body.innerText || ""')
            result['debug']['innertext_length'] = len(innertext)
            result['debug']['innertext_sample'] = innertext[:500]
        except:
            pass
        
        # Views (V7.3 strategy)
        html_views, view_attempts = search_views_in_html(html)
        result['debug']['view_extraction_attempts'] = view_attempts
        
        # Engagement
        engagement = extract_engagement_v77(page, html, result['debug'])
        
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
        
        # Set values
        if html_views > 0:
            result['data']['views'] = html_views
        result['data']['likes'] = engagement.get('likes', 0)
        result['data']['comments'] = engagement.get('comments', 0)
        result['data']['shares'] = engagement.get('shares', 0)
        result['data']['caption'] = decode_unicode_string(extracted.get('caption', ''))[:500]
        result['data']['thumbnail'] = extracted.get('thumbnail', '')
        result['data']['username'] = dom_data.get('username', '')
        result['data']['post_id'] = extracted.get('post_id')
        result['data']['video_url'] = final_url
        
        context.close()
        return True
    except Exception as e:
        logger.warning(f'Desktop mode failed: {e}')
        return False


def try_mobile_mode(browser, url, cookies, result):
    """Try mobile m.facebook.com mode"""
    try:
        result['debug']['tried_modes'].append('mobile')
        
        # Convert URL to m.facebook.com if possible
        mobile_url = url.replace('www.facebook.com', 'm.facebook.com')
        if 'm.facebook.com' not in mobile_url:
            mobile_url = mobile_url.replace('facebook.com', 'm.facebook.com')
        
        # Adjust cookies domain
        mobile_cookies = []
        for c in cookies:
            new_c = dict(c)
            if c['domain'] == '.facebook.com' or c['domain'] == 'facebook.com':
                new_c['domain'] = '.facebook.com'  # works for both
            mobile_cookies.append(new_c)
        
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
        
        context.add_cookies(mobile_cookies)
        page = context.new_page()
        
        logger.info(f'Mobile mode navigating to: {mobile_url}')
        response = page.goto(mobile_url, wait_until='domcontentloaded', timeout=45000)
        
        time.sleep(random.uniform(5, 8))
        
        # Scroll on mobile
        for offset in [200, 400, 600]:
            try:
                page.evaluate(f'window.scrollTo(0, {offset})')
                time.sleep(random.uniform(1, 2))
            except:
                pass
        
        time.sleep(2)
        
        html = page.content()
        
        # Try to extract from mobile HTML
        mobile_engagement = extract_engagement_mobile(html, page, result['debug'])
        
        # Update result if mobile found better data
        if mobile_engagement.get('likes', 0) > result['data']['likes']:
            result['data']['likes'] = mobile_engagement['likes']
            result['debug']['mode_used'] = 'desktop+mobile'
        if mobile_engagement.get('comments', 0) > result['data']['comments']:
            result['data']['comments'] = mobile_engagement['comments']
        if mobile_engagement.get('shares', 0) > result['data']['shares']:
            result['data']['shares'] = mobile_engagement['shares']
        
        # Mobile innertext
        try:
            mobile_innertext = page.evaluate('document.body.innerText || ""')
            result['debug']['mobile_innertext_length'] = len(mobile_innertext)
            result['debug']['mobile_innertext_sample'] = mobile_innertext[:500]
        except:
            pass
        
        context.close()
    except Exception as e:
        logger.warning(f'Mobile mode failed: {e}')


def extract_engagement_mobile(html, page, debug_info):
    """Extract engagement from m.facebook.com (often has plain text)"""
    data = {}
    attempts = []
    
    # Try DOM innerText first
    try:
        innertext = page.evaluate('document.body.innerText || ""')
        debug_info['mobile_innertext_sample'] = innertext[:1000]
        
        # Mobile may have different format: "63 người", "3 bình luận"
        text_to_search = innertext + '\n' + html
        
        # Likes
        like_patterns = [
            r'v\u00e0\s*([\d.,]+\s*[KkMmBb]?)\s*ng\u01b0\u1eddi\s*kh\u00e1c',
            r'([\d.,]+\s*[KkMmBb]?)\s*ng\u01b0\u1eddi\s*\u0111\u00e3\s*th\u00edch',
            r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*th\u00edch',
            r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*reactions?',
        ]
        like_candidates = []
        for pat in like_patterns:
            matches = re.findall(pat, text_to_search, re.IGNORECASE)
            for m in matches[:10]:
                value = parse_vietnamese_number(m)
                if 1 <= value <= 100000000:
                    if 'kh\u00e1c' in pat:
                        like_candidates.append(value + 1)
                    else:
                        like_candidates.append(value)
                    attempts.append({'type': 'like', 'match': m, 'pattern': pat[:30]})
        if like_candidates:
            data['likes'] = max(like_candidates)
        
        # Comments
        comment_patterns = [
            r'([\d.,]+\s*[KkMmBb]?)\s*b\u00ecnh\s*lu\u1eadn',
            r'([\d.,]+\s*[KkMmBb]?)\s*comments?',
        ]
        comment_candidates = []
        for pat in comment_patterns:
            matches = re.findall(pat, text_to_search, re.IGNORECASE)
            for m in matches[:10]:
                value = parse_vietnamese_number(m)
                if 0 <= value <= 100000000:
                    comment_candidates.append(value)
                    attempts.append({'type': 'comment', 'match': m, 'pattern': pat[:30]})
        if comment_candidates:
            from collections import Counter
            counter = Counter(comment_candidates)
            data['comments'] = counter.most_common(1)[0][0]
        
        # Shares
        share_patterns = [
            r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*chia\s*s\u1ebb',
            r'([\d.,]+\s*[KkMmBb]?)\s*shares?',
        ]
        share_candidates = []
        for pat in share_patterns:
            matches = re.findall(pat, text_to_search, re.IGNORECASE)
            for m in matches[:10]:
                value = parse_vietnamese_number(m)
                if 1 <= value <= 100000000:
                    share_candidates.append(value)
                    attempts.append({'type': 'share', 'match': m, 'pattern': pat[:30]})
        if share_candidates:
            from collections import Counter
            counter = Counter(share_candidates)
            data['shares'] = counter.most_common(1)[0][0]
    except Exception as e:
        attempts.append({'error': str(e)[:100]})
    
    debug_info['mobile_engagement_attempts'] = attempts[:10]
    return data


def search_views_in_html(html):
    """V7.3 strategy - works for views"""
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


def extract_engagement_v77(page, html, debug_info):
    """Extract engagement from desktop page"""
    data = {'likes': 0, 'comments': 0, 'shares': 0}
    attempts = []
    
    try:
        # Get DOM text + aria-labels combined
        dom_results = page.evaluate("""
            () => {
                const result = {ariaLabels: [], titles: [], innerText: ''};
                document.querySelectorAll('[aria-label]').forEach(el => {
                    const label = el.getAttribute('aria-label') || '';
                    if (/\\d/.test(label) && label.length < 200) {
                        result.ariaLabels.push(label);
                    }
                });
                document.querySelectorAll('[title]').forEach(el => {
                    const title = el.getAttribute('title') || '';
                    if (/\\d/.test(title) && title.length < 200) {
                        result.titles.push(title);
                    }
                });
                result.innerText = document.body.innerText || '';
                return result;
            }
        """)
        
        all_text = '\n'.join([
            *(dom_results.get('ariaLabels') or []),
            *(dom_results.get('titles') or []),
            dom_results.get('innerText', ''),
            html,  # last resort
        ])
        
        # Likes
        like_patterns = [
            r'v\u00e0\s*([\d.,]+\s*[KkMmBb]?)\s*ng\u01b0\u1eddi\s*kh\u00e1c',
            r'and\s*([\d.,]+\s*[KkMmBb]?)\s*others?',
            r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*th\u00edch',
            r'([\d.,]+\s*[KkMmBb]?)\s*ng\u01b0\u1eddi\s*\u0111\u00e3\s*th\u00edch',
        ]
        like_candidates = []
        for pat in like_patterns:
            matches = re.findall(pat, all_text, re.IGNORECASE)
            for m in matches[:10]:
                value = parse_vietnamese_number(m)
                if 1 <= value <= 100000000:
                    if 'kh\u00e1c' in pat or 'other' in pat:
                        like_candidates.append(value + 1)
                    else:
                        like_candidates.append(value)
                    attempts.append({'type': 'like', 'match': m})
        if like_candidates:
            data['likes'] = max(like_candidates)
        
        # Comments
        comment_patterns = [
            r'([\d.,]+\s*[KkMmBb]?)\s*b\u00ecnh\s*lu\u1eadn',
            r'([\d.,]+\s*[KkMmBb]?)\s*comments?',
        ]
        comment_candidates = []
        for pat in comment_patterns:
            matches = re.findall(pat, all_text, re.IGNORECASE)
            for m in matches[:10]:
                value = parse_vietnamese_number(m)
                if 0 <= value <= 100000000:
                    comment_candidates.append(value)
                    attempts.append({'type': 'comment', 'match': m})
        if comment_candidates:
            from collections import Counter
            counter = Counter(comment_candidates)
            data['comments'] = counter.most_common(1)[0][0]
        
        # Shares
        share_patterns = [
            r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*chia\s*s\u1ebb',
            r'([\d.,]+\s*[KkMmBb]?)\s*shares?',
        ]
        share_candidates = []
        for pat in share_patterns:
            matches = re.findall(pat, all_text, re.IGNORECASE)
            for m in matches[:10]:
                value = parse_vietnamese_number(m)
                if 1 <= value <= 100000000:
                    share_candidates.append(value)
                    attempts.append({'type': 'share', 'match': m})
        if share_candidates:
            from collections import Counter
            counter = Counter(share_candidates)
            data['shares'] = counter.most_common(1)[0][0]
        
        debug_info['dom_text_samples'] = {
            'aria_labels_count': len(dom_results.get('ariaLabels', [])),
            'titles_count': len(dom_results.get('titles', [])),
            'innertext_length': len(dom_results.get('innerText', '')),
        }
    except Exception as e:
        attempts.append({'error': str(e)[:100]})
    
    debug_info['engagement_attempts'] = attempts[:10]
    return data


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
        'version': '7.7-human-sim',
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
        'version': '7.7-human-sim',
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
