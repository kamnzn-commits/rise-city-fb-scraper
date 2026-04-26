"""
Rise City Facebook Scraper API - V6.1 with CORS 🎩
Updates from V6:
+ Added CORS headers (allow Hoppscotch Browser, Lovable Edge Function, etc.)
+ Same V6 features: Mobile viewport, DOM extraction, multi-strategy view extraction
"""
from flask import Flask, request, jsonify, make_response
from playwright.sync_api import sync_playwright
import os
import re
import json
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

COOKIES_PATH = os.getenv('FB_COOKIES_PATH', '/etc/secrets/cookies.txt')
API_SECRET = os.getenv('API_SECRET', 'rise-city-secret-2026')


# === CORS MIDDLEWARE ===
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
    """Handle CORS preflight"""
    response = make_response('', 204)
    return response


def decode_unicode_string(s):
    """Decode \\uXXXX escape sequences"""
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
    """Parse '1,5K' -> 1500, '2,3M' -> 2300000, etc."""
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
            'final_url': '',
            'page_title': '',
            'cookies_count': len(cookies),
            'extracted_data': {},
            'view_extraction_attempts': [],
        }
    }
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox', '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-gpu', '--no-first-run',
                    '--autoplay-policy=no-user-gesture-required',
                ]
            )
            
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 414, 'height': 896},
                locale='vi-VN',
                timezone_id='Asia/Ho_Chi_Minh',
                is_mobile=True,
                has_touch=True,
                extra_http_headers={
                    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8',
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
            
            logger.info(f'Navigating to: {url}')
            response = page.goto(url, wait_until='domcontentloaded', timeout=60000)
            time.sleep(5)
            
            html = page.content()
            post_id_match = re.search(r'"video_id[":\s]*"(\d+)"', html) or \
                           re.search(r'"top_level_post_id[":\s]*"(\d+)"', html)
            
            if post_id_match:
                post_id = post_id_match.group(1)
                reel_url = f'https://m.facebook.com/reel/{post_id}'
                logger.info(f'Trying mobile reel URL: {reel_url}')
                try:
                    page.goto(reel_url, wait_until='domcontentloaded', timeout=30000)
                    time.sleep(8)
                except Exception as e:
                    logger.warning(f'Mobile reel URL failed: {e}')
            
            try:
                page.evaluate('window.scrollTo(0, 100)')
                time.sleep(2)
                page.evaluate('window.scrollTo(0, 300)')
                time.sleep(2)
                page.evaluate('window.scrollTo(0, 0)')
                time.sleep(2)
            except:
                pass
            
            time.sleep(5)
            
            final_url = page.url
            page_title = page.title()
            result['debug']['final_url'] = final_url
            result['debug']['page_title'] = page_title
            
            if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
                result['error'] = 'Redirected to login - cookies invalid or expired'
                browser.close()
                return result
            
            html = page.content()
            result['debug']['html_length'] = len(html)
            
            views = extract_views_multistrategy(page, html, result['debug'])
            extracted = extract_from_html(html)
            result['debug']['extracted_data'] = extracted
            
            result['data']['views'] = views
            result['data']['likes'] = extracted.get('likes', 0)
            result['data']['comments'] = extracted.get('comments', 0)
            result['data']['shares'] = extracted.get('shares', 0)
            result['data']['caption'] = decode_unicode_string(extracted.get('caption', ''))[:500]
            result['data']['thumbnail'] = extracted.get('thumbnail', '')
            result['data']['username'] = extracted.get('username', '')
            result['data']['post_id'] = extracted.get('post_id')
            result['data']['video_url'] = final_url
            
            if (result['data']['views'] > 0 or 
                result['data']['likes'] > 0 or
                result['data']['caption']):
                result['success'] = True
            
            browser.close()
    except Exception as e:
        logger.exception('Playwright scrape failed')
        result['error'] = str(e)
        result['error_type'] = type(e).__name__
    
    return result


def extract_views_multistrategy(page, html, debug_info):
    attempts = []
    candidate_views = []
    
    # Strategy 1: HTML regex
    html_patterns = [
        r'"video_view_count[":\s]*(\d+)',
        r'"video_view_count_renderer"[^}]*"count[":\s]*(\d+)',
        r'"play_count[":\s]*(\d+)',
        r'"viewCount[":\s]*(\d+)',
        r'"unified_view_count_renderer"[^}]*"count[":\s]*(\d+)',
        r'"reels_view_count[":\s]*(\d+)',
        r'"organic_view_count[":\s]*(\d+)',
        r'"playbackVideoMetadata"[^}]*"viewCount[":\s]*(\d+)',
        r'"video_view_count_text"[^}]*"text[":\s]*"([^"]+)"',
    ]
    
    for pattern in html_patterns:
        match = re.search(pattern, html)
        if match:
            value = parse_vietnamese_number(match.group(1))
            if value > 0:
                candidate_views.append(value)
                attempts.append({'strategy': 'html_regex', 'value': value})
    
    # Strategy 2: JS DOM walker
    try:
        js_views = page.evaluate("""
            () => {
                const results = [];
                const spans = document.querySelectorAll('span');
                for (const span of spans) {
                    const text = (span.textContent || '').trim();
                    if (/^[\\d.,]+\\s*[KkMmBb]?$/.test(text)) {
                        const parent = span.parentElement;
                        if (parent && parent.querySelector('svg')) {
                            results.push({ text: text, hasIcon: true });
                        }
                    }
                }
                
                // Also search for "lượt xem" / "view" text
                const walker = document.createTreeWalker(
                    document.body, NodeFilter.SHOW_TEXT, null, false
                );
                let node;
                while (node = walker.nextNode()) {
                    const parent = node.parentElement;
                    if (!parent) continue;
                    const fullText = parent.textContent || '';
                    if (/(view|lượt xem|lần xem)/i.test(fullText)) {
                        const m = fullText.match(/([\\d.,]+\\s*[KkMmBb]?)/);
                        if (m) results.push({ text: fullText.substring(0, 80), number: m[1] });
                    }
                }
                
                return results;
            }
        """)
        
        if js_views:
            for item in js_views[:10]:
                num_str = item.get('number', item.get('text', ''))
                try:
                    s = num_str.strip().replace(',', '.')
                    multiplier = 1
                    if s.lower().endswith('k'):
                        multiplier = 1000
                        s = s[:-1]
                    elif s.lower().endswith('m'):
                        multiplier = 1000000
                        s = s[:-1]
                    value = int(float(s) * multiplier)
                    if 10 <= value <= 100000000:
                        candidate_views.append(value)
                        attempts.append({
                            'strategy': 'js_dom', 
                            'text': item.get('text', '')[:50], 
                            'value': value,
                            'hasIcon': item.get('hasIcon', False)
                        })
                except:
                    pass
    except Exception as e:
        attempts.append({'strategy': 'js_dom', 'error': str(e)[:100]})
    
    debug_info['view_extraction_attempts'] = attempts
    
    if candidate_views:
        valid_views = [v for v in candidate_views if 1 <= v <= 100000000]
        if valid_views:
            sorted_views = sorted(valid_views)
            return sorted_views[len(sorted_views) // 2]
    
    return 0


def extract_from_html(html):
    data = {}
    patterns = {
        'likes': [
            r'"reaction_count"[^}]*"count[":\s]*(\d+)',
            r'"top_reactions"[^}]*"count[":\s]*(\d+)',
            r'"likers"[^}]*"count[":\s]*(\d+)',
            r'"reactors"[^}]*"count[":\s]*(\d+)',
        ],
        'comments': [
            r'"total_comment_count[":\s]*(\d+)',
            r'"comment_count"[^}]*"total_count[":\s]*(\d+)',
        ],
        'shares': [
            r'"share_count"[^}]*"count[":\s]*(\d+)',
            r'"reshare_count[":\s]*(\d+)',
        ],
        'post_id': [
            r'"video_id[":\s]*"(\d+)"',
            r'"top_level_post_id[":\s]*"(\d+)"',
        ],
        'username': [
            r'"page_name[":\s]*"([^"]+)"',
            r'"profile_url[":\s]*"https://www\.facebook\.com/([^/"]+)"',
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
                if field in ['likes', 'comments', 'shares']:
                    parsed = parse_vietnamese_number(value)
                    if parsed > 0:
                        data[field] = parsed
                        break
                else:
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
        'version': '6.1-cors',
        'engine': 'Playwright Mobile + DOM extraction + CORS',
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
        'version': '6.1-cors',
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
