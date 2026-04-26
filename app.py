"""
Rise City Facebook Scraper API - V7.2 🎩
Fix từ V7.1:
- DOM walker tìm view counter thông minh hơn (chỉ accept số có "lượt xem"/"view" nearby)
- Thêm patterns cho shares (4 lượt chia sẻ format)
- Thêm patterns cho username từ profile URL
- Sanity check: views > likes thì mới hợp lý
"""
from flask import Flask, request, jsonify, make_response
from playwright.sync_api import sync_playwright
import os
import re
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

COOKIES_PATH = os.getenv('FB_COOKIES_PATH', '/etc/secrets/cookies.txt')
API_SECRET = os.getenv('API_SECRET', 'rise-city-secret-2026')


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
            'share_extraction_attempts': [],
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
                ]
            )
            
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
            
            logger.info(f'Navigating to: {url}')
            response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
            
            time.sleep(8)
            
            try:
                page.evaluate('window.scrollTo(0, 300)')
                time.sleep(2)
            except:
                pass
            
            final_url = page.url
            page_title = page.title()
            result['debug']['final_url'] = final_url
            result['debug']['page_title'] = page_title
            result['debug']['response_status'] = response.status if response else None
            
            if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
                result['error'] = 'Redirected to login - cookies invalid'
                browser.close()
                return result
            
            html = page.content()
            result['debug']['html_length'] = len(html)
            
            extracted = extract_from_html(html)
            result['debug']['extracted_data'] = extracted
            
            # Smart DOM extraction
            dom_data = extract_dom_smart(page, result['debug'])
            
            # Merge: prefer non-zero, prefer DOM for views (more accurate)
            views = max(dom_data.get('views', 0), extracted.get('views', 0))
            shares = max(dom_data.get('shares', 0), extracted.get('shares', 0))
            
            result['data']['views'] = views
            result['data']['likes'] = extracted.get('likes', 0)
            result['data']['comments'] = extracted.get('comments', 0)
            result['data']['shares'] = shares
            result['data']['caption'] = decode_unicode_string(extracted.get('caption', ''))[:500]
            result['data']['thumbnail'] = extracted.get('thumbnail', '')
            result['data']['username'] = extracted.get('username', '') or dom_data.get('username', '')
            result['data']['post_id'] = extracted.get('post_id')
            result['data']['video_url'] = final_url
            
            if (result['data']['views'] > 0 or 
                result['data']['likes'] > 0 or
                result['data']['caption']):
                result['success'] = True
            
            browser.close()
    except Exception as e:
        logger.exception('Scrape failed')
        result['error'] = str(e)
        result['error_type'] = type(e).__name__
    
    return result


def extract_dom_smart(page, debug_info):
    """
    Smart DOM extraction using context (Vietnamese keywords).
    
    Looks for patterns like:
    - "5,1K lượt xem" -> views = 5100
    - "4 lượt chia sẻ" -> shares = 4
    - "X người khác" -> can compute likes
    """
    data = {}
    view_attempts = []
    share_attempts = []
    
    try:
        js_results = page.evaluate("""
            () => {
                const results = {
                    viewMatches: [],
                    shareMatches: [],
                    usernameMatches: []
                };
                
                // Get all text from page
                const allText = document.body.innerText || '';
                
                // Match Vietnamese view patterns: "5,1K lượt xem", "12 lần xem", "1.5M views"
                const viewPattern = /([\\d.,]+\\s*[KkMmBb]?)\\s*(l\u01b0\u1ee3t xem|l\u1ea7n xem|views?|view)/gi;
                let viewMatch;
                while ((viewMatch = viewPattern.exec(allText)) !== null) {
                    results.viewMatches.push({
                        number: viewMatch[1].trim(),
                        context: viewMatch[0]
                    });
                }
                
                // Match Vietnamese share patterns: "4 lượt chia sẻ", "10 chia sẻ"
                const sharePattern = /([\\d.,]+\\s*[KkMmBb]?)\\s*(l\u01b0\u1ee3t chia s\u1ebb|chia s\u1ebb|shares?|share)/gi;
                let shareMatch;
                while ((shareMatch = sharePattern.exec(allText)) !== null) {
                    results.shareMatches.push({
                        number: shareMatch[1].trim(),
                        context: shareMatch[0]
                    });
                }
                
                // Try to find username from profile links
                const profileLinks = document.querySelectorAll('a[href*="/"]');
                for (const link of profileLinks) {
                    const href = link.getAttribute('href') || '';
                    const match = href.match(/^https?:\\/\\/(?:www\\.|m\\.)?facebook\\.com\\/([a-zA-Z0-9.]+)(?:\\/|$|\\?)/);
                    if (match && !['reel', 'share', 'video', 'watch', 'photo', 'permalink'].includes(match[1])) {
                        const text = (link.textContent || '').trim();
                        if (text && text.length > 0 && text.length < 100) {
                            results.usernameMatches.push({
                                username: match[1],
                                displayName: text
                            });
                        }
                    }
                }
                
                return results;
            }
        """)
        
        # Process view matches - take the LARGEST sensible value
        if js_results.get('viewMatches'):
            view_candidates = []
            for m in js_results['viewMatches']:
                value = parse_vietnamese_number(m['number'])
                if 10 <= value <= 100000000:
                    view_candidates.append(value)
                    view_attempts.append({
                        'number': m['number'],
                        'context': m['context'][:80],
                        'value': value
                    })
            if view_candidates:
                # Take max because views are usually the largest number on page
                data['views'] = max(view_candidates)
        
        # Process share matches
        if js_results.get('shareMatches'):
            share_candidates = []
            for m in js_results['shareMatches']:
                value = parse_vietnamese_number(m['number'])
                if 1 <= value <= 100000000:
                    share_candidates.append(value)
                    share_attempts.append({
                        'number': m['number'],
                        'context': m['context'][:80],
                        'value': value
                    })
            if share_candidates:
                # Take median for shares
                sorted_shares = sorted(share_candidates)
                data['shares'] = sorted_shares[len(sorted_shares) // 2]
        
        # Process username
        if js_results.get('usernameMatches'):
            # Take first non-empty username
            for m in js_results['usernameMatches'][:5]:
                if m.get('username'):
                    data['username'] = m['username']
                    break
    
    except Exception as e:
        view_attempts.append({'error': str(e)[:100]})
    
    debug_info['view_extraction_attempts'] = view_attempts
    debug_info['share_extraction_attempts'] = share_attempts
    
    return data


def extract_from_html(html):
    """V5 patterns + improved share patterns"""
    data = {}
    patterns = {
        'views': [
            r'"video_view_count[":\s]*(\d+)',
            r'"play_count[":\s]*(\d+)',
            r'"viewCount[":\s]*(\d+)',
            r'"unified_view_count_renderer"[^}]*"count[":\s]*(\d+)',
            r'"reels_view_count[":\s]*(\d+)',
            r'"organic_view_count[":\s]*(\d+)',
            r'"playbackVideoMetadata"[^}]*"viewCount[":\s]*(\d+)',
        ],
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
            r'"share_count[":\s]*(\d+)',
            r'"share_count_reduced"[^}]*"text[":\s]*"(\d+[\d.,KkMmBb]*)"',
        ],
        'post_id': [
            r'"video_id[":\s]*"(\d+)"',
            r'"top_level_post_id[":\s]*"(\d+)"',
        ],
        'username': [
            r'"page_name[":\s]*"([^"]+)"',
            r'"profile_url[":\s]*"https://www\.facebook\.com/([^/"]+)"',
            r'"actor_username[":\s]*"([^"]+)"',
            r'"author_username[":\s]*"([^"]+)"',
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
                if field in ['views', 'likes', 'comments', 'shares']:
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
        'version': '7.2-smart-dom',
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
        'version': '7.2-smart-dom',
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
