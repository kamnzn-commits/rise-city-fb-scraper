"""
Rise City Facebook Scraper API - V7.4 🎩
Combine V7.1 + V7.3:
- Wait 8s (V7.1 - lấy đúng likes/comments)
- Search HTML patterns (V7.3 - lấy đúng views)
- DOM walker cho shares (tìm "4 lượt chia sẻ")
- Username từ profile link
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
            
            # Wait 8s like V7.1 (better for likes/comments)
            time.sleep(8)
            
            # Light scroll
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
            
            # SEARCH RAW HTML for views (V7.3 method - works!)
            html_views, view_attempts = search_views_in_html(html)
            result['debug']['view_extraction_attempts'] = view_attempts
            
            # DOM walker for shares (search rendered DOM text)
            dom_data = extract_dom_for_shares(page, result['debug'])
            
            # Quick check Vietnamese keywords
            result['debug']['html_search_results'] = {
                'has_luot_xem': 'l\u01b0\u1ee3t xem' in html,
                'has_luot_chia_se': 'l\u01b0\u1ee3t chia s\u1ebb' in html,
                'has_luot_xem_count': html.count('l\u01b0\u1ee3t xem'),
                'has_luot_chia_se_count': html.count('l\u01b0\u1ee3t chia s\u1ebb'),
            }
            
            extracted = extract_from_html(html)
            result['debug']['extracted_data'] = extracted
            
            # Final values
            result['data']['views'] = max(html_views, extracted.get('views', 0))
            result['data']['likes'] = extracted.get('likes', 0)
            result['data']['comments'] = extracted.get('comments', 0)
            result['data']['shares'] = max(dom_data.get('shares', 0), extracted.get('shares', 0))
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


def search_views_in_html(html):
    """Search RAW HTML for view count - V7.3 method (works!)"""
    view_candidates = []
    view_attempts = []
    
    view_patterns = [
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\\u01b0\\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u1ea7n\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*views?\b',
    ]
    
    for pat in view_patterns:
        matches = re.findall(pat, html, re.IGNORECASE)
        for m in matches[:20]:
            value = parse_vietnamese_number(m)
            if 10 <= value <= 100000000:
                view_candidates.append(value)
                view_attempts.append({
                    'pattern': pat[:40],
                    'match': str(m)[:30],
                    'value': value
                })
    
    final_views = max(view_candidates) if view_candidates else 0
    return final_views, view_attempts


def extract_dom_for_shares(page, debug_info):
    """DOM walker for shares + username (text rendered by FB)"""
    data = {}
    share_attempts = []
    
    try:
        js_results = page.evaluate("""
            () => {
                const results = {
                    shareMatches: [],
                    usernameMatches: []
                };
                
                // Get all visible text from page
                const allText = document.body.innerText || '';
                
                // Match share patterns: "4 lượt chia sẻ", "10 chia sẻ"
                const sharePattern = /([\\d.,]+\\s*[KkMmBb]?)\\s*(l\u01b0\u1ee3t chia s\u1ebb|chia s\u1ebb|shares?)/gi;
                let shareMatch;
                while ((shareMatch = sharePattern.exec(allText)) !== null) {
                    results.shareMatches.push({
                        number: shareMatch[1].trim(),
                        context: shareMatch[0]
                    });
                }
                
                // Find username from profile links
                const profileLinks = document.querySelectorAll('a[href*="/"]');
                for (const link of profileLinks) {
                    const href = link.getAttribute('href') || '';
                    const match = href.match(/^https?:\\/\\/(?:www\\.|m\\.)?facebook\\.com\\/([a-zA-Z0-9.]+)(?:\\/|$|\\?)/);
                    if (match && !['reel', 'share', 'video', 'watch', 'photo', 'permalink', 'profile.php', 'login'].includes(match[1])) {
                        const text = (link.textContent || '').trim();
                        if (text && text.length > 1 && text.length < 100) {
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
        
        # Process shares
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
                # Take median (avoid outliers)
                sorted_shares = sorted(share_candidates)
                data['shares'] = sorted_shares[len(sorted_shares) // 2]
        
        # Username
        if js_results.get('usernameMatches'):
            for m in js_results['usernameMatches'][:5]:
                if m.get('username'):
                    data['username'] = m['username']
                    break
    
    except Exception as e:
        share_attempts.append({'error': str(e)[:100]})
    
    debug_info['share_extraction_attempts'] = share_attempts
    return data


def extract_from_html(html):
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
        ],
        'post_id': [
            r'"video_id[":\s]*"(\d+)"',
            r'"top_level_post_id[":\s]*"(\d+)"',
        ],
        'username': [
            r'"page_name[":\s]*"([^"]+)"',
            r'"profile_url[":\s]*"https://www\.facebook\.com/([^/"]+)"',
            r'"actor_username[":\s]*"([^"]+)"',
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
        'version': '7.4-combined',
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
        'version': '7.4-combined',
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
