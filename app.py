"""
Rise City Facebook Scraper API - V7.5 🎩
Apply V7.3 strategy (search Vietnamese text) cho TẤT CẢ engagement metrics:
- views: search "X lượt xem" 
- likes: search "X người khác", "X người đã thích" -> +1 cho người đầu
- comments: search "X bình luận"
- shares: search "X lượt chia sẻ"
- username: blacklist FB system pages
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

# Blacklist FB system pages (not real usernames)
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
            'like_extraction_attempts': [],
            'comment_extraction_attempts': [],
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
            
            # SEARCH RAW HTML for ALL engagement metrics (V7.3 strategy works!)
            html_data = search_engagement_in_html(html, result['debug'])
            
            # DOM walker for username (with blacklist)
            dom_data = extract_username_from_dom(page)
            
            # Quick check Vietnamese keywords
            result['debug']['html_search_results'] = {
                'has_luot_xem': 'l\u01b0\u1ee3t xem' in html,
                'has_nguoi_khac': 'ng\u01b0\u1eddi kh\u00e1c' in html,
                'has_binh_luan': 'b\u00ecnh lu\u1eadn' in html,
                'has_luot_chia_se': 'l\u01b0\u1ee3t chia s\u1ebb' in html,
                'has_nguoi_khac_count': html.count('ng\u01b0\u1eddi kh\u00e1c'),
                'has_binh_luan_count': html.count('b\u00ecnh lu\u1eadn'),
            }
            
            # Fallback: extract metadata from HTML (caption, thumbnail, post_id)
            extracted = extract_metadata_from_html(html)
            result['debug']['extracted_data'] = extracted
            
            # Final values - prioritize HTML search (V7.3 strategy)
            result['data']['views'] = html_data.get('views', 0)
            result['data']['likes'] = html_data.get('likes', 0)
            result['data']['comments'] = html_data.get('comments', 0)
            result['data']['shares'] = html_data.get('shares', 0)
            result['data']['caption'] = decode_unicode_string(extracted.get('caption', ''))[:500]
            result['data']['thumbnail'] = extracted.get('thumbnail', '')
            result['data']['username'] = dom_data.get('username', '')
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


def search_engagement_in_html(html, debug_info):
    """
    Search RAW HTML for ALL engagement metrics using Vietnamese patterns.
    
    Patterns:
    - Views: "5,2K lượt xem"
    - Likes: "62 người khác" -> +1 for the named person = 63
    - Comments: "3 bình luận"
    - Shares: "4 lượt chia sẻ"
    """
    data = {'views': 0, 'likes': 0, 'comments': 0, 'shares': 0}
    
    view_attempts = []
    like_attempts = []
    comment_attempts = []
    share_attempts = []
    
    # === VIEWS ===
    view_patterns = [
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\\u01b0\\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u1ea7n\s*xem',
    ]
    view_candidates = []
    for pat in view_patterns:
        matches = re.findall(pat, html, re.IGNORECASE)
        for m in matches[:20]:
            value = parse_vietnamese_number(m)
            if 10 <= value <= 100000000:
                view_candidates.append(value)
                view_attempts.append({'match': str(m)[:30], 'value': value})
    if view_candidates:
        data['views'] = max(view_candidates)
    
    # === LIKES === 
    # Pattern: "62 người khác" → +1 for the named person = 63 likes
    # Also: "Trần Xuân Hậu và 62 người khác"
    like_patterns = [
        r'v\u00e0\s*([\d.,]+\s*[KkMmBb]?)\s*ng\u01b0\u1eddi\s*kh\u00e1c',
        r'v\\u00e0\s*([\d.,]+\s*[KkMmBb]?)\s*ng\\u01b0\\u1eddi\s*kh\\u00e1c',
        r'and\s*([\d.,]+\s*[KkMmBb]?)\s*others?',
    ]
    like_candidates_with_plus_one = []
    for pat in like_patterns:
        matches = re.findall(pat, html, re.IGNORECASE)
        for m in matches[:20]:
            value = parse_vietnamese_number(m)
            if 0 <= value <= 100000000:
                # +1 because there's also the named person
                total = value + 1
                like_candidates_with_plus_one.append(total)
                like_attempts.append({'match': str(m)[:30], 'value': value, 'total_with_named': total})
    
    # Fallback patterns: direct like count
    if not like_candidates_with_plus_one:
        fallback_like_patterns = [
            r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*th\u00edch',
            r'([\d.,]+\s*[KkMmBb]?)\s*ng\u01b0\u1eddi\s*\u0111\u00e3\s*th\u00edch',
            r'([\d.,]+\s*[KkMmBb]?)\s*reactions?',
        ]
        for pat in fallback_like_patterns:
            matches = re.findall(pat, html, re.IGNORECASE)
            for m in matches[:20]:
                value = parse_vietnamese_number(m)
                if 1 <= value <= 100000000:
                    like_candidates_with_plus_one.append(value)
                    like_attempts.append({'match': str(m)[:30], 'value': value, 'fallback': True})
    
    if like_candidates_with_plus_one:
        data['likes'] = max(like_candidates_with_plus_one)
    
    # === COMMENTS ===
    comment_patterns = [
        r'([\d.,]+\s*[KkMmBb]?)\s*b\u00ecnh\s*lu\u1eadn',
        r'([\d.,]+\s*[KkMmBb]?)\s*b\\u00ecnh\s*lu\\u1eadn',
        r'([\d.,]+\s*[KkMmBb]?)\s*comments?',
    ]
    comment_candidates = []
    for pat in comment_patterns:
        matches = re.findall(pat, html, re.IGNORECASE)
        for m in matches[:20]:
            value = parse_vietnamese_number(m)
            if 0 <= value <= 100000000:
                comment_candidates.append(value)
                comment_attempts.append({'match': str(m)[:30], 'value': value})
    if comment_candidates:
        # Take most common (mode-like) - smallest sensible value usually correct
        # Sort and take the value that appears most often
        from collections import Counter
        counter = Counter(comment_candidates)
        most_common = counter.most_common(1)[0][0]
        data['comments'] = most_common
    
    # === SHARES ===
    share_patterns = [
        r'([\d.,]+\s*[KkMmBb]?)\s*l\u01b0\u1ee3t\s*chia\s*s\u1ebb',
        r'([\d.,]+\s*[KkMmBb]?)\s*l\\u01b0\\u1ee3t\s*chia\s*s\\u1ebb',
        r'([\d.,]+\s*[KkMmBb]?)\s*shares?\b',
    ]
    share_candidates = []
    for pat in share_patterns:
        matches = re.findall(pat, html, re.IGNORECASE)
        for m in matches[:20]:
            value = parse_vietnamese_number(m)
            if 1 <= value <= 100000000:
                share_candidates.append(value)
                share_attempts.append({'match': str(m)[:30], 'value': value})
    if share_candidates:
        from collections import Counter
        counter = Counter(share_candidates)
        most_common = counter.most_common(1)[0][0]
        data['shares'] = most_common
    
    debug_info['view_extraction_attempts'] = view_attempts[:10]
    debug_info['like_extraction_attempts'] = like_attempts[:10]
    debug_info['comment_extraction_attempts'] = comment_attempts[:10]
    debug_info['share_extraction_attempts'] = share_attempts[:10]
    
    return data


def extract_username_from_dom(page):
    """Get username from profile link, with blacklist filter"""
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
                            results.push({
                                username: match[1],
                                displayName: text
                            });
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
    except Exception as e:
        pass
    
    return data


def extract_metadata_from_html(html):
    """Extract caption, thumbnail, post_id from HTML"""
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
        'version': '7.5-vietnamese-text',
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
        'version': '7.5-vietnamese-text',
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
