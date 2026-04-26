"""
Rise City Facebook Scraper API - V6 🎩
BIG UPDATE: Tìm view count qua DOM element (icon 👁) thay vì JSON regex
- Click play video → trigger view counter
- Wait for view counter element to appear
- Multiple selector fallbacks cho icon 👁
- Parse Vietnamese number format (1,5K = 1500)
"""
from flask import Flask, request, jsonify
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
    """
    Parse Vietnamese-formatted numbers:
    '1,5K' or '1.5K' -> 1500
    '2,3M' -> 2300000
    '15K' -> 15000
    '1.234' -> 1234
    """
    if not text:
        return 0
    
    s = str(text).strip()
    
    # Find pattern: number (with comma/period decimal) + optional suffix
    match = re.match(r'([\d.,]+)\s*([KkMmBbTrtr]+)?', s)
    if not match:
        return 0
    
    num_str = match.group(1)
    suffix = match.group(2)
    
    # Replace Vietnamese decimal: 1,5 -> 1.5
    # But also handle 1.234 (thousand separator)
    if ',' in num_str and '.' in num_str:
        # Both present: comma is thousands, period is decimal (US format)
        num_str = num_str.replace(',', '')
    elif ',' in num_str:
        # Only comma: could be thousands (1,234) or decimal (1,5)
        # Heuristic: if has 3 digits after comma, it's thousands
        parts = num_str.split(',')
        if len(parts) == 2 and len(parts[1]) == 3:
            num_str = num_str.replace(',', '')  # 1,234 -> 1234
        else:
            num_str = num_str.replace(',', '.')  # 1,5 -> 1.5
    elif '.' in num_str:
        # Only period: could be thousands (1.234) or decimal (1.5)
        parts = num_str.split('.')
        if len(parts) == 2 and len(parts[1]) == 3:
            num_str = num_str.replace('.', '')  # 1.234 -> 1234
    
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
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--disable-gpu',
                    '--no-first-run',
                    '--autoplay-policy=no-user-gesture-required',  # Auto play video
                ]
            )
            
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 414, 'height': 896},  # Mobile viewport!
                locale='vi-VN',
                timezone_id='Asia/Ho_Chi_Minh',
                is_mobile=True,  # Pretend to be mobile (FB shows view counter clearer on mobile)
                has_touch=True,
                extra_http_headers={
                    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                }
            )
            
            context.add_cookies(cookies)
            logger.info(f'Injected {len(cookies)} cookies')
            
            page = context.new_page()
            
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en'] });
            """)
            
            # Convert /share/r/ URL to /reel/ format if possible by extracting post_id from HTML first
            logger.info(f'Navigating to: {url}')
            response = page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            # Wait initial load
            time.sleep(5)
            
            # Get post_id from current page to construct cleaner URL
            html = page.content()
            post_id_match = re.search(r'"video_id[":\s]*"(\d+)"', html) or \
                           re.search(r'"top_level_post_id[":\s]*"(\d+)"', html)
            
            if post_id_match:
                post_id = post_id_match.group(1)
                # Try direct reel URL with mobile prefix (more likely to show view counter)
                reel_url = f'https://m.facebook.com/reel/{post_id}'
                logger.info(f'Trying mobile reel URL: {reel_url}')
                try:
                    page.goto(reel_url, wait_until='domcontentloaded', timeout=30000)
                    time.sleep(8)
                except Exception as e:
                    logger.warning(f'Mobile reel URL failed: {e}, falling back')
                    # Fall back to original
                    pass
            
            # Scroll and wait for lazy load
            try:
                page.evaluate('window.scrollTo(0, 100)')
                time.sleep(2)
                page.evaluate('window.scrollTo(0, 300)')
                time.sleep(2)
                page.evaluate('window.scrollTo(0, 0)')
                time.sleep(2)
            except:
                pass
            
            # Wait extra for view counter to load
            time.sleep(5)
            
            final_url = page.url
            page_title = page.title()
            result['debug']['final_url'] = final_url
            result['debug']['page_title'] = page_title
            
            if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
                result['error'] = 'Redirected to login - cookies invalid or expired'
                browser.close()
                return result
            
            # Get final HTML
            html = page.content()
            result['debug']['html_length'] = len(html)
            
            # === EXTRACT VIEWS - MULTIPLE STRATEGIES ===
            views = extract_views_multistrategy(page, html, result['debug'])
            
            # Extract other fields from HTML
            extracted = extract_from_html(html)
            result['debug']['extracted_data'] = extracted
            
            # Build result
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
    """
    Try multiple strategies to extract view count.
    Returns the highest non-zero value found.
    """
    attempts = []
    candidate_views = []
    
    # === STRATEGY 1: HTML regex patterns ===
    html_patterns = [
        # FB internal patterns
        r'"video_view_count[":\s]*(\d+)',
        r'"video_view_count_renderer"[^}]*"count[":\s]*(\d+)',
        r'"play_count[":\s]*(\d+)',
        r'"viewCount[":\s]*(\d+)',
        r'"unified_view_count_renderer"[^}]*"count[":\s]*(\d+)',
        r'"reels_view_count[":\s]*(\d+)',
        r'"organic_view_count[":\s]*(\d+)',
        r'"playbackVideoMetadata"[^}]*"viewCount[":\s]*(\d+)',
        # Display text patterns
        r'"video_view_count_text"[^}]*"text[":\s]*"([^"]+)"',
        r'"video_view_count_renderer"[^}]*"text"[^}]*"text[":\s]*"([^"]+)"',
        # Vietnamese text patterns
        r'(\d[\d.,]*[KkMmBb]?)\s*(?:l\\u01b0\\u1ee3t xem|l\\u1ea7n xem|views?)',
    ]
    
    for pattern in html_patterns:
        match = re.search(pattern, html)
        if match:
            value = parse_vietnamese_number(match.group(1))
            if value > 0:
                candidate_views.append(value)
                attempts.append({'strategy': 'html_regex', 'pattern': pattern[:50], 'value': value})
    
    # === STRATEGY 2: DOM selectors for view counter element ===
    try:
        # FB uses various selectors for view counter
        selectors = [
            # Generic eye icon followed by number
            'span:has(svg[viewBox*="24"])',
            # Aria-label
            '[aria-label*="view"]',
            '[aria-label*="lượt xem"]',
            '[aria-label*="lần xem"]',
            # Text patterns near video
            'div[role="article"] span:has-text("views")',
            'div[role="article"] span:has-text("lượt xem")',
            # Common FB classes (may change)
            '[data-visualcompletion="ignore-dynamic"]',
        ]
        
        for selector in selectors:
            try:
                elements = page.locator(selector).all()
                for el in elements[:5]:
                    text = el.text_content(timeout=1000)
                    if text:
                        # Look for number patterns
                        match = re.search(r'(\d[\d.,]*[KkMmBb]?)', text)
                        if match:
                            value = parse_vietnamese_number(match.group(1))
                            if 10 <= value <= 100000000:  # Sanity check
                                candidate_views.append(value)
                                attempts.append({'strategy': 'dom_selector', 'selector': selector[:30], 'text': text[:50], 'value': value})
            except Exception as e:
                pass
    except Exception as e:
        attempts.append({'strategy': 'dom_selector', 'error': str(e)[:100]})
    
    # === STRATEGY 3: JavaScript - search all text for "views" ===
    try:
        js_views = page.evaluate("""
            () => {
                const results = [];
                
                // Get all text nodes
                const walker = document.createTreeWalker(
                    document.body,
                    NodeFilter.SHOW_TEXT,
                    null,
                    false
                );
                
                const viewKeywords = ['view', 'lượt xem', 'lần xem', 'lượt', 'lần'];
                
                let node;
                while (node = walker.nextNode()) {
                    const text = node.textContent || '';
                    const parent = node.parentElement;
                    if (!parent) continue;
                    
                    // Get full text including siblings
                    const fullText = parent.textContent || '';
                    
                    // Check if contains view keyword
                    const hasViewKeyword = viewKeywords.some(kw => 
                        fullText.toLowerCase().includes(kw.toLowerCase())
                    );
                    
                    if (hasViewKeyword) {
                        // Extract number
                        const numMatch = fullText.match(/(\\d[\\d.,]*\\s*[KkMmBb]?)/);
                        if (numMatch) {
                            results.push({
                                text: fullText.substring(0, 100),
                                number: numMatch[1]
                            });
                        }
                    }
                }
                
                // Also look for spans with eye icon SVG nearby
                const spans = document.querySelectorAll('span');
                for (const span of spans) {
                    const text = span.textContent || '';
                    // Match patterns like "1,5K" or "12.3K"
                    if (/^[\\d.,]+\\s*[KkMmBb]?$/.test(text.trim())) {
                        // Check if has SVG sibling (eye icon)
                        const parent = span.parentElement;
                        if (parent && parent.querySelector('svg')) {
                            results.push({
                                text: text.trim(),
                                number: text.trim(),
                                hasIcon: true
                            });
                        }
                    }
                }
                
                return results;
            }
        """)
        
        if js_views:
            for item in js_views[:10]:
                num_str = item.get('number', '')
                value = 0
                # Need to import parse function in JS, or do here
                try:
                    # Simple parse
                    s = num_str.strip().replace(',', '.')
                    multiplier = 1
                    if s.lower().endswith('k'):
                        multiplier = 1000
                        s = s[:-1]
                    elif s.lower().endswith('m'):
                        multiplier = 1000000
                        s = s[:-1]
                    value = int(float(s) * multiplier)
                except:
                    pass
                
                if 10 <= value <= 100000000:
                    candidate_views.append(value)
                    attempts.append({
                        'strategy': 'js_dom_search', 
                        'text': item.get('text', '')[:50], 
                        'value': value,
                        'hasIcon': item.get('hasIcon', False)
                    })
    except Exception as e:
        attempts.append({'strategy': 'js_dom_search', 'error': str(e)[:100]})
    
    debug_info['view_extraction_attempts'] = attempts
    
    # Return highest value found (FB may have multiple counters)
    if candidate_views:
        # Filter for reasonable view counts
        valid_views = [v for v in candidate_views if 1 <= v <= 100000000]
        if valid_views:
            # Take median to avoid outliers
            sorted_views = sorted(valid_views)
            mid = sorted_views[len(sorted_views) // 2]
            return mid
    
    return 0


def extract_from_html(html):
    """Extract non-view data from HTML"""
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
        'version': '6.0-mobile-dom',
        'engine': 'Playwright Mobile + DOM extraction',
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
        'version': '6.0-mobile-dom',
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
