"""
Rise City Facebook Scraper API - V5 Production Ready 🎩
Cải tiến V5:
- Decode Unicode caption (B\\u1ecb -> Bị)
- More patterns for views, username, shares
- Cleaner DOM extraction
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
    """Decode \\uXXXX escape sequences to actual unicode"""
    if not s:
        return s
    try:
        # Decode \uXXXX sequences
        return s.encode('utf-8').decode('unicode_escape').encode('latin-1').decode('utf-8')
    except:
        try:
            return bytes(s, 'utf-8').decode('unicode_escape')
        except:
            return s


def parse_netscape_cookies(cookies_path):
    """Parse Netscape cookies.txt to Playwright format"""
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
                'name': name,
                'value': value,
                'domain': domain,
                'path': path,
                'secure': secure.upper() == 'TRUE',
                'httpOnly': False,
                'sameSite': 'Lax',
            })
    return cookies


def parse_count(text):
    """Parse '1.2K', '5M', '1,234' to integer"""
    if not text:
        return 0
    s = str(text).strip().replace(',', '').replace(' ', '')
    
    multipliers = {
        'K': 1000, 'k': 1000, 'M': 1000000, 'm': 1000000,
        'B': 1000000000, 'b': 1000000000,
        'tr': 1000000, 'TR': 1000000,
    }
    
    for suffix, mult in multipliers.items():
        if suffix in s:
            num_part = re.sub(r'[^\d.]', '', s.replace(suffix, ''))
            try:
                return int(float(num_part) * mult)
            except:
                continue
    
    try:
        return int(re.sub(r'[^\d]', '', s) or 0)
    except:
        return 0


def scrape_with_playwright(url):
    """Scrape Facebook video using Playwright + cookies"""
    
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
            logger.info(f'Injected {len(cookies)} cookies')
            
            page = context.new_page()
            
            # Anti-detection
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['vi-VN', 'vi', 'en'] });
            """)
            
            logger.info(f'Navigating to: {url}')
            response = page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            time.sleep(8)
            
            try:
                page.evaluate('window.scrollTo(0, 500)')
                time.sleep(2)
                page.evaluate('window.scrollTo(0, 0)')
                time.sleep(1)
            except:
                pass
            
            final_url = page.url
            page_title = page.title()
            result['debug']['final_url'] = final_url
            result['debug']['page_title'] = page_title
            result['debug']['response_status'] = response.status if response else None
            
            logger.info(f'Final URL: {final_url}')
            
            if 'login' in final_url.lower() or 'Log into Facebook' in page_title:
                result['error'] = 'Redirected to login - cookies invalid or expired'
                browser.close()
                return result
            
            html = page.content()
            result['debug']['html_length'] = len(html)
            
            extracted = extract_from_html(html)
            result['debug']['extracted_data'] = extracted
            
            dom_data = extract_from_dom(page)
            result['debug']['dom_data'] = dom_data
            
            # Merge - prefer non-zero values
            result['data']['views'] = max(
                dom_data.get('views', 0),
                extracted.get('views', 0)
            )
            result['data']['likes'] = max(
                dom_data.get('likes', 0),
                extracted.get('likes', 0)
            )
            result['data']['comments'] = max(
                dom_data.get('comments', 0),
                extracted.get('comments', 0)
            )
            result['data']['shares'] = max(
                dom_data.get('shares', 0),
                extracted.get('shares', 0)
            )
            
            # Decode caption from \uXXXX format
            raw_caption = dom_data.get('caption') or extracted.get('caption', '')
            result['data']['caption'] = decode_unicode_string(raw_caption)[:500]
            
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


def extract_from_html(html):
    """Extract data from HTML using regex patterns"""
    data = {}
    
    patterns = {
        'views': [
            # Standard FB patterns
            r'"video_view_count[":\s]*(\d+)',
            r'"video_view_count_renderer"[^}]*"count[":\s]*(\d+)',
            r'"play_count[":\s]*(\d+)',
            r'"viewCount[":\s]*(\d+)',
            r'"unified_view_count_renderer"[^}]*"count[":\s]*(\d+)',
            # Reels-specific patterns 2026
            r'"reels_view_count[":\s]*(\d+)',
            r'"reels_video_count[":\s]*(\d+)',
            r'"organic_view_count[":\s]*(\d+)',
            r'"video_view_count_renderer"[^}]*"video_view_count[":\s]*(\d+)',
            # Display text patterns
            r'"video_view_count_text"[^}]*"text[":\s]*"([^"]+)"',
            # FB GraphQL response
            r'"playbackVideoMetadata"[^}]*"viewCount[":\s]*(\d+)',
        ],
        'likes': [
            r'"reaction_count"[^}]*"count[":\s]*(\d+)',
            r'"top_reactions"[^}]*"count[":\s]*(\d+)',
            r'"likers"[^}]*"count[":\s]*(\d+)',
            r'"likes_count[":\s]*(\d+)',
            r'"reactors"[^}]*"count[":\s]*(\d+)',
        ],
        'comments': [
            r'"total_comment_count[":\s]*(\d+)',
            r'"comment_count"[^}]*"total_count[":\s]*(\d+)',
            r'"comments_count_summary_renderer"[^}]*"count[":\s]*(\d+)',
            r'"comment_count_total[":\s]*(\d+)',
        ],
        'shares': [
            r'"share_count"[^}]*"count[":\s]*(\d+)',
            r'"share_count_reduced[":\s]*"([^"]+)"',
            r'"reshare_count[":\s]*(\d+)',
            r'"share_count_total[":\s]*(\d+)',
        ],
        'post_id': [
            r'"top_level_post_id[":\s]*"(\d+)"',
            r'"video_id[":\s]*"(\d+)"',
        ],
        'username': [
            # Multiple patterns to catch username
            r'"page_name[":\s]*"([^"]+)"',
            r'"author_username[":\s]*"([^"]+)"',
            r'"actor_username[":\s]*"([^"]+)"',
            r'"username[":\s]*"([^"]+)"',
            r'"profile_url[":\s]*"https://www\.facebook\.com/([^/"]+)"',
            r'"vanity[":\s]*"([^"]+)"',
            r'"actor_name[":\s]*"([^"]+)"',
            r'<meta\s+property="profile:username"\s+content="([^"]+)"',
            r'"name[":\s]*"([^"]+)"[^}]*"is_verified',
        ],
        'caption': [
            r'"message[":\s]*\{[^}]*"text[":\s]*"([^"]+)"',
            r'<meta\s+property="og:description"\s+content="([^"]+)"',
            r'"text[":\s]*"([^"]+)"[^}]*"ranges',
        ],
        'thumbnail': [
            r'<meta\s+property="og:image"\s+content="([^"]+)"',
            r'"first_frame_thumbnail[":\s]*"([^"]+)"',
            r'"thumbnailImage[":\s]*\{[^}]*"uri[":\s]*"([^"]+)"',
        ]
    }
    
    for field, pats in patterns.items():
        for pat in pats:
            match = re.search(pat, html)
            if match:
                value = match.group(1)
                if field in ['views', 'likes', 'comments', 'shares']:
                    parsed = parse_count(value)
                    if parsed > 0:  # Only accept if > 0
                        data[field] = parsed
                        break
                else:
                    cleaned = value.replace('\\u003C', '<').replace('\\/', '/').replace('\\u0026', '&')
                    if cleaned:
                        data[field] = cleaned[:500]
                        break
    
    return data


def extract_from_dom(page):
    """Extract data from DOM using JavaScript"""
    data = {}
    
    try:
        # Try meta tags first
        try:
            desc = page.locator('meta[property="og:description"]').first.get_attribute('content', timeout=3000)
            if desc:
                data['caption'] = desc[:500]
        except:
            pass
        
        # JavaScript extraction
        try:
            js_data = page.evaluate("""
                () => {
                    const result = {};
                    
                    // Search all script tags for FB data
                    const scripts = document.querySelectorAll('script');
                    for (const script of scripts) {
                        const text = script.textContent || '';
                        
                        // Views
                        if (!result.views) {
                            const m = text.match(/"video_view_count[":\\\\s]*(\\\\d+)/);
                            if (m) result.views = parseInt(m[1]);
                        }
                        if (!result.views) {
                            const m = text.match(/"play_count[":\\\\s]*(\\\\d+)/);
                            if (m) result.views = parseInt(m[1]);
                        }
                        
                        // Likes
                        if (!result.likes) {
                            const m = text.match(/"reaction_count"[^}]*"count[":\\\\s]*(\\\\d+)/);
                            if (m) result.likes = parseInt(m[1]);
                        }
                        
                        // Comments
                        if (!result.comments) {
                            const m = text.match(/"total_comment_count[":\\\\s]*(\\\\d+)/);
                            if (m) result.comments = parseInt(m[1]);
                        }
                        
                        // Shares  
                        if (!result.shares) {
                            const m = text.match(/"share_count"[^}]*"count[":\\\\s]*(\\\\d+)/);
                            if (m) result.shares = parseInt(m[1]);
                        }
                        
                        // Username
                        if (!result.username) {
                            const m = text.match(/"page_name[":\\\\s]*"([^"]+)"/);
                            if (m) result.username = m[1];
                        }
                    }
                    
                    return result;
                }
            """)
            if js_data:
                data.update({k: v for k, v in js_data.items() if v})
        except Exception as e:
            logger.warning(f'JS extract failed: {e}')
        
    except Exception as e:
        logger.warning(f'DOM extract failed: {e}')
    
    return data


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'ok',
        'service': 'Rise City Facebook Scraper 🎩',
        'version': '5.0-production',
        'engine': 'Playwright + Chromium + Anti-detection',
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
        'version': '5.0-production',
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
