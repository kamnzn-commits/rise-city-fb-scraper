"""
Rise City Facebook Scraper API - V3 với Playwright 🎩
Sử dụng Chromium + cookies để giả lập browser thật scrape Facebook
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
    text = str(text).strip().replace(',', '').replace('.', '')
    
    # Vietnamese number parsing (period as thousand separator)
    text_clean = re.sub(r'[^\d.,KMBkmbtr]', '', text)
    
    multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000, 'tr': 1000000, 'k': 1000}
    
    for suffix, mult in multipliers.items():
        if suffix in text_clean:
            num_str = text_clean.replace(suffix, '')
            try:
                return int(float(num_str) * mult)
            except:
                continue
    
    try:
        return int(re.sub(r'[^\d]', '', text_clean) or 0)
    except:
        return 0


def scrape_with_playwright(url):
    """Scrape Facebook video using Playwright + cookies"""
    
    cookies = parse_netscape_cookies(COOKIES_PATH)
    if not cookies:
        return {'error': 'No cookies loaded'}
    
    result = {
        'success': False,
        'data': {
            'views': 0, 'likes': 0, 'comments': 0, 'shares': 0,
            'caption': '', 'thumbnail': '', 'username': '', 'post_id': None
        },
        'debug': {
            'final_url': '',
            'page_title': '',
            'extracted_data': {},
        }
    }
    
    try:
        with sync_playwright() as p:
            # Launch with minimal memory footprint
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-extensions',
                    '--no-first-run',
                    '--disable-blink-features=AutomationControlled',
                    '--single-process',  # Save memory
                    '--no-zygote',
                ]
            )
            
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 720},
                locale='vi-VN',
            )
            
            # Inject cookies
            context.add_cookies(cookies)
            logger.info(f'Injected {len(cookies)} cookies')
            
            page = context.new_page()
            
            # Block unnecessary resources to save memory
            page.route('**/*', lambda route: route.abort() 
                      if route.request.resource_type in ['image', 'media', 'font', 'stylesheet']
                      else route.continue_())
            
            # Navigate
            logger.info(f'Navigating to: {url}')
            page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            # Wait for content to load
            time.sleep(5)
            
            final_url = page.url
            page_title = page.title()
            result['debug']['final_url'] = final_url
            result['debug']['page_title'] = page_title
            
            logger.info(f'Final URL: {final_url}')
            logger.info(f'Page title: {page_title}')
            
            # Try to get HTML and extract data
            html = page.content()
            
            # Extract from HTML using regex (FB embeds data in JSON)
            extracted = extract_from_html(html)
            result['debug']['extracted_data'] = extracted
            
            # Try to get visible counts from DOM
            dom_data = extract_from_dom(page)
            
            # Merge: prefer DOM data if available, fallback to HTML
            result['data']['views'] = dom_data.get('views') or extracted.get('views', 0)
            result['data']['likes'] = dom_data.get('likes') or extracted.get('likes', 0)
            result['data']['comments'] = dom_data.get('comments') or extracted.get('comments', 0)
            result['data']['shares'] = dom_data.get('shares') or extracted.get('shares', 0)
            result['data']['caption'] = dom_data.get('caption') or extracted.get('caption', '')[:500]
            result['data']['thumbnail'] = extracted.get('thumbnail', '')
            result['data']['username'] = extracted.get('username', '')
            result['data']['post_id'] = extracted.get('post_id')
            
            if (result['data']['views'] > 0 or result['data']['likes'] > 0 or
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
    
    # Patterns for various FB data
    patterns = {
        'views': [
            r'"video_view_count[":\s]*(\d+)',
            r'"video_view_count_renderer"[^}]*"count[":\s]*(\d+)',
            r'"play_count[":\s]*(\d+)',
            r'"viewCount[":\s]*(\d+)',
        ],
        'likes': [
            r'"reaction_count"[^}]*"count[":\s]*(\d+)',
            r'"top_reactions"[^}]*"count[":\s]*(\d+)',
            r'"likes_count[":\s]*(\d+)',
        ],
        'comments': [
            r'"total_comment_count[":\s]*(\d+)',
            r'"comment_count"[^}]*"total_count[":\s]*(\d+)',
        ],
        'shares': [
            r'"share_count"[^}]*"count[":\s]*(\d+)',
            r'"share_count_reduced[":\s]*"([^"]+)"',
        ],
        'post_id': [
            r'"top_level_post_id[":\s]*"(\d+)"',
            r'"video_id[":\s]*"(\d+)"',
        ],
        'username': [
            r'"page_name[":\s]*"([^"]+)"',
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
                    data[field] = parse_count(value)
                else:
                    data[field] = value.replace('\\u003C', '<').replace('\\/', '/')[:500]
                break
    
    return data


def extract_from_dom(page):
    """Extract visible counts from DOM"""
    data = {}
    
    try:
        # Get caption from meta or visible text
        try:
            desc = page.locator('meta[property="og:description"]').get_attribute('content', timeout=2000)
            if desc:
                data['caption'] = desc[:500]
        except:
            pass
        
        # Try to find view count in visible text
        try:
            # Vietnamese: "lượt xem", English: "views"
            view_patterns = [
                'text=/\\d[\\d.,]*[KkMmBb]?\\s*(views?|lượt xem|lần xem)/i',
            ]
            for pattern in view_patterns:
                elements = page.locator(pattern).all()
                for el in elements[:3]:
                    text = el.text_content()
                    if text:
                        match = re.search(r'(\d[\d.,]*[KkMmBb]?)', text)
                        if match:
                            data['views'] = parse_count(match.group(1))
                            break
                if data.get('views'):
                    break
        except:
            pass
    except Exception as e:
        logger.warning(f'DOM extract failed: {e}')
    
    return data


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'ok',
        'service': 'Rise City Facebook Scraper 🎩',
        'version': '3.0-playwright',
        'engine': 'Playwright + Chromium',
    })


@app.route('/health', methods=['GET'])
def health():
    cookies_exist = os.path.exists(COOKIES_PATH)
    cookies_count = len(parse_netscape_cookies(COOKIES_PATH)) if cookies_exist else 0
    
    return jsonify({
        'status': 'ok',
        'cookies_loaded': cookies_exist,
        'cookies_count': cookies_count,
        'version': '3.0-playwright',
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
    
    logger.info(f'Scraping with Playwright: {url}')
    result = scrape_with_playwright(url)
    return jsonify(result)


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
