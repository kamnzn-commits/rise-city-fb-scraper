"""
Rise City Facebook Scraper API - V8.6 🎩
ROOT CAUSE FIX: Map icon→field by Unicode codepoint, NOT pattern matching.

Discovery:
- U+F0378 (󰍸) = Like icon
- U+F0379 (󰍹) = Comment icon
- U+F037A (󰍺) = Share icon
- U+F174D (󱝍) = Eye icon (view)

When engagement = 0, FB shows ICON without number!
Pattern parsing fails because:
  󰍸\n󰍹\n6\n󰍺  ← like icon, comment icon, "6" (comments), share icon
  
V7.8/V8.1/V8.3 takes [6, 11, 1] as [likes, comments, shares] but actually:
- "6" = comments of reel 1
- "11" = LIKES of reel 2 (different video!)
- "1" = comments of reel 2

V8.6 Strategy:
1. Find FIRST occurrence of each icon
2. Number AFTER icon = value for that field
3. If NO number after icon → field = 0
4. Block boundary: when icon SEQUENCE restarts (icon already seen) → next reel
5. Eye icon has different parsing
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

# FB private use area icons
ICON_LIKE = '\U000F0378'      # 󰍸
ICON_COMMENT = '\U000F0379'   # 󰍹
ICON_SHARE = '\U000F037A'     # 󰍺
ICON_EYE = '\U000F174D'       # 󱝍 (view count)

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
    
    # Handle "X triệu" format
    if 'tri\u1ec7u' in s:
        # "1,3 triệu" -> 1.3 * 1M = 1,300,000
        num_match = re.match(r'([\d.,]+)', s)
        if num_match:
            num_str = num_match.group(1).replace(',', '.')
            try:
                return int(float(num_str) * 1000000)
            except:
                return 0
    
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


def parse_engagement_v83(innertext, debug_info):
    """
    V8.6: Icon-specific parsing.
    
    Strategy:
    1. Tokenize innertext into icons and numbers (with positions)
    2. Walk through tokens, identifying "blocks" (each reel)
    3. Take FIRST block's icons (the target video)
    4. For each icon, get number that follows IMMEDIATELY
    5. If next token is another ICON (not number), the value = 0
    """
    result = {'likes': 0, 'comments': 0, 'shares': 0, 'views': 0}
    
    if not innertext:
        return result
    
    debug_info['parse_v83'] = {'tokens': [], 'blocks': []}
    
    # ============================================
    # STEP 1: Tokenize innertext
    # ============================================
    # Each line, classify as: icon, number, or text
    lines = innertext.split('\n')
    tokens = []
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        if not line_stripped:
            continue
        
        # Check if line is a number FIRST (before icon check)
        # because numbers like "6" also have len=1
        num_match = re.match(r'^([\d.,]+\s*[KkMmBb]?(?:\s*tri\u1ec7u)?)$', line_stripped)
        if num_match:
            value = parse_vietnamese_number(num_match.group(1))
            tokens.append({
                'type': 'number',
                'value': value,
                'raw': line_stripped,
                'line': i,
            })
            continue
        
        # Check if line is single icon (after number check)
        if len(line_stripped) <= 2:
            for ch in line_stripped:
                cp = ord(ch)
                if 0xF0000 <= cp <= 0xFFFFD:
                    icon_type = None
                    if ch == ICON_LIKE:
                        icon_type = 'like'
                    elif ch == ICON_COMMENT:
                        icon_type = 'comment'
                    elif ch == ICON_SHARE:
                        icon_type = 'share'
                    elif ch == ICON_EYE:
                        icon_type = 'eye'
                    else:
                        icon_type = 'other_icon'
                    
                    tokens.append({
                        'type': 'icon',
                        'icon_type': icon_type,
                        'raw': ch,
                        'codepoint': f'U+{cp:05X}',
                        'line': i,
                    })
                    break
            continue
        
        # Otherwise it's text
        tokens.append({
            'type': 'text',
            'raw': line_stripped[:50],
            'line': i,
        })
    
    debug_info['parse_v83']['tokens_count'] = len(tokens)
    debug_info['parse_v83']['tokens_first_30'] = [
        {'t': t['type'], 'r': t.get('icon_type', '') or t.get('raw', '')[:30] or str(t.get('value', ''))}
        for t in tokens[:30]
    ]
    
    # ============================================
    # STEP 2: Find FIRST reel block
    # A reel block starts with first like/comment/share icon
    # Block ends when we see another like icon (next reel) OR text "Watch more"
    # ============================================
    
    first_reel_tokens = []
    seen_like = False
    seen_share = False
    
    for tok in tokens:
        # Stop conditions
        if tok['type'] == 'text':
            text = tok['raw'].lower()
            if 'watch more' in text or 'kh\u00e1m ph\u00e1' in text or 'tr\u1ed1ng' in text:
                break
        
        # If we already have likes+comments+shares of reel 1, stop
        if tok['type'] == 'icon' and tok.get('icon_type') == 'like' and seen_like and seen_share:
            # This is start of reel 2
            break
        
        if tok['type'] == 'icon' and tok.get('icon_type') == 'like':
            seen_like = True
        if tok['type'] == 'icon' and tok.get('icon_type') == 'share':
            seen_share = True
        
        first_reel_tokens.append(tok)
    
    debug_info['parse_v83']['first_reel_tokens_count'] = len(first_reel_tokens)
    debug_info['parse_v83']['first_reel_summary'] = [
        f"{t['type']}:{t.get('icon_type', '') or t.get('value', '') or t.get('raw', '')[:20]}"
        for t in first_reel_tokens
    ]
    
    # ============================================
    # STEP 3: Smart mapping - icon followed by number assigns to that icon
    # When icon followed by ANOTHER icon, that field = 0 (FB hides 0 values)
    # ============================================
    
    # First, identify positions of each icon type
    icon_positions = {'like': [], 'comment': [], 'share': [], 'eye': []}
    for i, tok in enumerate(first_reel_tokens):
        if tok['type'] == 'icon':
            it = tok.get('icon_type')
            if it in icon_positions:
                icon_positions[it].append(i)
    
    debug_info['parse_v83']['icon_positions'] = icon_positions
    
    # Map icon -> number that follows directly
    for icon_type in ['like', 'comment', 'share', 'eye']:
        if not icon_positions[icon_type]:
            continue
        
        idx = icon_positions[icon_type][0]  # First occurrence
        
        # Look at next token
        if idx + 1 < len(first_reel_tokens):
            next_tok = first_reel_tokens[idx + 1]
            if next_tok['type'] == 'number':
                value = next_tok['value']
                if icon_type == 'like' and 0 <= value < 1000000000:
                    result['likes'] = value
                elif icon_type == 'comment' and 0 <= value < 1000000000:
                    result['comments'] = value
                elif icon_type == 'share' and 0 <= value < 1000000000:
                    result['shares'] = value
                elif icon_type == 'eye' and 0 <= value < 10000000000:
                    result['views'] = value
    
    # SPECIAL CASE: Icons grouped without numbers, then numbers come later
    # Pattern: icon_like, icon_comment, NUMBER(N1), icon_share
    # → likes=0, comments=N1, shares=0 (because share has no number after)
    
    # If all 3 main icons are 0 but we have numbers in tokens, try grouped pattern
    if (result['likes'] == 0 and result['comments'] == 0 and result['shares'] == 0):
        # Find all icons in order, then all numbers in order in first block
        icons_seq = []
        numbers_seq = []
        for tok in first_reel_tokens:
            if tok['type'] == 'icon' and tok.get('icon_type') in ('like', 'comment', 'share'):
                icons_seq.append(tok.get('icon_type'))
            elif tok['type'] == 'number' and 0 <= tok['value'] < 1000000000:
                numbers_seq.append(tok['value'])
        
        debug_info['parse_v83']['grouped_icons'] = icons_seq
        debug_info['parse_v83']['grouped_numbers'] = numbers_seq
        
        # If we have 1-3 numbers and exactly 3 icons (like, comment, share)
        if icons_seq == ['like', 'comment', 'share'] or icons_seq[:3] == ['like', 'comment', 'share']:
            # FB hides numbers for 0 values
            if len(numbers_seq) == 3:
                result['likes'] = numbers_seq[0]
                result['comments'] = numbers_seq[1]
                result['shares'] = numbers_seq[2]
            elif len(numbers_seq) == 2:
                # Likes=0 hidden, take 2 numbers as comments+shares
                result['likes'] = 0
                result['comments'] = numbers_seq[0]
                result['shares'] = numbers_seq[1]
            elif len(numbers_seq) == 1:
                # Only 1 number visible → assign to comments
                result['likes'] = 0
                result['comments'] = numbers_seq[0]
                result['shares'] = 0
    
    debug_info['parse_v83']['parsed_result'] = result.copy()
    
    # ============================================
    # STEP 4: Eye icon view count - ONLY from first reel block
    # Don't use eye icons from "Watch more reels" section (other reels)
    # ============================================
    # Already handled in STEP 3 via icon_positions['eye']
    # If eye icon is NOT in first_reel_tokens, views stays 0
    # (We do NOT fall back to other reels' view counts)
    
    return result


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


def search_views_in_text(text):
    """Search Vietnamese view patterns + JSON patterns + aria-label"""
    candidates = []
    
    # Vietnamese natural language patterns
    patterns = [
        r'([\d.,]+\s*[KkMmBb]?(?:\s*ngh\u00ecn|\s*tri\u1ec7u)?)\s*l\u01b0\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?(?:\s*ngh\u00ecn|\s*tri\u1ec7u)?)\s*l\\u01b0\\u1ee3t\s*xem',
        r'([\d.,]+\s*[KkMmBb]?(?:\s*ngh\u00ecn|\s*tri\u1ec7u)?)\s*l\u1ea7n\s*xem',
        r'([\d.,]+\s*[KkMmBb]?(?:\s*ngh\u00ecn|\s*tri\u1ec7u)?)\s*views?\b',
    ]
    
    for pat in patterns:
        matches = re.findall(pat, text, re.IGNORECASE)
        for m in matches[:30]:
            value = parse_vietnamese_number(m)
            if 10 <= value <= 1000000000:
                candidates.append(value)
    
    # JSON patterns (server-rendered data)
    json_patterns = [
        r'"video_view_count"\s*:\s*(\d+)',
        r'"play_count"\s*:\s*(\d+)',
        r'"viewCount"\s*:\s*(\d+)',
        r'"reels_view_count"\s*:\s*(\d+)',
        r'"organic_view_count"\s*:\s*(\d+)',
        r'"post_view_count"\s*:\s*(\d+)',
        r'"feedback_video_view_count"\s*:\s*(\d+)',
        r'"reels_video_view_count"\s*:\s*(\d+)',
        r'"reel_view_count"\s*:\s*(\d+)',
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
    
    # ARIA-LABEL patterns (UI accessibility)
    # FB uses aria-label for view counter that's visually shown
    aria_patterns = [
        r'aria-label="([\d.,]+\s*[KkMmBb]?(?:\s*ngh\u00ecn|\s*tri\u1ec7u)?)\s*l\u01b0\u1ee3t\s*xem"',
        r'aria-label="([\d.,]+\s*[KkMmBb]?(?:\s*ngh\u00ecn|\s*tri\u1ec7u)?)\s*views?"',
    ]
    
    for pat in aria_patterns:
        matches = re.findall(pat, text, re.IGNORECASE)
        for m in matches[:20]:
            value = parse_vietnamese_number(m)
            if 10 <= value <= 1000000000:
                candidates.append(value)
    
    return max(candidates) if candidates else 0


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
                ]
            )
            
            views_desktop = try_desktop_with_cookies(browser, url, cookies, result)
            result['debug']['view_sources']['desktop_cookies'] = views_desktop
            
            try_mobile_mode_v83(browser, url, cookies, result)
            
            browser.close()
            
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
    """Desktop with cookies for caption + metadata + sometimes views from HTML"""
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
        
        response = page.goto(url, wait_until='domcontentloaded', timeout=45000)
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


def try_mobile_mode_v83(browser, url, cookies, result):
    """V8.6 Mobile mode with API response capture + force play to load views"""
    try:
        result['debug']['tried_modes'].append('mobile_v86')
        
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
        
        # V8.6: CAPTURE network responses to find view count from API calls
        captured_api_responses = []
        def handle_response(response):
            try:
                if response.status == 200:
                    url_lower = response.url.lower()
                    if any(k in url_lower for k in ['graphql', 'api/graphql', 'reel', 'video']):
                        try:
                            body = response.text()
                            if body and 100 < len(body) < 5000000:
                                if any(k in body for k in ['view_count', 'play_count', 'viewCount', 
                                                             'playCount', 'reel_view', 'l\u01b0\u1ee3t xem']):
                                    captured_api_responses.append(body)
                        except:
                            pass
            except:
                pass
        page.on('response', handle_response)
        
        response = page.goto(mobile_url, wait_until='domcontentloaded', timeout=45000)
        time.sleep(random.uniform(3, 4))
        
        # V8.6: Force video play to trigger view counter API
        try:
            page.evaluate("""
                () => {
                    const videos = document.querySelectorAll('video');
                    videos.forEach(v => {
                        try {
                            v.muted = true;
                            v.autoplay = true;
                            const playPromise = v.play();
                            if (playPromise) playPromise.catch(() => {});
                        } catch(e) {}
                    });
                    const videoContainer = document.querySelector('[role="article"], [data-pagelet*="reel" i]');
                    if (videoContainer) videoContainer.click();
                }
            """)
            time.sleep(2)
        except:
            pass
        
        for offset in [200, 400, 600]:
            try:
                page.evaluate(f'window.scrollTo(0, {offset})')
                time.sleep(random.uniform(0.8, 1.2))
            except:
                pass
        time.sleep(3)
        
        result['debug']['captured_api_count'] = len(captured_api_responses)
        
        mobile_innertext = ''
        try:
            mobile_innertext = page.evaluate('document.body.innerText || ""')
            result['debug']['mobile_innertext_length'] = len(mobile_innertext)
            result['debug']['mobile_innertext_sample'] = mobile_innertext[:1500]
        except:
            pass
        
        # V8.6: Search captured API responses for target video views
        target_post_id = result['data'].get('post_id', '')
        api_views = 0
        api_views_source = None
        
        for body in captured_api_responses:
            if target_post_id and target_post_id in body:
                view_patterns = [
                    r'"video_view_count"\s*:\s*(\d+)',
                    r'"play_count"\s*:\s*(\d+)',
                    r'"reels_video_view_count"\s*:\s*(\d+)',
                    r'"viewCount"\s*:\s*(\d+)',
                    r'"playCount"\s*:\s*(\d+)',
                ]
                for pat in view_patterns:
                    matches = re.findall(pat, body)
                    for m in matches:
                        try:
                            val = int(m)
                            if 10 <= val <= 1000000000:
                                if val > api_views:
                                    api_views = val
                                    api_views_source = pat[:30]
                        except:
                            pass
                if api_views > 0:
                    break
        
        if api_views > 0:
            result['data']['views'] = api_views
            result['debug']['view_sources']['api_capture'] = api_views
            result['debug']['api_views_pattern'] = api_views_source
        
        result['debug']['api_responses_summary'] = [
            {'length': len(r), 'has_target_id': target_post_id in r if target_post_id else False}
            for r in captured_api_responses[:5]
        ]
        
        # V8.6: Query DOM for view counter via JavaScript with USERNAME FILTER
        # Aria-labels contain "Xem video thước phim của {username} có {views} lượt xem"
        # We need to MATCH username from caption/owner to filter target video
        try:
            # Get username/owner from page
            page_username = ''
            try:
                # Try to get from URL or DOM
                page_username = page.evaluate("""
                    () => {
                        // Try multiple sources
                        // 1. og:title meta
                        const ogTitle = document.querySelector('meta[property="og:title"]')?.getAttribute('content') || '';
                        // 2. First profile link in page
                        const profileLink = document.querySelector('a[href*="facebook.com/"]')?.textContent || '';
                        return ogTitle + '||' + profileLink;
                    }
                """)
            except:
                pass
            result['debug']['page_username_hint'] = page_username[:200]
            
            view_count_dom = page.evaluate("""
                () => {
                    const results = [];
                    
                    // Strategy 1: aria-label with view counts
                    // Format: "Xem video thước phim của {USERNAME} có {N} lượt xem"
                    // Or: "Video has X views"
                    const elementsWithAria = document.querySelectorAll('[aria-label*="\u01b0\u1ee3t xem" i], [aria-label*="views" i]');
                    elementsWithAria.forEach(el => {
                        const label = el.getAttribute('aria-label') || '';
                        results.push({source: 'aria-label', text: label, html_class: el.className?.toString()?.substring(0, 80) || ''});
                    });
                    
                    // Strategy 2: Find video element and inspect siblings/parents
                    const videoElements = document.querySelectorAll('video');
                    for (const video of videoElements) {
                        // Walk up parents looking for view count text
                        let parent = video.parentElement;
                        let depth = 0;
                        while (parent && depth < 6) {
                            const ariaLabel = parent.getAttribute('aria-label') || '';
                            if (ariaLabel.includes('\u01b0\u1ee3t xem') || ariaLabel.toLowerCase().includes('view')) {
                                results.push({source: 'video-parent-aria', text: ariaLabel, depth: depth});
                            }
                            // Check title attribute
                            const title = parent.getAttribute('title') || '';
                            if (title.includes('\u01b0\u1ee3t xem')) {
                                results.push({source: 'video-parent-title', text: title, depth: depth});
                            }
                            parent = parent.parentElement;
                            depth++;
                        }
                    }
                    
                    // Strategy 3: Find scripts containing reel data (inline JSON)
                    const scripts = document.querySelectorAll('script');
                    for (const script of scripts) {
                        const content = script.textContent || '';
                        if (content.length > 500000) continue; // skip huge scripts
                        
                        // Look for view count patterns in inline JSON
                        const patterns = [
                            /"video_view_count"\\s*:\\s*(\\d+)/g,
                            /"play_count"\\s*:\\s*(\\d+)/g,
                            /"reels_video_view_count"\\s*:\\s*(\\d+)/g,
                            /"viewCount"\\s*:\\s*(\\d+)/g,
                            /"playCount"\\s*:\\s*(\\d+)/g,
                            /"organic_view_count_v2"\\s*:\\s*(\\d+)/g,
                        ];
                        
                        for (const pattern of patterns) {
                            const matches = [...content.matchAll(pattern)];
                            for (const m of matches.slice(0, 5)) {
                                const val = parseInt(m[1]);
                                if (val >= 10 && val <= 1000000000) {
                                    results.push({source: 'inline-json', text: String(val), pattern: m[0].substring(0, 50)});
                                }
                            }
                        }
                    }
                    
                    return results;
                }
            """)
            result['debug']['view_dom_candidates'] = view_count_dom[:30]
            
            # Filter aria-labels to find target video
            # Match by username from caption, or by first non-related-reels match
            target_video_views = 0
            
            # Get caption first chars to match username
            caption_lower = result['data']['caption'].lower() if result['data']['caption'] else ''
            
            # Get the username from page if available  
            target_usernames = []
            if page_username:
                # Extract names that look like usernames
                names = re.findall(r'([\w\u00C0-\u1EF9]+)', page_username)
                target_usernames = [n.strip() for n in names if len(n.strip()) > 2][:5]
            result['debug']['target_username_candidates'] = target_usernames
            
            # Process aria-label candidates
            aria_candidates = [c for c in view_count_dom if c.get('source') == 'aria-label']
            
            # Strategy A: inline-json values (highest confidence)
            json_candidates = [c for c in view_count_dom if c.get('source') == 'inline-json']
            if json_candidates:
                # Take the first reasonable JSON value
                for c in json_candidates:
                    val = parse_vietnamese_number(c.get('text', ''))
                    if 10 <= val <= 1000000000:
                        target_video_views = val
                        result['debug']['view_sources']['inline_json'] = val
                        break
            
            # Strategy B: video-parent-aria (likely target video)
            if target_video_views == 0:
                parent_candidates = [c for c in view_count_dom if c.get('source') == 'video-parent-aria']
                if parent_candidates:
                    for c in parent_candidates:
                        text = c.get('text', '')
                        # Extract number from "có X lượt xem"
                        m = re.search(r'c\u00f3\s+([\d.,]+\s*[KkMmBb]?(?:\s*ngh\u00ecn|\s*tri\u1ec7u)?)\s+l\u01b0\u1ee3t\s+xem', text)
                        if m:
                            val = parse_vietnamese_number(m.group(1))
                            if 10 <= val <= 1000000000:
                                target_video_views = val
                                result['debug']['view_sources']['video_parent_aria'] = val
                                break
            
            # Strategy C: Try aria-label with username matching (filter related reels)
            if target_video_views == 0 and aria_candidates:
                # Try to match by username substring in caption
                # Caption might mention author, e.g. "LOA LOA LOA..." doesn't help
                # But we can EXCLUDE aria-labels that mention different usernames
                
                # Get usernames from aria-labels
                # Format: "Xem video thước phim của {username} có {views} lượt xem"
                aria_usernames = []
                for c in aria_candidates:
                    text = c.get('text', '')
                    m = re.search(r'thư\u1edbc phim c\u1ee7a\s+([^\s]+(?:\s+[^\s]+)?(?:\s+[^\s]+)?)\s+c\u00f3', text)
                    if m:
                        aria_usernames.append(m.group(1))
                
                result['debug']['aria_usernames_found'] = aria_usernames
                
                # If we have pageUsername that matches one of aria_usernames, use that view
                # Otherwise, DO NOT pick any aria-label (they're all related reels)
                # because target video usually has NO aria-label
            
            if target_video_views > 0:
                if result['data']['views'] == 0:
                    result['data']['views'] = target_video_views
        except Exception as e:
            logger.warning(f'DOM view query failed: {e}')
        
        # V8.6: Use icon-specific parser
        parsed = parse_engagement_v83(mobile_innertext, result['debug'])
        
        # Always set values from V8.6 (they may be 0 which is correct)
        result['data']['likes'] = parsed.get('likes', 0)
        result['data']['comments'] = parsed.get('comments', 0)
        result['data']['shares'] = parsed.get('shares', 0)
        if parsed.get('views', 0) > 0:
            result['data']['views'] = parsed['views']
            result['debug']['view_sources']['mobile_v83_eye'] = parsed['views']
        
        # V8.6: Removed fallback to mobile_html search_views_in_text
        # because it was picking up views from "Watch more reels" section (related reels)
        # not the target video. This caused incorrect views (e.g. 8.4M instead of 1.3K).
        # 
        # If DOM extraction (above) didn't find target video views, we leave views=0
        # rather than returning incorrect data from related reels.
        
        context.close()
    except Exception as e:
        logger.warning(f'Mobile v83 mode failed: {e}')


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
        'service': 'Rise City Facebook Scraper 🎩',
        'version': '8.6-api-capture',
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
        'version': '8.6-api-capture',
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
