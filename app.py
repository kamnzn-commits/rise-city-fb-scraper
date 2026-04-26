"""
Rise City Facebook Scraper API 🎩 - V2
Cải tiến: 
- Auto-resolve shortened URL (/share/r/, /share/v/, fb.watch)
- Try nhiều URL formats
- Better error logging
"""
from flask import Flask, request, jsonify
from facebook_scraper import get_posts
import os
import re
import logging
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

COOKIES_PATH = os.getenv('FB_COOKIES_PATH', '/etc/secrets/cookies.txt')
API_SECRET = os.getenv('API_SECRET', 'rise-city-secret-2026')


def resolve_facebook_url(url):
    """
    Resolve shortened FB URL to full URL.
    Examples:
    - https://www.facebook.com/share/r/XXXX/ -> https://www.facebook.com/reel/123456
    - https://fb.watch/XXXX/ -> https://www.facebook.com/...
    """
    try:
        # Load cookies for redirect tracking
        cookies = {}
        if os.path.exists(COOKIES_PATH):
            with open(COOKIES_PATH, 'r') as f:
                for line in f:
                    if line.startswith('#') or not line.strip():
                        continue
                    parts = line.strip().split('\t')
                    if len(parts) >= 7:
                        cookies[parts[5]] = parts[6]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        # Follow redirects
        response = requests.get(url, headers=headers, cookies=cookies, allow_redirects=True, timeout=15)
        final_url = response.url
        logger.info(f'Resolved {url} -> {final_url}')
        return final_url
    except Exception as e:
        logger.error(f'URL resolve failed: {e}')
        return url


def extract_video_id_from_html(url):
    """Try to extract video_id from FB page HTML"""
    try:
        cookies = {}
        if os.path.exists(COOKIES_PATH):
            with open(COOKIES_PATH, 'r') as f:
                for line in f:
                    if line.startswith('#') or not line.strip():
                        continue
                    parts = line.strip().split('\t')
                    if len(parts) >= 7:
                        cookies[parts[5]] = parts[6]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        response = requests.get(url, headers=headers, cookies=cookies, allow_redirects=True, timeout=15)
        html = response.text
        
        # Look for video_id patterns in HTML
        patterns = [
            r'"video_id":"(\d+)"',
            r'"videoID":"(\d+)"',
            r'/reel/(\d+)',
            r'/videos/(\d+)',
            r'story_fbid=(\d+)',
            r'"top_level_post_id":"(\d+)"',
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                vid = match.group(1)
                logger.info(f'Found video ID from HTML: {vid}')
                return vid
        
        return None
    except Exception as e:
        logger.error(f'Extract video ID from HTML failed: {e}')
        return None


def extract_post_id(url):
    """Extract post ID from various Facebook URL formats"""
    patterns = [
        r'/reel/(\d+)',
        r'/videos/(\d+)',
        r'/posts/(\d+)',
        r'fbid=(\d+)',
        r'story_fbid=(\d+)',
        r'/permalink/(\d+)',
        r'/share/[rvp]/([^/?]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'ok',
        'service': 'Rise City Facebook Scraper 🎩',
        'version': '2.0',
        'message': 'Niệm Bụt 3 lần để scrape Facebook!',
        'endpoints': {
            '/scrape': 'POST - Scrape video stats (cần API key)',
            '/health': 'GET - Health check',
            '/resolve': 'POST - Test URL resolver',
        }
    })


@app.route('/health', methods=['GET'])
def health():
    cookies_exist = os.path.exists(COOKIES_PATH)
    return jsonify({
        'status': 'ok',
        'cookies_loaded': cookies_exist,
        'cookies_path': COOKIES_PATH if cookies_exist else 'NOT FOUND',
        'version': '2.0'
    })


@app.route('/resolve', methods=['POST'])
def resolve_url_endpoint():
    """Test endpoint to resolve URL"""
    api_key = request.headers.get('X-API-Key') or request.headers.get('x-api-key')
    if api_key != API_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.get_json(silent=True) or {}
    url = data.get('url', '')
    if not url:
        return jsonify({'error': 'Missing url'}), 400
    
    resolved = resolve_facebook_url(url)
    video_id = extract_video_id_from_html(url)
    post_id_from_url = extract_post_id(url)
    post_id_from_resolved = extract_post_id(resolved)
    
    return jsonify({
        'original_url': url,
        'resolved_url': resolved,
        'post_id_from_url': post_id_from_url,
        'post_id_from_resolved': post_id_from_resolved,
        'video_id_from_html': video_id,
    })


@app.route('/scrape', methods=['POST'])
def scrape_video():
    """Scrape Facebook video stats"""
    
    api_key = request.headers.get('X-API-Key') or request.headers.get('x-api-key')
    if api_key != API_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.get_json(silent=True) or {}
    url = data.get('url', '')
    
    if not url:
        return jsonify({'error': 'Missing url parameter'}), 400
    
    logger.info(f'Scraping: {url}')
    
    if not os.path.exists(COOKIES_PATH):
        return jsonify({
            'error': 'Cookies file not found',
            'message': 'Server chưa được cấu hình đúng.'
        }), 500
    
    # Step 1: Resolve URL if shortened
    is_shortened = '/share/' in url or 'fb.watch' in url
    resolved_url = url
    
    if is_shortened:
        resolved_url = resolve_facebook_url(url)
        logger.info(f'Resolved URL: {resolved_url}')
    
    # Step 2: Get post/video ID
    post_id = extract_post_id(resolved_url) or extract_post_id(url)
    
    # Step 3: If still no ID, try extracting from HTML
    if not post_id:
        post_id = extract_video_id_from_html(url)
    
    logger.info(f'Final post_id: {post_id}')
    
    # Step 4: Try multiple URL formats with facebook-scraper
    try_urls = []
    
    # Add original and resolved
    try_urls.append(resolved_url)
    if url != resolved_url:
        try_urls.append(url)
    
    # Add post_id-based URLs
    if post_id:
        try_urls.append(post_id)
        try_urls.append(f'https://www.facebook.com/{post_id}')
        try_urls.append(f'https://www.facebook.com/reel/{post_id}')
        try_urls.append(f'https://www.facebook.com/watch/?v={post_id}')
    
    debug_attempts = []
    posts = []
    
    for try_url in try_urls:
        try:
            logger.info(f'Trying URL: {try_url}')
            results = list(get_posts(
                post_urls=[try_url],
                cookies=COOKIES_PATH,
                options={
                    'reactors': False,
                    'comments': False,
                    'progress': False,
                    'allow_extra_requests': True,
                }
            ))
            
            if results and len(results) > 0:
                post = results[0]
                # Check if we got real data (not empty)
                has_data = (
                    post.get('post_id') or 
                    post.get('text') or 
                    post.get('video_watches') or 
                    post.get('likes') or
                    post.get('reactions')
                )
                
                debug_attempts.append({
                    'url': try_url,
                    'success': True,
                    'has_data': bool(has_data),
                    'fields_found': [k for k, v in post.items() if v is not None and v != '' and v != 0]
                })
                
                if has_data:
                    posts = results
                    logger.info(f'SUCCESS with URL: {try_url}')
                    break
            else:
                debug_attempts.append({'url': try_url, 'success': False, 'message': 'Empty result'})
        except Exception as e:
            debug_attempts.append({'url': try_url, 'success': False, 'error': str(e)})
            logger.warning(f'Failed with {try_url}: {e}')
            continue
    
    if not posts:
        return jsonify({
            'success': False,
            'error': 'No data found',
            'message': 'facebook-scraper không lấy được data từ link này',
            'debug': {
                'original_url': url,
                'resolved_url': resolved_url,
                'extracted_post_id': post_id,
                'attempts': debug_attempts
            }
        }), 200  # Return 200 with success=false for easier debugging
    
    post = posts[0]
    
    # Sum reactions
    reactions = post.get('reactions') or {}
    total_likes = 0
    if reactions:
        total_likes = sum(v for v in reactions.values() if isinstance(v, int))
    if total_likes == 0:
        total_likes = post.get('likes') or 0
    
    return jsonify({
        'success': True,
        'data': {
            'views': post.get('video_watches') or 0,
            'likes': total_likes,
            'comments': post.get('comments') or 0,
            'shares': post.get('shares') or 0,
            'caption': (post.get('text') or post.get('post_text') or '')[:500],
            'thumbnail': post.get('image') or post.get('video_thumbnail') or '',
            'reactions_breakdown': reactions,
            'post_id': post.get('post_id'),
            'username': post.get('username'),
            'time': str(post.get('time')) if post.get('time') else None,
            'video_url': post.get('video') or '',
            'is_live': post.get('is_live', False),
        },
        'debug': {
            'resolved_url': resolved_url,
            'post_id_used': post_id,
            'attempts_count': len(debug_attempts)
        }
    })


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
