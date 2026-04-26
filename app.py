"""
Rise City Facebook Scraper API 🎩
Sử dụng kevinzg/facebook-scraper để scrape view, like, comment, share
từ Facebook video cá nhân thông qua cookies.
"""
from flask import Flask, request, jsonify
from facebook_scraper import get_posts
import os
import re
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Đường dẫn file cookies (sẽ upload lên Render qua Secret File)
COOKIES_PATH = os.getenv('FB_COOKIES_PATH', '/etc/secrets/cookies.txt')

# API key bảo vệ endpoint - chỉ Lovable mới gọi được
API_SECRET = os.getenv('API_SECRET', 'rise-city-secret-2026')


def extract_post_id(url):
    """Trích xuất post ID từ nhiều dạng URL Facebook"""
    patterns = [
        r'/share/[rvp]/([^/?]+)',
        r'/posts/(\d+)',
        r'/videos/(\d+)',
        r'/reel/(\d+)',
        r'fbid=(\d+)',
        r'story_fbid=(\d+)',
        r'/permalink/(\d+)',
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
        'message': 'Niệm Bụt 3 lần để scrape Facebook!',
        'endpoints': {
            '/scrape': 'POST - Scrape video stats (cần API key)',
            '/health': 'GET - Health check'
        }
    })


@app.route('/health', methods=['GET'])
def health():
    cookies_exist = os.path.exists(COOKIES_PATH)
    return jsonify({
        'status': 'ok',
        'cookies_loaded': cookies_exist,
        'cookies_path': COOKIES_PATH if cookies_exist else 'NOT FOUND'
    })


@app.route('/scrape', methods=['POST'])
def scrape_video():
    """Scrape Facebook video stats"""
    
    # Verify API key
    api_key = request.headers.get('X-API-Key') or request.headers.get('x-api-key')
    if api_key != API_SECRET:
        logger.warning(f'Unauthorized request from {request.remote_addr}')
        return jsonify({'error': 'Unauthorized', 'message': 'Invalid API key'}), 401
    
    # Get URL from body
    data = request.get_json(silent=True) or {}
    url = data.get('url', '')
    
    if not url:
        return jsonify({'error': 'Missing url parameter'}), 400
    
    logger.info(f'Scraping: {url}')
    
    # Check cookies file exists
    if not os.path.exists(COOKIES_PATH):
        return jsonify({
            'error': 'Cookies file not found',
            'message': 'Server chưa được cấu hình đúng. Hãy upload cookies.txt vào /etc/secrets/'
        }), 500
    
    try:
        # Extract post ID
        post_id = extract_post_id(url)
        logger.info(f'Extracted post ID: {post_id}')
        
        # Try with post_urls first (works with full URL)
        try_urls = [url]
        if post_id and post_id not in url:
            try_urls.append(post_id)
        
        posts = []
        last_error = None
        
        for try_url in try_urls:
            try:
                posts = list(get_posts(
                    post_urls=[try_url],
                    cookies=COOKIES_PATH,
                    options={
                        'reactors': False,
                        'comments': False,
                        'progress': False,
                        'allow_extra_requests': True,
                    }
                ))
                if posts:
                    logger.info(f'Success with URL: {try_url}')
                    break
            except Exception as e:
                last_error = str(e)
                logger.warning(f'Failed with {try_url}: {e}')
                continue
        
        if not posts:
            return jsonify({
                'success': False,
                'error': 'No data found',
                'message': 'facebook-scraper không lấy được data từ link này',
                'last_error': last_error,
                'tried_urls': try_urls
            }), 404
        
        post = posts[0]
        
        # Sum reactions to get total likes
        reactions = post.get('reactions') or {}
        total_likes = 0
        if reactions:
            total_likes = sum(v for v in reactions.values() if isinstance(v, int))
        else:
            total_likes = post.get('likes') or 0
        
        # Build response
        result = {
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
            }
        }
        
        logger.info(f'Result: views={result["data"]["views"]}, likes={total_likes}, comments={result["data"]["comments"]}')
        return jsonify(result)
    
    except Exception as e:
        logger.exception('Scrape error')
        return jsonify({
            'success': False,
            'error': type(e).__name__,
            'message': str(e),
            'url': url
        }), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
