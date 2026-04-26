# 🎩 Rise City Facebook Scraper

API server scrape view, like, comment, share từ Facebook video cá nhân.

## 🏗️ Kiến trúc

```
Lovable App → POST /scrape → This Server → Facebook (with cookies) → Return stats
```

## 📋 Deploy lên Render

### Bước 1: Upload code lên GitHub

1. Tạo repo mới trên GitHub: `rise-city-fb-scraper`
2. Upload tất cả files (app.py, requirements.txt, render.yaml, README.md)
3. **KHÔNG upload cookies.txt!**

### Bước 2: Deploy trên Render

1. Vào https://dashboard.render.com → New → Web Service
2. Connect GitHub repo `rise-city-fb-scraper`
3. Render tự đọc render.yaml và setup
4. Click "Create Web Service"

### Bước 3: Upload cookies

1. Trong Render dashboard, vào Service → Settings → Secret Files
2. Click "Add Secret File"
   - Filename: `cookies.txt`
   - Path: `/etc/secrets/cookies.txt`
   - Contents: Paste nội dung file cookies.txt từ máy
3. Save → Service tự restart

### Bước 4: Lấy URL & API key

1. Render dashboard → Service → URL có dạng: `https://rise-city-fb-scraper.onrender.com`
2. Settings → Environment → Copy `API_SECRET` value

### Bước 5: Test

```bash
# Test health
curl https://your-service.onrender.com/health

# Test scrape (replace API_KEY và URL)
curl -X POST https://your-service.onrender.com/scrape \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_API_KEY" \
  -d '{"url": "https://www.facebook.com/share/r/18DB7h4tkt/"}'
```

## 🔌 Tích hợp với Lovable

Trong Edge Function `detect-and-fetch-video`, thêm:

```typescript
const SCRAPER_URL = Deno.env.get('FB_SCRAPER_URL');
const SCRAPER_KEY = Deno.env.get('FB_SCRAPER_KEY');

async function tryFacebookScraper(url: string) {
  if (!SCRAPER_URL) return null;
  
  const response = await fetch(`${SCRAPER_URL}/scrape`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': SCRAPER_KEY!
    },
    body: JSON.stringify({ url })
  });
  
  if (!response.ok) return null;
  const result = await response.json();
  
  if (result.success && result.data) {
    return result.data;
  }
  return null;
}
```

## ⚠️ Lưu ý bảo mật

- **KHÔNG** commit cookies.txt lên GitHub
- **KHÔNG** share API_SECRET ra ngoài
- Cookies sẽ hết hạn sau 1-2 tháng → cần re-export

## 🆘 Troubleshooting

### Lỗi "Cookies file not found"
→ Upload cookies.txt qua Render Secret Files

### Lỗi "Login required" hoặc trả về empty
→ Cookies hết hạn, re-login FB phụ và export lại

### Service timeout (>30s)
→ Render free tier có cold start. Lần đầu gọi sau 15 phút sẽ chậm.
