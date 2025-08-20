# HRIS - Human Resource Information System

Sistem informasi manajemen SDM dengan integrasi Facebook Ads dan Google Ads Manager.

## Features

- **Facebook Ads Management**: Dashboard untuk monitoring dan analisis Facebook Ads insights
- **Redis Caching**: Sistem caching untuk meningkatkan performa dan mengatasi rate limit API
- **Google OAuth2**: Autentikasi menggunakan Google OAuth2
- **Google Ads Integration**: Integrasi dengan Google Ads Manager
- **Real-time Analytics**: Dashboard real-time untuk monitoring campaign performance

## Setup

### 1. Clone Repository
```bash
git clone https://github.com/ariefadi/hris.git
cd hris
```

### 2. Install Dependencies
```bash
pip install -r requiredment.txt
```

### 3. Environment Variables
Copy `.env.example` ke `.env` dan isi dengan nilai yang sesuai:
```bash
cp .env.example .env
```

Edit file `.env` dan isi dengan konfigurasi Anda:
```env
# Django Configuration
DJANGO_SECRET_KEY=your-actual-secret-key-here
DEBUG=True

# Google OAuth2 Configuration
GOOGLE_OAUTH2_KEY=your-google-oauth2-key
GOOGLE_OAUTH2_SECRET=your-google-oauth2-secret

# Facebook API Configuration
FACEBOOK_APP_ID=your-facebook-app-id
FACEBOOK_APP_SECRET=your-facebook-app-secret
FACEBOOK_ACCESS_TOKEN=your-facebook-access-token
```

### 4. Setup Google Ads API
Buat file `googleads.yaml` di root directory:
```yaml
developer_token: 'YOUR_DEVELOPER_TOKEN'
client_id: 'YOUR_CLIENT_ID'
client_secret: 'YOUR_CLIENT_SECRET'
refresh_token: 'YOUR_REFRESH_TOKEN'
customer_id: 'YOUR_CUSTOMER_ID'
```

### 5. Setup Redis
Install dan jalankan Redis server:
```bash
# macOS
brew install redis
brew services start redis

# Ubuntu/Debian
sudo apt-get install redis-server
sudo systemctl start redis-server
```

### 6. Database Migration
```bash
python manage.py migrate
```

### 7. Run Server
```bash
python manage.py runserver
```

## Security Notes

⚠️ **PENTING**: Jangan pernah commit file yang mengandung secrets ke repository!

- File `googleads.yaml` sudah di-exclude di `.gitignore`
- File `.env` sudah di-exclude di `.gitignore`
- Gunakan environment variables untuk semua konfigurasi sensitif
- File `.env.example` hanya berisi template, bukan nilai sebenarnya

## Redis Caching

Sistem menggunakan Redis untuk caching Facebook API responses:
- **Cache Timeout**: 5 menit untuk insights data
- **Fallback Cache**: 24 jam untuk rate limit scenarios
- **Auto Invalidation**: Cache otomatis di-refresh setelah timeout

## API Integrations

### Facebook Ads API
- Monitoring campaign performance
- Real-time insights data
- Budget management
- Status tracking

### Google Ads Manager
- Ad inventory management
- Revenue reporting
- Traffic analytics

## Development

### Project Structure
```
hris/
├── management/          # Main Django app
│   ├── static/         # Static files (CSS, JS)
│   ├── templates/      # HTML templates
│   ├── utils.py        # Utility functions & caching
│   ├── views.py        # View controllers
│   └── database.py     # Database operations
├── hris/               # Django project settings
└── requirements.txt    # Python dependencies
```

### Key Files
- `management/utils.py`: Facebook API functions dengan Redis caching
- `hris/settings.py`: Django configuration dengan Redis setup
- `.gitignore`: Security exclusions
- `.env.example`: Environment variables template

## Contributing

1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## License

Private project - All rights reserved.