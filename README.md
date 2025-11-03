# HRIS - Human Resource Information System

Sistem informasi manajemen SDM dengan integrasi Facebook Ads dan Google Ad Manager and Adsense.

## Requirements

- **Python 3.11** or Maks 3.13
- Redis server
- MySQL database

## Features

- **Facebook Ads Management**: Dashboard untuk monitoring dan analisis Facebook Ads insights
- **Redis Caching**: Sistem caching untuk meningkatkan performa dan mengatasi rate limit API
- **Google OAuth2**: Autentikasi menggunakan Google OAuth2
- **Google Ad Manager and Adsense Integration**: Integrasi dengan Google Ad Manager dan Adsense
- **Real-time Analytics**: Dashboard real-time untuk monitoring campaign performance
 - **Mail Utility**: Kirim email via SMTP dengan antarmuka mirip Laravel Mail

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
Copy file `.env.example` menjadi `.env`:
```bash
# Windows (Command Prompt)
copy .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env

# Linux/Mac
cp .env.example .env
```

Kemudian edit file `.env` dan isi dengan konfigurasi Anda:
```env
# Django Configuration
DJANGO_SECRET_KEY=your-secret-key-here
DEBUG=True

# Database Configuration
DB_NAME=hris_trendhorizone
DB_USER=root
DB_PASSWORD=
DB_HOST=localhost
DB_PORT=3306

# Google OAuth2 Configuration
GOOGLE_OAUTH2_CLIENT_ID=your-google-oauth2-client-id
GOOGLE_OAUTH2_CLIENT_SECRET=your-google-oauth2-client-secret
```

Tambahkan konfigurasi Mail (SMTP) di `.env`:
```env
# Mail Configuration
MAIL_MAILER=smtp
MAIL_HOST=smtp.gmail.com
MAIL_PORT=587
MAIL_USERNAME=cs.extools@gmail.com
MAIL_PASSWORD=cfvgewmrevypiscx
MAIL_ENCRYPTION=tls
MAIL_FROM_ADDRESS=account@trendhorizone.id
MAIL_FROM_NAME="HRiS Trend Horizone"
```

### 4. Setup Redis
Install dan jalankan Redis server:
```bash
# macOS
brew install redis
brew services start redis

# Ubuntu/Debian
sudo apt-get install redis-server
sudo systemctl start redis-server
```

### 5. Database Migration
```bash
python manage.py migrate
```

### 6. Run Server
```bash
python manage.py runserver
```

## Security Notes

⚠️ **PENTING**: Jangan pernah commit file yang mengandung secrets ke repository!

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

### Google Ad Manager and Adsense
- Revenue reporting
- Traffic analytics

## OAuth2 Configuration

For Google OAuth2 authentication, you need to configure the following in your Google Cloud Console:

### Required OAuth2 Credentials:
- **Client ID**: Your Google OAuth2 client ID
- **Client Secret**: Your Google OAuth2 client secret

### Authorized Redirect URIs:
Add these callback URLs to your Google Cloud Console OAuth2 client configuration:
- `https://kiwipixel.com/accounts/complete/google-oauth2/`
- `https://kiwipixel.com/management/admin/oauth/callback/`

### OAuth2 Scopes:
The application requests the following scopes for full functionality:
- `openid` - OpenID Connect authentication
- `email` - Access to user's email address
- `profile` - Access to user's basic profile information
- `https://www.googleapis.com/auth/dfp` - Google Ad Manager API access
- `https://www.googleapis.com/auth/adsense` - Google AdSense API access
- `https://www.googleapis.com/auth/adsense.readonly` - Google AdSense read-only access

### Configuration:
No JSON file is required. Simply configure the Client ID and Client Secret values in your `.env` file as shown in the Environment Variables section above.

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
 - `hris/mail/`: Utilitas pengiriman email (builder & fungsi cepat)

## Contributing

1. Fork repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## License

Private project - All rights reserved.
## Mail Usage

Kirim email dari file mana pun dengan antarmuka mirip Laravel.

- Quick helper:

```python
from hris.mail import send_mail

send_mail(
    to=['user@example.com', 'admin@example.com'],
    subject='Welcome to HRIS',
    template='emails/simple.html',  # opsional: gunakan template Django
    context={'name': 'Hendrik', 'body': body, 'subject': subject, 'brand_name': from_name or 'HRiS Trend Horizone'},     # data untuk template
    attachments=['/path/to/file.pdf']  # dukungan lampiran (path atau bytes)
)
```

- Fluent builder:

```python
from hris.mail import Mail

Mail.to('user@example.com') \
    .cc(['manager@example.com']) \
    .subject('Weekly Report') \
    .view('emails/report.html', {'week': 44, 'summary': 'OK'}) \
    .attach(file_path='reports/week_44.xlsx') \
    .send()
```

Konfigurasi diambil otomatis dari `.env` (atau `settings`) menggunakan `python-dotenv`:

- `MAIL_MAILER`: `smtp` (default) atau backend Django lainnya
- `MAIL_HOST`, `MAIL_PORT`, `MAIL_USERNAME`, `MAIL_PASSWORD`
- `MAIL_ENCRYPTION`: `tls` atau `ssl`
- `MAIL_FROM_ADDRESS`, `MAIL_FROM_NAME`

Jika menggunakan template, pastikan direktori template telah diset di `hris/settings.py` sesuai panduan `TEMPLATE_SETTINGS_UPDATE.md`.