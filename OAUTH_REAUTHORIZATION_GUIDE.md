# 🔧 Panduan Re-Authorization OAuth untuk Google Ad Manager API

## 📋 Ringkasan Masalah
Error "unauthorized_client: Unauthorized" terjadi karena:
1. OAuth refresh token tidak memiliki scope yang lengkap
2. Scope `https://www.googleapis.com/auth/admanager` belum ada dalam token yang tersimpan
3. User perlu melakukan re-authorization dengan scope yang diperbaharui

## ✅ Perbaikan yang Sudah Dilakukan
1. ✅ Menambahkan scope `https://www.googleapis.com/auth/admanager` ke `hris/settings.py`
2. ✅ Memperbarui `SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE` dengan scope lengkap
3. ✅ Membuat script `fix_oauth_unauthorized.py` untuk generate OAuth URL
4. ✅ Membuat script `exchange_oauth_code.py` untuk menukar authorization code

## 🚀 Langkah-langkah Re-Authorization

### Langkah 1: Generate OAuth URL
```bash
echo "adiarief463@gmail.com" | python fix_oauth_unauthorized.py
```

### Langkah 2: Akses URL OAuth
1. Copy URL yang dihasilkan dari output script
2. Buka URL tersebut di browser
3. Login dengan akun Google yang sesuai (adiarief463@gmail.com)
4. **PENTING**: Berikan izin untuk SEMUA scope yang diminta:
   - openid
   - email  
   - profile
   - https://www.googleapis.com/auth/dfp
   - https://www.googleapis.com/auth/admanager

### Langkah 3: Dapatkan Authorization Code
1. Setelah memberikan izin, Google akan menampilkan authorization code
2. Copy code tersebut (biasanya berupa string panjang)

### Langkah 4: Exchange Code dengan Refresh Token
```bash
python exchange_oauth_code.py adiarief463@gmail.com
```
Masukkan authorization code yang sudah di-copy

### Langkah 5: Verifikasi Perbaikan
```bash
python check_oauth_token.py
```

## ⚠️ Troubleshooting

### Jika Google tidak menampilkan permission screen:
1. Buka https://myaccount.google.com/permissions
2. Cari aplikasi HRIS/OAuth app
3. Klik "Remove access" 
4. Ulangi proses dari Langkah 1

### Jika masih mendapat error "invalid_client":
1. Periksa Client ID dan Client Secret di database
2. Pastikan OAuth app sudah dikonfigurasi dengan benar di Google Cloud Console
3. Pastikan redirect URI sudah sesuai

### Jika error "access_denied":
1. Pastikan user memiliki akses ke Google Ad Manager network
2. Periksa network_code yang digunakan
3. Pastikan service account sudah ditambahkan ke Ad Manager (lihat ADMANAGER_SETUP_GUIDE.md)

## 🔍 Verifikasi Hasil

Setelah re-authorization berhasil, test dengan:
1. Akses AdX Account Data di aplikasi
2. Periksa tidak ada lagi error "unauthorized_client"
3. Pastikan data Ad Manager bisa diambil dengan benar

## 📝 Catatan Penting
- Re-authorization harus dilakukan untuk setiap user yang mengalami error
- Scope baru akan berlaku untuk semua user baru yang melakukan authorization
- User lama perlu melakukan re-authorization manual seperti langkah di atas