# Google Ad Manager Setup Guide

Masalah "Unknown error occurred" di AdX Traffic Account telah diperbaiki di sisi kode, namun masih memerlukan konfigurasi yang benar di Google Ad Manager.

## Status Perbaikan

✅ **Kode telah diperbaiki:**
- Error handling di frontend sudah diperbaiki
- OAuth scope sudah ditambahkan (`https://www.googleapis.com/auth/dfp`)
- Service account authentication sudah diprioritaskan
- Pesan error sekarang informatif

❌ **Masih perlu konfigurasi Ad Manager:**
- Service account belum ditambahkan ke Ad Manager account
- Permission belum diberikan ke service account

## Langkah-langkah Perbaikan

### 1. Tambahkan Service Account ke Google Ad Manager

**Service Account Email:** `hris-24@named-tube-469113-j3.iam.gserviceaccount.com`

**Langkah:**
1. Login ke [Google Ad Manager](https://admanager.google.com/)
2. Pilih network code: `23303534834`
3. Pergi ke **Admin** → **Global Settings** → **Network Settings**
4. Scroll ke bagian **API Access**
5. Klik **Add API User**
6. Masukkan email: `hris-24@named-tube-469113-j3.iam.gserviceaccount.com`
7. Pilih role: **Admin** atau **Reporting**
8. Klik **Save**

### 2. Verifikasi Network Code

**Current Network Code:** `23303534834`

Pastikan network code ini benar dengan:
1. Login ke Google Ad Manager
2. Lihat di URL atau header halaman
3. Network code harus sama dengan yang ada di `.env` file

### 3. Test Koneksi

Setelah menambahkan service account, jalankan:

```bash
python test_auth_fix.py
```

### 4. Restart Server

Setelah konfigurasi selesai:

```bash
# Stop server jika sedang berjalan
# Kemudian restart
python manage.py runserver
```

## Troubleshooting

### Error: "Authentication Error"
- **Penyebab:** Service account belum ditambahkan ke Ad Manager
- **Solusi:** Ikuti langkah 1 di atas

### Error: "Permission Error"
- **Penyebab:** Service account tidak memiliki permission yang cukup
- **Solusi:** Berikan role "Admin" atau "Reporting" ke service account

### Error: "Network Error"
- **Penyebab:** Network code salah atau tidak memiliki akses
- **Solusi:** Verifikasi network code dan pastikan service account memiliki akses

### Error: "Invalid Credentials"
- **Penyebab:** Service account key file rusak atau salah
- **Solusi:** Download ulang service account key dari Google Cloud Console

## File yang Telah Dimodifikasi

1. **`management/utils.py`** - Diperbaiki error handling dan prioritas service account
2. **`hris/settings.py`** - Ditambahkan OAuth scope untuk DFP
3. **`management/utils.py`** - Ditambahkan field `error` pada response

## Informasi Kredensial

**Service Account:**
- Project ID: `named-tube-469113-j3`
- Email: `hris-24@named-tube-469113-j3.iam.gserviceaccount.com`
- Key File: `/Users/ariefdwicahyoadi/hris/service-account-key.json`

**Ad Manager:**
- Network Code: `23303534834`
- Required Scope: `https://www.googleapis.com/auth/dfp`

## Hasil Setelah Perbaikan

Setelah konfigurasi selesai:

✅ Error "Unknown error occurred" tidak akan muncul lagi
✅ Pesan error akan informatif dan jelas
✅ AdX Traffic Account akan berfungsi normal
✅ Data traffic akan dapat diambil dari Ad Manager API

## Kontak Support

Jika masih mengalami masalah setelah mengikuti panduan ini:
1. Pastikan semua langkah telah diikuti dengan benar
2. Periksa log error di terminal untuk detail lebih lanjut
3. Verifikasi bahwa Anda memiliki akses Admin ke Google Ad Manager account