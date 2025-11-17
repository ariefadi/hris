import multiprocessing

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
workers = max(2, multiprocessing.cpu_count())  # turunkan agar stabil di memori
worker_class = "sync"
worker_connections = 1000
timeout = 180  # beri waktu lebih untuk request berat
graceful_timeout = 180  # beri waktu bagi worker menyelesaikan request sebelum di-kill
keepalive = 5

# Restart workers after this many requests, to help prevent memory leaks
max_requests = 2000
max_requests_jitter = 200

# Logging
errorlog = "/var/log/gunicorn/hris_error.log"
loglevel = "info"
accesslog = "/var/log/gunicorn/hris_access.log"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
capture_output = True  # tangkap stdout/stderr ke errorlog

# Process naming
proc_name = 'hris_gunicorn'
raw_env = [
    'PYCRYPTODOME_DISABLE_CPUID=1',
    'HOME=/var/cache/hris',
    'XDG_CACHE_HOME=/var/cache/hris/.cache'
]

# Server mechanics
preload_app = False  # kurangi beban startup saat respawn
daemon = False
pidfile = "/var/run/gunicorn/hris.pid"
user = "www-data"
group = "www-data"
tmp_upload_dir = None
worker_tmp_dir = "/dev/shm"  # kurangi I/O disk untuk temp files

# SSL
keyfile = None
certfile = None