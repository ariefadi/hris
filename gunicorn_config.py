import multiprocessing

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "sync"
worker_connections = 1000
timeout = 30
keepalive = 2

# Restart workers after this many requests, to help prevent memory leaks
max_requests = 1000
max_requests_jitter = 50

# Logging
errorlog = "/var/log/gunicorn/hris_error.log"
loglevel = "info"
accesslog = "/var/log/gunicorn/hris_access.log"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Process naming
proc_name = 'hris_gunicorn'
raw_env = [
    'PYCRYPTODOME_DISABLE_CPUID=1',
    'HOME=/var/cache/hris',
    'XDG_CACHE_HOME=/var/cache/hris/.cache'
]

# Server mechanics
preload_app = True
daemon = False
pidfile = "/var/run/gunicorn/hris.pid"
user = "www-data"
group = "www-data"
tmp_upload_dir = None

# SSL
keyfile = None
certfile = None