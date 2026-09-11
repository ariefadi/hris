$().ready(function () {

    var LOGIN_LOADER_MS = 320;

    function readCsrfCookie() {
        var prefix = 'csrftoken=';
        var parts = document.cookie ? document.cookie.split(';') : [];
        for (var i = 0; i < parts.length; i++) {
            var part = parts[i].trim();
            if (part.indexOf(prefix) === 0) {
                return decodeURIComponent(part.substring(prefix.length));
            }
        }
        return '';
    }

    function syncLoginCsrfToken() {
        // CSRF token di form selalu fresh dari halaman saat ini (CSRF_USE_SESSIONS=True).
        // Jangan timpa dengan cookie csrftoken lama — itu bikin 403 loop setelah timeout.
        var $input = $('input[name=csrfmiddlewaretoken]').first();
        var formToken = String($input.val() || '').trim();
        if (formToken) {
            return formToken;
        }
        return readCsrfCookie();
    }

    function clearStaleCsrfCookieIfNeeded() {
        var $input = $('input[name=csrfmiddlewaretoken]').first();
        var formToken = String($input.val() || '').trim();
        var cookieToken = readCsrfCookie();
        if (!cookieToken || !formToken || cookieToken === formToken) {
            return;
        }
        try {
            document.cookie = 'csrftoken=; Max-Age=0; path=/; SameSite=Lax';
            if (window.location && window.location.hostname) {
                document.cookie = 'csrftoken=; Max-Age=0; path=/; domain=' + window.location.hostname + '; SameSite=Lax';
            }
        } catch (_e) {}
    }

    function showLoginError(title, text, opts) {
        var options = opts || {};
        if (typeof Swal !== 'undefined' && Swal.fire) {
            Swal.fire({
                icon: options.icon || 'error',
                title: String(title || 'Gagal'),
                text: String(text || ''),
                confirmButtonText: options.confirmButtonText || 'OK'
            }).then(function () {
                if (typeof options.onClose === 'function') {
                    options.onClose();
                }
            });
            return;
        }
        alert(String(title || 'Gagal') + '\n' + String(text || ''));
        if (typeof options.onClose === 'function') {
            options.onClose();
        }
    }

    report_eror = function (jqXHR, exception) {
        if (jqXHR.status === 403) {
            clearStaleCsrfCookieIfNeeded();
            showLoginError(
                'Sesi Form Kedaluwarsa',
                'Token keamanan login sudah tidak valid (biasanya setelah idle 15 menit atau tab dibiarkan terbuka lama). Klik Muat Ulang, lalu login kembali.',
                {
                    icon: 'warning',
                    confirmButtonText: 'Muat Ulang',
                    onClose: function () {
                        window.location.href = '/management/admin/login?_=' + Date.now();
                    }
                }
            );
            return;
        }
        if (jqXHR.status === 401) {
            showLoginError(
                'Sesi Berakhir',
                'Silakan muat ulang halaman login lalu coba lagi.',
                {
                    icon: 'warning',
                    onClose: function () {
                        window.location.reload();
                    }
                }
            );
            return;
        }
        var msg = '';
        if (jqXHR.status === 0) {
            msg = 'Tidak ada koneksi ke server. Periksa internet Anda lalu coba lagi.';
        } else if (jqXHR.status == 404) {
            msg = 'Endpoint login tidak ditemukan (404). Hubungi administrator.';
        } else if (jqXHR.status == 500) {
            msg = 'Terjadi kesalahan server internal (500). Coba lagi beberapa saat.';
        } else if (exception === 'parsererror') {
            msg = 'Respons server tidak valid. Silakan muat ulang halaman lalu login lagi.';
        } else if (exception === 'timeout') {
            msg = 'Permintaan login timeout. Coba lagi.';
        } else if (exception === 'abort') {
            msg = 'Permintaan login dibatalkan.';
        } else {
            msg = 'Login gagal (HTTP ' + jqXHR.status + '). Silakan muat ulang halaman lalu coba lagi.';
        }
        showLoginError('Gagal Login', msg);
    };

    function showLoginLoader(message) {
        var $el = $('#overlay');
        if (!$el.length) return;

        if (message) {
            $el.find('.login-loader-text').html(
                message + '<span class="login-loader-dots" aria-hidden="true"><i></i><i></i><i></i></span>'
            );
        }

        $el.attr('aria-hidden', 'false');
        void $el[0].offsetWidth;
        $el.addClass('is-active');
    }

    function hideLoginLoader(callback) {
        var $el = $('#overlay');
        if (!$el.length) {
            if (typeof callback === 'function') callback();
            return;
        }

        $el.removeClass('is-active');
        $el.attr('aria-hidden', 'true');

        setTimeout(function () {
            if (typeof callback === 'function') callback();
        }, LOGIN_LOADER_MS);
    }

    function showLoginTab() {
        try {
            $('#tab-login').tab('show');
        } catch (e) {
        }
    }

    $(document).on('click', '#btn_cancel_register', function (e) {
        e.preventDefault();
        showLoginTab();
        try {
            $('#registerForm')[0].reset();
        } catch (err) {
        }
    });

    clearStaleCsrfCookieIfNeeded();

    function getBrowserLocation(timeoutMs) {
        return new Promise(function (resolve) {
            if (!navigator.geolocation) {
                resolve(null);
                return;
            }
            var finished = false;
            var timer = setTimeout(function () {
                if (finished) return;
                finished = true;
                resolve(null);
            }, timeoutMs || 8000);
            navigator.geolocation.getCurrentPosition(
                function (pos) {
                    if (finished) return;
                    finished = true;
                    clearTimeout(timer);
                    resolve({
                        latitude: pos.coords.latitude,
                        longitude: pos.coords.longitude
                    });
                },
                function () {
                    if (finished) return;
                    finished = true;
                    clearTimeout(timer);
                    resolve(null);
                },
                { enableHighAccuracy: true, timeout: timeoutMs || 8000, maximumAge: 30000 }
            );
        });
    }

    $(document).on('click', '#btnGoogleLogin', function (e) {
        var href = this.href;
        if (!href) return;
        e.preventDefault();
        getBrowserLocation(8000).then(function (loc) {
            try {
                if (loc) sessionStorage.setItem('hris_pending_gps', JSON.stringify(loc));
            } catch (_err) {}
            window.location.href = href;
        });
    });

    $(document).on('submit', '#loginForm', function (e) {
        e.preventDefault();
        var formEl = this;
        showLoginLoader('Meminta akses lokasi...');
        getBrowserLocation(8000).then(function (loc) {
            var form_data = new FormData(formEl);
            var csrfToken = syncLoginCsrfToken();
            if (csrfToken) {
                form_data.set('csrfmiddlewaretoken', csrfToken);
            }
            if (loc) {
                form_data.set('latitude', String(loc.latitude));
                form_data.set('longitude', String(loc.longitude));
                try { sessionStorage.setItem('hris_pending_gps', JSON.stringify(loc)); } catch (_err) {}
            }
            $.ajax({
                type: 'POST',
                url: '/management/admin/login_process',
                data: form_data,
                cache: false,
                contentType: false,
                processData: false,
                dataType: 'json',
                credentials: 'same-origin',
                headers: csrfToken ? { 'X-CSRFToken': csrfToken } : {},
                beforeSend: function () {
                    showLoginLoader('Login Process');
                },
                error: function (jqXHR, exception) {
                    hideLoginLoader(function () {
                        report_eror(jqXHR, exception);
                    });
                },
                success: function (data) {
                    hideLoginLoader(function () {
                        if (data.status === true) {
                            Swal.fire({
                                icon: 'success',
                                title: 'Berhasil Login',
                                text: data.message,
                                timer: 2000,
                                timerProgressBar: true,
                                showConfirmButton: false
                            });
                            setTimeout(function () {
                                location.reload();
                            }, 2000);
                        } else {
                            Swal.fire({
                                icon: 'error',
                                title: 'Gagal Login',
                                text: data.message,
                            }).then(function () {
                                location.reload();
                            });
                        }
                    });
                }
            });
        });
    });

    $(document).on('submit', '#registerForm', function (e) {
        e.preventDefault();
        var form_data = new FormData(this);
        var csrfToken = syncLoginCsrfToken();
        if (csrfToken) {
            form_data.set('csrfmiddlewaretoken', csrfToken);
        }
        $.ajax({
            type: 'POST',
            url: '/management/admin/register_account',
            data: form_data,
            cache: false,
            contentType: false,
            processData: false,
            dataType: 'json',
            credentials: 'same-origin',
            headers: csrfToken ? { 'X-CSRFToken': csrfToken } : {},
            beforeSend: function () {
                showLoginLoader('Register Process');
            },
            error: function (jqXHR, exception) {
                hideLoginLoader(function () {
                    report_eror(jqXHR, exception);
                });
            },
            success: function (data) {
                hideLoginLoader(function () {
                    if (data && data.status === true) {
                        Swal.fire({
                            icon: 'success',
                            title: 'Berhasil Register',
                            text: data.message || 'Account berhasil dibuat. Silakan login.',
                            timer: 2500,
                            timerProgressBar: true,
                            showConfirmButton: false
                        });
                        showLoginTab();
                        try {
                            $('#registerForm')[0].reset();
                        } catch (err) {
                        }
                    } else {
                        Swal.fire({
                            icon: 'error',
                            title: 'Gagal Register',
                            text: (data && data.message) ? data.message : 'Gagal membuat account.',
                        });
                    }
                });
            }
        });
    });
});
