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

    var LOCATION_DENIED_HTML =
        '<p style="margin:0 0 10px;">Aplikasi mewajibkan akses lokasi browser sebelum Anda bisa masuk, meski username dan password sudah benar.</p>' +
        '<p style="margin:0 0 10px;">Klik <b>Coba Izinkan Lagi</b> untuk memunculkan permintaan izin lokasi dari browser, lalu pilih <b>Izinkan</b>.</p>' +
        '<p style="margin:0;color:#64748b;font-size:12.5px;line-height:1.55;">Jika popup browser tidak muncul, klik ikon gembok di address bar, ubah Location menjadi Allow, lalu coba lagi.</p>';

    function persistGps(lat, lng) {
        try {
            sessionStorage.setItem('hris_pending_gps', JSON.stringify({ latitude: lat, longitude: lng }));
        } catch (_err) {}
        try {
            var secure = window.location.protocol === 'https:' ? '; Secure' : '';
            document.cookie = 'hris_login_gps=' + encodeURIComponent(String(lat) + ',' + String(lng))
                + '; Path=/; Max-Age=600; SameSite=Lax' + secure;
        } catch (_err2) {}
    }

    function setLocationStatus(state, text) {
        var $box = $('#loginLocationStatus');
        if (!$box.length) return;
        $box.removeClass('is-pending is-denied is-granted is-unsupported').addClass('is-' + state);
        var $text = $('#loginLocationStatusText');
        if ($text.length && text) $text.text(text);
        var $icon = $box.children('i').first();
        if ($icon.length) {
            $icon.attr('class', state === 'granted' ? 'bi bi-geo-alt-fill' : 'bi bi-geo-alt-fill');
        }
    }

    function showLocationRequiredNotice(result) {
        var code = result && result.code;
        var title = 'Aktifkan Akses Lokasi';
        var html = LOCATION_DENIED_HTML;
        if (code === 'unsupported') {
            html = '<p style="margin:0;">Browser ini tidak mendukung akses lokasi. Gunakan Chrome, Edge, Firefox, atau Safari terbaru, lalu izinkan Location.</p>';
        } else if (code === 'timeout' || code === 'unavailable') {
            html = '<p style="margin:0 0 10px;">Lokasi tidak berhasil diperoleh. Pastikan GPS / layanan lokasi perangkat aktif.</p>' +
                '<p style="margin:0;">Klik <b>Coba Izinkan Lagi</b> untuk memunculkan permintaan izin lokasi dari browser.</p>';
        }
        if (typeof Swal !== 'undefined' && Swal.fire) {
            Swal.fire({
                icon: 'warning',
                title: title,
                html: html,
                confirmButtonText: 'Coba Izinkan Lagi',
                confirmButtonColor: '#ea580c',
                showLoaderOnConfirm: true,
                allowOutsideClick: function () { return !Swal.isLoading(); },
                allowEscapeKey: function () { return !Swal.isLoading(); },
                preConfirm: function () {
                    // Dipanggil langsung dari klik tombol agar gestur user tetap valid,
                    // sehingga browser bisa menampilkan popup izin lokasi lagi.
                    return getBrowserLocation(15000).then(function (loc) {
                        if (loc && loc.ok) {
                            return loc;
                        }
                        var msg = 'Izin lokasi belum diberikan. Pilih Izinkan pada popup browser.';
                        if (loc && loc.code === 'denied') {
                            msg = 'Popup izin belum diizinkan. Klik Coba Izinkan Lagi, lalu pilih Izinkan.';
                        } else if (loc && loc.code === 'timeout') {
                            msg = 'Waktu menunggu izin lokasi habis. Klik Coba Izinkan Lagi.';
                        }
                        Swal.showValidationMessage(msg);
                        return false;
                    });
                }
            }).then(function (res) {
                if (res && res.isConfirmed && res.value && res.value.ok) {
                    persistGps(res.value.latitude, res.value.longitude);
                    setLocationStatus('granted', 'Akses lokasi aktif. Silakan login.');
                    Swal.fire({
                        icon: 'success',
                        title: 'Lokasi diizinkan',
                        text: 'Silakan login kembali.',
                        timer: 1600,
                        showConfirmButton: false
                    });
                }
            });
            return;
        }
        alert('Aktifkan akses lokasi di browser terlebih dahulu sebelum masuk ke aplikasi.');
    }

    function getBrowserLocation(timeoutMs) {
        return new Promise(function (resolve) {
            if (!navigator.geolocation) {
                resolve({ ok: false, code: 'unsupported' });
                return;
            }
            var finished = false;
            var wait = timeoutMs || 15000;
            var timer = setTimeout(function () {
                if (finished) return;
                finished = true;
                resolve({ ok: false, code: 'timeout' });
            }, wait + 500);
            navigator.geolocation.getCurrentPosition(
                function (pos) {
                    if (finished) return;
                    finished = true;
                    clearTimeout(timer);
                    resolve({
                        ok: true,
                        latitude: pos.coords.latitude,
                        longitude: pos.coords.longitude
                    });
                },
                function (err) {
                    if (finished) return;
                    finished = true;
                    clearTimeout(timer);
                    var code = (err && err.code === 1) ? 'denied'
                        : (err && err.code === 3) ? 'timeout'
                        : 'unavailable';
                    resolve({ ok: false, code: code });
                },
                { enableHighAccuracy: true, timeout: wait, maximumAge: 0 }
            );
        });
    }

    function requestLocationPermission(fromNotice) {
        setLocationStatus('pending', 'Meminta izin lokasi browser...');
        return getBrowserLocation(15000).then(function (result) {
            if (result && result.ok) {
                persistGps(result.latitude, result.longitude);
                setLocationStatus('granted', 'Akses lokasi aktif. Silakan login.');
                return result;
            }
            var denied = result && result.code === 'denied';
            setLocationStatus(
                result && result.code === 'unsupported' ? 'unsupported' : 'denied',
                denied
                    ? 'Lokasi diblokir di browser. Aktifkan Location (Allow), lalu coba lagi.'
                    : 'Akses lokasi belum aktif. Izinkan Location di browser untuk masuk.'
            );
            if (fromNotice || denied) {
                showLocationRequiredNotice(result || { code: 'denied' });
            }
            return result;
        });
    }

    function refreshLocationPermissionState() {
        if (!navigator.geolocation) {
            setLocationStatus('unsupported', 'Browser tidak mendukung akses lokasi. Gunakan browser terbaru.');
            return;
        }
        if (!navigator.permissions || !navigator.permissions.query) return;
        try {
            navigator.permissions.query({ name: 'geolocation' }).then(function (status) {
                function apply(state) {
                    if (state === 'granted') {
                        setLocationStatus('granted', 'Akses lokasi aktif. Silakan login.');
                    } else if (state === 'denied') {
                        setLocationStatus('denied', 'Lokasi diblokir. Klik ikon gembok di address bar, izinkan Location, lalu login lagi.');
                    } else {
                        setLocationStatus('pending', 'Izinkan lokasi di browser sebelum masuk. Tanpa izin ini, login akan ditolak meskipun username dan password benar.');
                    }
                }
                apply(status.state);
                status.onchange = function () {
                    apply(status.state);
                };
            }).catch(function () {});
        } catch (_e) {}
    }

    refreshLocationPermissionState();

    $(document).on('click', '#btnEnableLocation', function (e) {
        e.preventDefault();
        requestLocationPermission(true);
    });

    $(document).on('click', '#btnGoogleLogin', function (e) {
        var href = this.href;
        if (!href) return;
        e.preventDefault();
        showLoginLoader('Meminta akses lokasi...');
        getBrowserLocation(15000).then(function (result) {
            if (!result || !result.ok) {
                hideLoginLoader(function () {
                    showLocationRequiredNotice(result || { code: 'denied' });
                });
                return;
            }
            persistGps(result.latitude, result.longitude);
            setLocationStatus('granted', 'Akses lokasi aktif. Mengalihkan ke Google...');
            window.location.href = href;
        });
    });

    $(document).on('submit', '#loginForm', function (e) {
        e.preventDefault();
        var formEl = this;
        showLoginLoader('Meminta akses lokasi...');
        getBrowserLocation(15000).then(function (result) {
            if (!result || !result.ok) {
                hideLoginLoader(function () {
                    showLocationRequiredNotice(result || { code: 'denied' });
                });
                return;
            }
            persistGps(result.latitude, result.longitude);
            setLocationStatus('granted', 'Akses lokasi aktif. Memproses login...');
            var form_data = new FormData(formEl);
            var csrfToken = syncLoginCsrfToken();
            if (csrfToken) {
                form_data.set('csrfmiddlewaretoken', csrfToken);
            }
            form_data.set('latitude', String(result.latitude));
            form_data.set('longitude', String(result.longitude));
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
                        } else if (data.code === 'LOCATION_REQUIRED') {
                            showLocationRequiredNotice({ code: 'denied', message: data.message });
                        } else {
                            Swal.fire({
                                icon: 'error',
                                title: 'Gagal Login',
                                text: data.message,
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
