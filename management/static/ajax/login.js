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
        var token = readCsrfCookie();
        if (!token) {
            var $existing = $('input[name=csrfmiddlewaretoken]').first();
            token = String($existing.val() || '').trim();
        } else {
            var $input = $('input[name=csrfmiddlewaretoken]').first();
            if ($input.length) {
                $input.val(token);
            }
        }
        return token;
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
            showLoginError(
                'Sesi Form Kedaluwarsa',
                'Token keamanan login sudah tidak valid (biasanya setelah idle 15 menit atau tab dibiarkan terbuka lama). Halaman akan dimuat ulang — silakan login kembali.',
                {
                    icon: 'warning',
                    confirmButtonText: 'Muat Ulang',
                    onClose: function () {
                        window.location.reload();
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

    $(document).on('submit', '#loginForm', function (e) {
        e.preventDefault();
        var form_data = new FormData(this);
        var csrfToken = syncLoginCsrfToken();
        if (csrfToken) {
            form_data.set('csrfmiddlewaretoken', csrfToken);
        }
        $.ajax({
            type: 'POST',
            url: '/management/admin/login_process',
            data: form_data,
            cache: false,
            contentType: false,
            processData: false,
            dataType: 'json',
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
