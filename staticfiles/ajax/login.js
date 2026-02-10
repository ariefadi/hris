$().ready(function () {

    report_eror = function (jqXHR, exception) {
        var msg = '';
        if (jqXHR.status === 0) {
            msg = 'TIDAK ADA KONEKSI.\n TOLONG HUBUNGI DEVELOPER';
        } else if (jqXHR.status == 404) {
            msg = 'Requested page not found. [404]';
        } else if (jqXHR.status == 500) {
            msg = 'Internal Server Error [500].';
        } else if (exception === 'parsererror') {
            msg = 'Requested JSON parse failed.';
        } else if (exception === 'timeout') {
            msg = 'Time out error.';
        } else if (exception === 'abort') {
            msg = 'Ajax request aborted.';
        } else {
            msg = 'Uncaught Error.\n' + jqXHR.responseText;
        }
        alert(msg);
    };

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
        $.ajax({
            type: 'POST',
            url: '/management/admin/login_process',
            data: form_data,
            cache: false,
            contentType: false,
            processData: false,
            error: report_eror,
            beforeSend: function () {
                $('#overlay').fadeIn(500);
            },
            success: function (data) {
                $('#overlay').fadeOut(500);
                if (data.status === true) {
                    Swal.fire({
                        icon: 'success',
                        title: 'Berhasil Login',
                        text: data.message,
                        timer: 2000,
                        timerProgressBar: true,
                        showConfirmButton: false
                    });
                    setTimeout(() => {
                        location.reload();
                    }, 2000);
                } else {
                    Swal.fire({
                        icon: 'error',
                        title: 'Gagal Login',
                        text: data.message,
                    }).then(() => {
                        location.reload();
                    });
                }
            }
        });
    });

    $(document).on('submit', '#registerForm', function (e) {
        e.preventDefault();
        var form_data = new FormData(this);
        $.ajax({
            type: 'POST',
            url: '/management/admin/register_account',
            data: form_data,
            cache: false,
            contentType: false,
            processData: false,
            error: report_eror,
            beforeSend: function () {
                $('#overlay').fadeIn(500);
            },
            success: function (data) {
                $('#overlay').fadeOut(500);
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
            }
        });
    });
});