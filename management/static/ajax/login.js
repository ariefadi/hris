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

    $(document).on('submit', "form", function (e) {
        e.preventDefault();
        form_data = new FormData(this);
        // console.log(form_data)
        $.ajax({
            type: 'POST',
            url: '/management/admin/login_process',
            // dataType: 'json',
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
                    // Redirect to admin panel after 2 seconds
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
});

