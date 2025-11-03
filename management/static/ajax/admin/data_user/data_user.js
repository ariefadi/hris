/**
 * Reference Ajax Admin Data User Js
 */

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

    $('.select2').select2()
    $('#select_user_st_add').select2({
        dropdownParent: $('#add_user'),
        placeholder: '-- Pilih Status --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    $('#select_user_st_edit').select2({
        dropdownParent: $('#edit_user'),
        placeholder: '-- Pilih Status --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    // default reload data
    table_data_user();
});
function table_data_user() {
    $.ajax({
        url: '/settings/users/data/page',
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_user) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Data User"
            $.each(data_user.data_user, function (index, value) {
                var url_detail = 'page_detail_user/'+value.user_id;
                var event_data = '<tr>';
                event_data += '<td class="text-center">' + (index + 1) + '</td>';
                event_data += '<td class="text-left">' + value.user_alias + '</td>';
                event_data += '<td class="text-center">' + value.user_name + '</td>';
                event_data += '<td class="text-center">' + value.user_mail + '</td>';
                event_data += '<td class="text-center">' + value.user_telp + '</td>';
                event_data += '<td class="text-left">' + value.user_alamat + '</td>';
                if(value.user_st == '1'){
                    event_data += '<td class="text-center"><span class="badge badge-primary" style="color: white;">Aktif</span></td>';
                }else{
                    event_data += '<td class="text-center"><span class="badge badge-danger" style="color: white;">Tidak Aktif</span></td>';
                }
                event_data += '<td class="text-center">' +
                    '<button type="button" class="btn btn-warning btn-xs btn-edit-user me-1" data-user-id="' + value.user_id + '"><i class="bi bi-pencil"></i> Edit</button>'  +
                    '<button type="button" class="btn btn-danger btn-xs btn-delete-user" data-user-id="' + value.user_id + '"><i class="bi bi-trash"></i> Delete</button>' +
                    '</td>';
                event_data += '</tr>';  
                $("#table_data_user tbody").append(event_data);    
            })
            $('#table_data_user').DataTable({  
                "paging": true,
                "lengthChange": true,
                "searching": true,
                "ordering": true,
                "responsive": true,
                "scrollX": true,
                "autoWidth": false,
                dom: 'Bfrtip',
                searching: true,
                buttons: [
                    {
                        extend: 'excel',
                        filename: judul,
                        text: 'Download Excel',
                        title: judul,
                        messageTop: "laporan user didownload pada "
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()+" "
                                    +tanggal.getDate()+"-"
                                    +(tanggal.getMonth()+1)+"-"
                                    +tanggal.getFullYear(),
                        exportOptions: {
                            columns: ':visible', 
                            columns: [0, 1, 2, 3, 4, 5, 6],      // hanya kolom yang terlihat
                            modifier: {
                                search: 'applied',      // sesuai filter pencarian
                                order: 'applied'        // sesuai urutan saat itu
                            }
                        },
                        customize: function (xlsx) {
                            const sheet = xlsx.xl.worksheets['sheet1.xml'];
                            // =========================
                            // Set column width secara manual (unit: character width)
                            // =========================
                            const colWidths = [5, 25, 10, 15, 10, 25, 5]; // 💡 Sesuaikan berdasarkan % di HTML
                            const cols = $('cols', sheet);
                            cols.empty(); // Kosongkan default <col> dari DataTables
                            for (let i = 0; i < colWidths.length; i++) {
                                cols.append(
                                    `<col min="${i + 1}" max="${i + 1}" width="${colWidths[i]}" customWidth="1"/>`
                                );
                            }
                            
                        }
                    },
                    {
                        extend: 'pdf',
                        orientation: 'landscape',
                        pageSize: 'A4', 
                        filename: judul,
                        text: 'Download Pdf',
                        className: 'btn btn-warning',
                        title: judul,
                        messageBottom: "laporan user didownload pada "
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()
                                    +" "+tanggal.getDate()
                                    +"-"+(tanggal.getMonth()+1)
                                    +"-"+tanggal.getFullYear(),
                        exportOptions: {
                            columns: ':not(:last-child)' // ❌ Kecualikan kolom terakhir
                        },
                        customize: function (doc) {
                            // Header style (bold + center)
                            doc.styles.tableHeader = {
                                bold: true,
                                fontSize: 11,
                                color: 'black',
                                alignment: 'center'
                            };

                            // Ambil body tabel (data + header)
                            const body = doc.content[1].table.body;

                            // Loop dari baris kedua (index 1, karena index 0 adalah header)
                            for (let i = 1; i < body.length; i++) {
                                // Center kolom ke-1 (index 0) dan kolom ke-3 (index 2)
                                body[i][0].alignment = 'center';
                                body[i][1].alignment = 'left';
                                body[i][2].alignment = 'center';
                                body[i][3].alignment = 'center';
                                body[i][4].alignment = 'center';
                                body[i][5].alignment = 'left';
                                body[i][6].alignment = 'center';
                            }

                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]

                            // Manual width sesuai presentase kolom HTML (tanpa kolom terakhir)
                            doc.content[1].table.widths = ['5%', '25%', '10%', '15%', '10%', '25%', '5%'];
                        }
                    }
                ]
            });
        }
    });
}

function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}
const csrftoken = getCookie('csrftoken');

$('#simpan_data_user').on('click',function(e){
    var user_alias =  $("#user_alias_add").val();
    var user_name =  $("#user_name_add").val();
    var user_pass =  $("#user_pass_add").val();
    var user_mail =  $("#user_mail_add").val();
    var user_telp =  $("#user_telp_add").val();
    var user_alamat =  $("#user_alamat_add").val();
    var user_st =  $("#select_user_st_add option:selected").val();
    // alert(select_unit)
    let formData = new FormData();
    if( user_alias!="" && user_name!="" && user_pass!="" && user_mail!="" && user_st!="" ) {
        formData.append('user_alias',user_alias);
        formData.append('user_name',user_name);
        formData.append('user_pass',user_pass);
        formData.append('user_mail',user_mail);
        formData.append('user_telp',user_telp);
        formData.append('user_alamat',user_alamat);
        formData.append('user_st',user_st);
        $.ajax({
            type: 'POST',
            url: '/settings/users/data/create',
            data: formData,
            headers: { 
                "X-CSRFToken": csrftoken 
            },
            processData: false,
            contentType: false,
            dataType: "json",
            success: function (data) {
                if (data.status === true) {
                    Swal.fire({
                        icon: 'success',
                        title: 'Berhasil',
                        text: data.message,
                    }).then(() => {
                        location.reload();
                    });
                } else {
                    Swal.fire({
                        icon: 'error',
                        title: 'Gagal',
                        text: data.message,
                    }).then(() => {
                        location.reload();
                    });
                }
            }
        })
    }
})

// Handle edit user button click
$(document).on('click', '.btn-edit-user', function() {
    var user_id = $(this).data('user-id');
    window.location.href = '/settings/users/data/edit/' + user_id;
});

// Handle delete user button click with double confirmation
$(document).on('click', '.btn-delete-user', function() {
    var user_id = $(this).data('user-id');
    var $row = $(this).closest('tr');

    Swal.fire({
        icon: 'warning',
        title: 'Hapus User?',
        text: 'Apakah Anda yakin ingin menghapus user ini?',
        showCancelButton: true,
        confirmButtonText: 'Ya, hapus',
        cancelButtonText: 'Batal'
    }).then(function(firstConfirm) {
        if (!firstConfirm.isConfirmed) return;

        Swal.fire({
            title: 'Konfirmasi Hapus',
            html: 'Ketik <b>HAPUS</b> untuk melanjutkan.',
            input: 'text',
            inputPlaceholder: 'HAPUS',
            showCancelButton: true,
            confirmButtonText: 'Konfirmasi',
            cancelButtonText: 'Batal',
            preConfirm: function(value) {
                if (value !== 'HAPUS') {
                    Swal.showValidationMessage('Anda harus mengetik HAPUS');
                }
                return value;
            }
        }).then(function(secondConfirm) {
            if (!secondConfirm.isConfirmed || secondConfirm.value !== 'HAPUS') return;

            // Proceed to delete via AJAX
            let formData = new FormData();
            formData.append('user_id', user_id);
            $.ajax({
                url: '/settings/users/data/delete',
                type: 'POST',
                data: formData,
                headers: { "X-CSRFToken": csrftoken },
                processData: false,
                contentType: false,
                dataType: 'json',
                beforeSend: function() { $('#overlay').fadeIn(200); },
                complete: function() { $('#overlay').fadeOut(200); },
                success: function(response) {
                    if (response && response.status) {
                        Swal.fire({
                            icon: 'success',
                            title: 'Berhasil',
                            text: response.message
                        });
                        // Remove row from DataTable
                        try {
                            var table = $('#table_data_user').DataTable();
                            table.row($row).remove().draw();
                        } catch (e) {
                            // Fallback: reload page
                            location.reload();
                        }
                    } else {
                        Swal.fire({
                            icon: 'error',
                            title: 'Gagal',
                            text: (response && response.message) ? response.message : 'Gagal menghapus user'
                        });
                    }
                },
                error: function() {
                    Swal.fire({
                        icon: 'error',
                        title: 'Error',
                        text: 'Terjadi kesalahan saat menghapus user'
                    });
                }
            });
        });
    });
});

// Handle update user button click
$('#update_data_user').click(function() {
    update_user_data();
});

// Function to load user data for editing
function load_user_data(user_id) {
    $.ajax({
        url: '/settings/users/data/get/' + user_id,
        type: 'GET',
        success: function(response) {
            if (response && response.status) {
                var user = response.data;
                $('#user_id_edit').val(user.user_id);
                $('#user_alias_edit').val(user.user_alias);
                $('#user_name_edit').val(user.user_name);
                // Do not prefill password for security; leave empty
                $('#user_pass_edit').val('');
                $('#user_mail_edit').val(user.user_mail);
                $('#user_telp_edit').val(user.user_telp);
                $('#user_alamat_edit').val(user.user_alamat);
                $('#select_user_st_edit').val(user.user_st).trigger('change');
                $('#edit_user').modal('show');
            } else {
                Swal.fire({
                    icon: 'error',
                    title: 'Error!',
                    text: 'Gagal memuat data user'
                });
            }
        },
        error: function() {
            Swal.fire({
                icon: 'error',
                title: 'Error!',
                text: 'Terjadi kesalahan saat memuat data user'
            });
        }
    });
}

// Function to update user data
function update_user_data() {
    var user_id = $('#user_id_edit').val();
    var user_alias = $('#user_alias_edit').val();
    var user_name = $('#user_name_edit').val();
    var user_pass = $('#user_pass_edit').val();
    var user_mail = $('#user_mail_edit').val();
    var user_telp = $('#user_telp_edit').val();
    var user_alamat = $('#user_alamat_edit').val();
    var user_st = $('#select_user_st_edit').val();

    // Password optional during edit; do not require user_pass
    if (!user_alias || !user_name || !user_mail || !user_st) {
        Swal.fire({
            icon: 'warning',
            title: 'Peringatan!',
            text: 'Semua field wajib diisi!'
        });
        return;
    }

    let formData = new FormData();
    formData.append('user_id', user_id);
    formData.append('user_alias', user_alias);
    formData.append('user_name', user_name);
    if (user_pass && user_pass.trim() !== '') {
        formData.append('user_pass', user_pass);
    }
    formData.append('user_mail', user_mail);
    formData.append('user_telp', user_telp);
    formData.append('user_alamat', user_alamat);
    formData.append('user_st', user_st);

    $.ajax({
        url: '/settings/users/data/update',
        type: 'POST',
        data: formData,
        headers: { 
            "X-CSRFToken": csrftoken 
        },
        processData: false,
        contentType: false,
        dataType: "json",
        success: function(response) {
            if (response && response.status) {
                Swal.fire({
                    icon: 'success',
                    title: 'Berhasil!',
                    text: response.message
                }).then(function() {
                    $('#edit_user').modal('hide');
                    location.reload();
                });
            } else {
                Swal.fire({
                    icon: 'error',
                    title: 'Error!',
                    text: response.message
                });
            }
        },
        error: function() {
            Swal.fire({
                icon: 'error',
                title: 'Error!',
                text: 'Terjadi kesalahan saat mengupdate data user'
            });
        }
    });
}

