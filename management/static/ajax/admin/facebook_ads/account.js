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
        minimumResultsForSearch: 0
    })
    // default reload data
    table_data_account_ads();
});
function table_data_account_ads() {
    $.ajax({
        url: '/management/admin/page_account_facebook',
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_account_ads) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Account Facebook Ads"
            $.each(data_account_ads.data_account_ads, function (index, value) {
                var url_detail = '/management/admin/edit_account_facebook/'+value.account_ads_id;
                var event_data = '<tr>';
                event_data += '<td class="text-center">' + (index + 1) + '</td>';
                event_data += '<td class="text-left">' + value.account_name + '</td>';
                event_data += '<td class="text-center">' + value.account_email + '</td>';
                event_data += '<td class="text-center">' + value.account_id + '</td>';
                event_data += '<td class="text-center">' + value.app_id + '</td>';
                event_data += '<td class="text-center">' + value.pemilik_account + '</td>';
                event_data += '<td class="text-center">' + '<a href='+url_detail+' type="button" id="btnEdit" class="btn btn-success btn-xs"><i class="bi bi-pencil"></i></a>'  + '</td>';
                event_data += '</tr>';  
                $("#table_data_account tbody").append(event_data);    
            })
            $('#table_data_account').DataTable({
                "responsive": true,
                "scrollX": true,
                "autoWidth": false,  
                "paging": true,
                "lengthChange": true,
                "searching": true,
                "ordering": true,
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
                            columns: [0, 1, 2, 3, 4, 5],      // hanya kolom yang terlihat
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
                            const colWidths = [5, 25, 15, 15, 15, 20]; // 💡 Sesuaikan berdasarkan % di HTML
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
                            }

                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]

                            // Manual width sesuai presentase kolom HTML (tanpa kolom terakhir)
                            doc.content[1].table.widths = ['5%', '25%', '15%', '15%', '15%', '20%'];
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

$('#simpan_data_account').on('click',function(e){
    var account_name = $("#account_name").val();
    var account_email =  $("#account_email").val();
    var account_id =  $("#account_id").val();
    var app_id =  $("#app_id").val();
    var app_secret =  $("#app_secret").val();
    var access_token =  $("#access_token").val();
    // alert(select_unit)
    let formData = new FormData();
    if( account_name!="" && account_email!="" && account_id!="" && app_id!="") {
        formData.append('account_name',account_name);
        formData.append('account_email',account_email);
        formData.append('account_id',account_id);
        formData.append('app_id',app_id);
        formData.append('app_secret',app_secret);
        formData.append('access_token',access_token);
        $.ajax({
            type: 'POST',
            url: '/management/admin/post_account_ads',
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