/**
 * Reference Ajax Admin Login User Js
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
    // default reload data
    table_data_login_user();
});
function table_data_login_user() {
    $.ajax({
        url: '/settings/users/page_login_user',
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_login_user) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Data Login User"
            $.each(data_login_user.data_login_user, function (index, value) {
                // Format tanggal login
                let login_day = dayjs(value.login_day).format("DD-MM-YYYY");
                let login_time = dayjs(value.login_date).format("HH:mm:ss");
                // Jika logout_date tidak ada, gunakan 'Belum Logout'
                let logout_day = value.logout_date ? dayjs(value.logout_date).format("DD-MM-YYYY") : 'Belum <br> Logout';
                let logout_time = value.logout_date ? dayjs(value.logout_date).format("HH:mm:ss") : '--:--';
                var event_data = '<tr>';
                event_data += '<td class="text-center">' + (index + 1) + '</td>';
                event_data += '<td class="text-left">' + value.user_alias + '</td>';
                event_data += '<td class="text-center"><span class="badge badge-info" style="color: white;">' + login_day + '</span></td>';
                event_data += '<td class="text-center">' + login_time + '</td>';
                event_data += '<td class="text-center"><span class="badge badge-warning" style="color: white;">' + value.ip_address + '</span></td>';
                event_data += '<td class="text-left">' + value.lokasi + '</td>';
                event_data += '<td class="text-center"><span class="badge badge-danger" style="color: white;">' + logout_day + '</span></td>';
                event_data += '<td class="text-center">' + logout_time + '</td>';
                event_data += '</tr>';  
                $("#table_data_login_user tbody").append(event_data);    
            })
            $('#table_data_login_user').DataTable({  
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
                            columns: [0, 1, 2, 3, 4, 5, 6, 7],      // hanya kolom yang terlihat
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
                            const colWidths = [5, 20, 10, 5, 10, 25, 10, 5]; // 💡 Sesuaikan berdasarkan % di HTML
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
                                if (body[i]) {
                                    if (body[i][0]) body[i][0].alignment = 'center';
                                    if (body[i][1]) body[i][1].alignment = 'left';
                                    if (body[i][2]) body[i][2].alignment = 'center';
                                    if (body[i][3]) body[i][3].alignment = 'center';
                                    if (body[i][4]) body[i][4].alignment = 'left';
                                    if (body[i][5]) body[i][5].alignment = 'center';
                                    if (body[i][6]) body[i][6].alignment = 'center';
                                    if (body[i][7]) body[i][7].alignment = 'center';
                                }
                            }
                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]
                            // Manual width sesuai presentase kolom HTML (tanpa kolom terakhir)
                            doc.content[1].table.widths = ['5%', '20%', '10%', '5%', '10%', '25%', '10%', '5%'];
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

