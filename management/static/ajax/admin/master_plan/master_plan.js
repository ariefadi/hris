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
    table_data_master_plan();
});
function table_data_master_plan() {
    $.ajax({
        url: '/settings/users/page_master_plan',
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_master_plan) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Master Plan"
            $.each(data_master_plan.data_master_plan, function (index, value) {
                var url_detail = 'page_detail_master_plan/'+value.master_plan_id;
                var event_data = '<tr>';
                event_data += '<td class="text-center">' + (index + 1) + '</td>';
                event_data += '<td class="text-center">' + value.task_date + '</td>';
                event_data += '<td class="text-center">' + value.master_task_code + '</td>';
                event_data += '<td class="text-center">' + value.project_kategori.toUpperCase() + '</td>';
                event_data += '<td class="text-left">' + value.master_task_plan + '</td>';
                event_data += '<td class="text-center">' + value.submit_task + '</td>';
                event_data += '<td class="text-center">' + value.assign_task + '</td>';
                event_data += '<td class="text-center">' + value.urgency + '</td>';
                if(value.execute_status == 'in-progress'){
                    event_data += '<td class="text-center"><span class="badge badge-warning" style="color: white;">On-Progress</span></td>';
                }else if(value.execute_status == 'review'){
                    event_data += '<td class="text-center"><span class="badge badge-danger" style="color: white;">Under Review</span></td>';
                }else{
                    event_data += '<td class="text-center"><span class="badge badge-primary" style="color: white;">Resolved</span></td>';
                }
                event_data += '<td class="text-left">' + value.catatan + '</td>';
                event_data += '<td class="text-center">' + '<a href='+url_detail+' type="button" id="btnDetail" class="btn btn-success btn-xs"><i class="bi bi-pencil"></i></a>'  + '</td>';
                event_data += '</tr>';  
                $("#table_data_master_plan tbody").append(event_data);    
            })
            $('#table_data_master_plan').DataTable({  
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
                            columns: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],      // hanya kolom yang terlihat
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
                            const colWidths = [3, 10, 5, 5, 25, 10, 10, 5, 10, 22, 5]; // 💡 Sesuaikan berdasarkan % di HTML
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
                                    if (body[i][1]) body[i][1].alignment = 'center';
                                    if (body[i][2]) body[i][2].alignment = 'center';
                                    if (body[i][3]) body[i][3].alignment = 'center';
                                    if (body[i][4]) body[i][4].alignment = 'left';
                                    if (body[i][5]) body[i][5].alignment = 'center';
                                    if (body[i][6]) body[i][6].alignment = 'center';
                                    if (body[i][7]) body[i][7].alignment = 'center';
                                    if (body[i][8]) body[i][9].alignment = 'center';
                                    if (body[i][8]) body[i][9].alignment = 'left';
                                }
                            }
                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]
                            // Manual width sesuai presentase kolom HTML (tanpa kolom terakhir)
                            doc.content[1].table.widths = ['3%', '10%', '5%', '5%', '25%', '10%', '10%', '5%', '10%', '22%', '5%'];
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

