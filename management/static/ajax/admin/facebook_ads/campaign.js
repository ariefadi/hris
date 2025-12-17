/**
 * Reference Ajax Traffic Per Campaign Js
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

    // Set default tanggal hari ini
    var today = new Date();
    var todayString = today.getFullYear() + '-' +
        String(today.getMonth() + 1).padStart(2, '0') + '-' +
        String(today.getDate()).padStart(2, '0');
    $('#tanggal_dari').val(todayString);
    $('#tanggal_sampai').val(todayString);
    $('#tanggal_dari').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    });
    $('#tanggal_sampai').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    });
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    $('#select_domain').select2({
        placeholder: '-- Pilih Domain --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    $('#btn_load_data').click(function (e) {
        e.preventDefault();
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#select_account").val() || '%';
        var data_account = selected_account ? selected_account : '';
        var selected_domain = $("#select_domain").val() || '%';
        var data_domain = selected_domain ? selected_domain : '%';
        if (tanggal_dari !== '' && tanggal_sampai !== '') {
            if (selected_account != "") {
                ads_site_list();
            }
            destroy_table_data_campaign_facebook()
            table_data_campaign_facebook(tanggal_dari, tanggal_sampai, data_account, data_domain)
        }
    });
});
function ads_site_list() {
    var selected_account = $("#select_account").val();
    return $.ajax({
        url: '/management/admin/ads_sites_list',
        type: 'GET',
        data: {
            selected_accounts: selected_account
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            if (response && response.status) {
                $('#select_domain')
                    .val(response.data)
                    .trigger('change');
            }
        },
        error: function (xhr, status, error) {
            report_eror(xhr, error);
        }
    });
}
function table_data_campaign_facebook(tanggal_dari, tanggal_sampai, data_account, data_domain) {
    $.ajax({
        url: '/management/admin/page_per_campaign_facebook?tanggal_dari=' + tanggal_dari + '&tanggal_sampai=' + tanggal_sampai + '&data_account=' + data_account + '&data_domain=' + data_domain,
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_campaign) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Traffic Per Campaign Facebook";
            $.each(data_campaign.data_campaign, function (index, value) {
                let data_cpr = value.cpr;
                let cpr_number = parseFloat(data_cpr)
                let cpr = cpr_number.toFixed(0).replace(',', '.');
                const frequency = Number(value?.frequency) || 0;
                const formattedFrequency = frequency.toFixed(1).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                var formattedDate = value.date || '-';
                if (value.date && value.date.match(/\d{4}-\d{2}-\d{2}/)) {
                    var months = [
                        'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                        'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'
                    ];
                    var date = new Date(value.date + 'T00:00:00');
                    var day = date.getDate();
                    var month = months[date.getMonth()];
                    var year = date.getFullYear();
                    formattedDate = day + ' ' + month + ' ' + year;
                }
                var event_data = '<tr>';
                event_data += '<td class="text-center" style="font-size: 12px;"><b>' + formattedDate + '</b></td>';
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-info" style="color: white;">' + value.account_name + '</span></td>';
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-danger" style="color: white;">' + value.domain + '</span></td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.spend).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.impressions).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.reach).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.clicks).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + formattedFrequency + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + cpr + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + value.cpc + '</td>';
                event_data += '</tr>';
                $("#table_data_campaign_facebook tbody").append(event_data);
            })
            $.each(data_campaign.total_campaign, function (index, value) {
                // Spend
                const spend = Number(value?.total_spend) || 0;
                const totalSpend = spend.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Impressions
                const impressions = Number(value?.total_impressions) || 0;
                const totalImpressions = impressions.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Reach
                const reach = Number(value?.total_reach) || 0;
                const totalReach = reach.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Clicks
                const clicks = Number(value?.total_click) || 0;
                const totalClicks = clicks.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Frequency
                const frequency = Number(value?.total_frequency) || 0;
                const totalFrequency = frequency.toFixed(1).replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // CPR
                let data_cpr = Number(value.total_cpr) || 0;
                data_cpr = data_cpr.toFixed(0).replace(',', '.');
                // CPC
                let data_cpc = Number(value.total_cpc) || 0;
                data_cpc = data_cpc.toFixed(0).replace(',', '.');   
                $('#total_spend').text(totalSpend);
                $('#total_impressions').text(totalImpressions);
                $('#total_reach').text(totalReach);
                $('#total_clicks').text(totalClicks);
                $('#total_frequency').text(totalFrequency);
                $('#total_cpr').text(data_cpr);
                // CPC
                $('#total_cpc').text(data_cpc);
            })
            $('#table_data_campaign_facebook').DataTable({
                "paging": true,
                "pageLength": 50,
                "lengthChange": true,
                "lengthMenu": [[10, 25, 50, 100, -1], [10, 25, 50, 100, "Semua"]],
                "searching": true,
                "ordering": true,
                responsive: false,
                dom: 'Blfrtip',
                searching: true,
                buttons: [
                    {
                        extend: 'excel',
                        filename: judul,
                        text: 'Download Excel',
                        title: judul,
                        messageTop: "laporan traffic per campaign facebook didownload pada "
                            + tanggal.getHours() + ":"
                            + tanggal.getMinutes() + " "
                            + tanggal.getDate() + "-"
                            + (tanggal.getMonth() + 1) + "-"
                            + tanggal.getFullYear(),
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
                            const colWidths = [10, 15, 15, 10, 10, 10, 10, 10]; // 💡 Sesuaikan berdasarkan % di HTML
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
                        messageBottom: "laporan traffic per campaign facebook didownload pada "
                            + tanggal.getHours() + ":"
                            + tanggal.getMinutes()
                            + " " + tanggal.getDate()
                            + "-" + (tanggal.getMonth() + 1)
                            + "-" + tanggal.getFullYear(),
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
                                    if (body[i][2]) body[i][2].alignment = 'left';
                                    if (body[i][3]) body[i][3].alignment = 'right';
                                    if (body[i][4]) body[i][4].alignment = 'right';
                                    if (body[i][5]) body[i][5].alignment = 'right';
                                    if (body[i][6]) body[i][6].alignment = 'right';
                                    if (body[i][7]) body[i][7].alignment = 'right';
                                }
                            }
                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]
                            // Manual width sesuai presentase kolom HTML (tanpa kolom terakhir)
                            doc.content[1].table.widths = ['10%', '15%', '15%', '10%', '10%', '10%', '10%', '10%'];
                        }
                    }
                ]
            });
        },
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

function destroy_table_data_campaign_facebook() {
    $('#table_data_campaign_facebook').dataTable().fnClearTable();
    $('#table_data_campaign_facebook').dataTable().fnDraw();
    $('#table_data_campaign_facebook').dataTable().fnDestroy();
}
