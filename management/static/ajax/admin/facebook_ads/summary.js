/**
 * Reference Ajax Summary Facebook Ads Js
 */

function normalizeDomainFilter(selected_domain) {
    if (Array.isArray(selected_domain)) {
        return selected_domain.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
    }
    return String(selected_domain || '').trim();
}

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
    // select_domain sekarang freetext input (tanpa select2)
    $('#btn_load_data').click(function (e) {
        e.preventDefault();
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#select_account").val() || '%';
        var data_account = selected_account ? selected_account : '';
        var selected_domain = normalizeDomainFilter($("#select_domain").val());
        var data_domain = selected_domain ? selected_domain : '%';
        if (tanggal_dari !== '' && tanggal_sampai !== '') {
            destroy_table_data_campaign_facebook()
            table_data_campaign_facebook(tanggal_dari, tanggal_sampai, data_account, data_domain)
        }
    });
    // Filter silang account-domain dinonaktifkan karena domain menggunakan freetext.
});

function renderFacebookSummaryCharts(rows) {
    if (typeof Highcharts === 'undefined') return;
    const toNum = (v) => {
        const n = Number(String(v == null ? 0 : v).replace(/[^0-9.-]/g, ''));
        return isFinite(n) ? n : 0;
    };

    const byDate = {};
    const byAccount = {};

    (rows || []).forEach(function (r) {
        const d = String(r.date || '').slice(0, 10);
        const acc = String(r.account_name || '-');
        const spend = toNum(r.spend);
        const impressions = toNum(r.impressions);
        const clicks = toNum(r.clicks);

        if (!byDate[d]) byDate[d] = { spend: 0, impressions: 0, clicks: 0 };
        byDate[d].spend += spend;
        byDate[d].impressions += impressions;
        byDate[d].clicks += clicks;

        if (!byAccount[acc]) byAccount[acc] = { spend: 0, impressions: 0, clicks: 0 };
        byAccount[acc].spend += spend;
        byAccount[acc].impressions += impressions;
        byAccount[acc].clicks += clicks;
    });

    const dates = Object.keys(byDate).sort();
    const dateLabels = dates.map(d => {
        const p = d.split('-');
        return (p.length === 3) ? `${p[2]}-${p[1]}-${p[0]}` : d;
    });

    if (window.__fbSummaryLineChart && typeof window.__fbSummaryLineChart.destroy === 'function') {
        window.__fbSummaryLineChart.destroy();
    }
    window.__fbSummaryLineChart = Highcharts.chart('facebookSummaryLineChart', {
        chart: { type: 'spline' },
        title: { text: 'Pergerakan Spend, Impresi, dan Klik per Tanggal' },
        xAxis: { categories: dateLabels },
        yAxis: [{ title: { text: 'Nilai' } }],
        tooltip: { shared: true },
        series: [
            { name: 'Spend', data: dates.map(d => byDate[d].spend) },
            { name: 'Impresi', data: dates.map(d => byDate[d].impressions) },
            { name: 'Klik', data: dates.map(d => byDate[d].clicks) }
        ]
    });

    const accounts = Object.keys(byAccount)
        .map(k => ({ account: k, spend: byAccount[k].spend, impressions: byAccount[k].impressions, clicks: byAccount[k].clicks }))
        .sort((a, b) => b.spend - a.spend);

    if (window.__fbSummaryAccountBarChart && typeof window.__fbSummaryAccountBarChart.destroy === 'function') {
        window.__fbSummaryAccountBarChart.destroy();
    }
    window.__fbSummaryAccountBarChart = Highcharts.chart('facebookSummaryAccountBarChart', {
        chart: { type: 'column' },
        title: { text: 'Perbandingan Spend, Impresi, dan Klik per Akun' },
        xAxis: { categories: accounts.map(x => x.account), labels: { rotation: -20 } },
        yAxis: [{ title: { text: 'Nilai' } }],
        tooltip: { shared: true },
        series: [
            { name: 'Spend', data: accounts.map(x => x.spend) },
            { name: 'Impresi', data: accounts.map(x => x.impressions) },
            { name: 'Klik', data: accounts.map(x => x.clicks) }
        ]
    });
}

function renderFacebookMonitoringCampaignTable(rows) {
    const $table = $('#table_monitoring_campaign_facebook');
    const $tbody = $('#table_monitoring_campaign_facebook tbody');
    if (!$table.length) return;
    if ($.fn.DataTable.isDataTable('#table_monitoring_campaign_facebook')) {
        $table.DataTable().clear().destroy();
    }
    $tbody.empty();

    const toNum = (v) => {
        const n = Number(String(v == null ? 0 : v).replace(/[^0-9.-]/g, ''));
        return isFinite(n) ? n : 0;
    };
    (rows || []).forEach(function (r) {
        const spend = toNum(r.spend);
        const budget = toNum(r.daily_budget);
        const status = String(r.campaign_status || '-');
        const remarkRaw = String(r.remark || '').toLowerCase();
        const remarkClass = (remarkRaw.includes('overspend') || remarkRaw.includes('paused')) ? 'badge badge-danger' : 'badge badge-success';
        const remarkText = r.remark || 'Normal';
        $tbody.append(
            '<tr>' +
            '<td>' + (r.account_name || '-') + '</td>' +
            '<td>' + (r.campaign || '-') + '</td>' +
            '<td class="text-right">Rp ' + spend.toLocaleString('id-ID') + '</td>' +
            '<td class="text-right">Rp ' + budget.toLocaleString('id-ID') + '</td>' +
            '<td class="text-center"><span class="badge badge-secondary">' + status + '</span></td>' +
            '<td class="text-center"><span class="' + remarkClass + '">' + remarkText + '</span></td>' +
            '</tr>'
        );
    });

    $table.DataTable({
        paging: true,
        pageLength: 10,
        searching: true,
        ordering: true,
        info: true,
        order: [[2, 'desc']],
        language: {
            lengthMenu: 'Tampilkan _MENU_ data',
            search: 'Cari:',
            info: 'Menampilkan _START_ - _END_ dari _TOTAL_ data',
            infoEmpty: 'Tidak ada data',
            zeroRecords: 'Data tidak ditemukan',
            paginate: { previous: 'Sebelumnya', next: 'Berikutnya' }
        }
    });
}

function table_data_campaign_facebook(tanggal_dari, tanggal_sampai, data_account, data_domain) {
    $.ajax({
        url: '/management/admin/page_summary_facebook?tanggal_dari=' + encodeURIComponent(tanggal_dari) + '&tanggal_sampai=' + encodeURIComponent(tanggal_sampai) + '&data_account=' + encodeURIComponent(data_account) + '&data_domain=' + encodeURIComponent(data_domain),
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_campaign) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Traffic Per Campaign Facebook";

            window.__facebookCampaignRows = (data_campaign && data_campaign.data_campaign) ? data_campaign.data_campaign : [];

            $.each(window.__facebookCampaignRows, function (index, value) {
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
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-danger" style="color: white;">' + (value.campaign || '-') + '</span></td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.spend).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.impressions).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.reach).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.clicks).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + formattedFrequency + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + cpr + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + value.cpc + '</td>';
                event_data += '<td class="text-center no-export" style="font-size: 12px;">'
                    + '<button type="button" class="btn btn-sm btn-outline-primary btn-facebook-campaign-detail" data-row-index="' + index + '" title="Detail">'
                    + '<i class="bi bi-eye-fill" aria-hidden="true"></i>'
                    + '</button>'
                    + '</td>';
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
                $('#total_cpc').text(data_cpc);

                // Summary box (di atas tabel)
                $('#summary_total_spend').text('Rp ' + totalSpend);
                $('#summary_total_impressions').text(totalImpressions);
                $('#summary_total_clicks').text(totalClicks);
                $('#summary_total_cpc').text('Rp ' + data_cpc);
            })
            $('#table_data_campaign_facebook').DataTable({
                columnDefs: [
                    { targets: -1, orderable: false, searchable: false }
                ],
                "paging": true,
                "pageLength": 10,
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
                            columns: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],      // tanpa kolom Detail
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
                            const colWidths = [10, 15, 15, 10, 10, 10, 10, 10, 10, 10]; // 💡 Sesuaikan berdasarkan % di HTML
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
                                    if (body[i][8]) body[i][8].alignment = 'right';
                                    if (body[i][9]) body[i][9].alignment = 'right';
                                }
                            }
                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]
                            // Manual width sesuai presentase kolom HTML (tanpa kolom Detail)
                            doc.content[1].table.widths = ['10%', '15%', '15%', '10%', '10%', '10%', '10%', '10%', '10%', '10%'];
                        }
                    }
                ]
            });

            renderFacebookSummaryCharts(window.__facebookCampaignRows);
            renderFacebookMonitoringCampaignTable((data_campaign && data_campaign.monitoring_campaign) ? data_campaign.monitoring_campaign : []);

            $('#table_data_campaign_facebook tbody')
                .off('click', '.btn-facebook-campaign-detail')
                .on('click', '.btn-facebook-campaign-detail', function () {
                    var idx = parseInt($(this).attr('data-row-index') || '0', 10);
                    var row = (window.__facebookCampaignRows || [])[idx] || {};

                    function fmtInt(v) {
                        return (Number(v || 0)).toLocaleString('id-ID');
                    }
                    function fmtIdr(v) {
                        var n = Number(v || 0);
                        return 'Rp ' + Math.round(n).toLocaleString('id-ID');
                    }

                    $('#facebookCampaignDetailDate').text(row.date || '-');
                    $('#facebookCampaignDetailAccount').text(row.account_name || '-');
                    $('#facebookCampaignDetailDomain').text(row.domain || '-');
                    $('#facebookCampaignDetailCampaign').text(row.campaign || '-');

                    $('#facebookCampaignDetailSpend').text(fmtIdr(row.spend));
                    $('#facebookCampaignDetailImpressions').text(fmtInt(row.impressions));
                    $('#facebookCampaignDetailReach').text(fmtInt(row.reach));
                    $('#facebookCampaignDetailClicks').text(fmtInt(row.clicks));

                    var freq = Number(row.frequency || 0);
                    $('#facebookCampaignDetailFrequency').text(isNaN(freq) ? '0' : freq.toFixed(1));

                    $('#facebookCampaignDetailCpr').text(fmtIdr(row.cpr));
                    $('#facebookCampaignDetailCpc').text(fmtIdr(row.cpc));

                    $('#facebookCampaignDetailLpv').text(fmtInt(row.lpv));
                    var lr = Number(row.lpv_rate || 0);
                    $('#facebookCampaignDetailLpvRate').text((isNaN(lr) ? 0 : lr).toFixed(2) + '%');

                    $('#facebookCampaignDetailModal').modal('show');
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
    if ($.fn.DataTable.isDataTable('#table_data_campaign_facebook')) {
        $('#table_data_campaign_facebook').DataTable().clear().destroy();
    }
    $('#table_data_campaign_facebook tbody').empty();
    if ($.fn.DataTable.isDataTable('#table_monitoring_campaign_facebook')) {
        $('#table_monitoring_campaign_facebook').DataTable().clear().destroy();
    }
    $('#table_monitoring_campaign_facebook tbody').empty();
    if (window.__fbSummaryLineChart && typeof window.__fbSummaryLineChart.destroy === 'function') {
        window.__fbSummaryLineChart.destroy();
        window.__fbSummaryLineChart = null;
    }
    if (window.__fbSummaryAccountBarChart && typeof window.__fbSummaryAccountBarChart.destroy === 'function') {
        window.__fbSummaryAccountBarChart.destroy();
        window.__fbSummaryAccountBarChart = null;
    }
}
