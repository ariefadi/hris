/**
 * Reference Ajax AdX Traffic Per Account
 */

function normalizeDomainFilter(selected_domain) {
    if (Array.isArray(selected_domain)) {
        return selected_domain.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
    }
    return String(selected_domain || '').trim();
}

function isAdxDarkTheme() {
    return document.documentElement.getAttribute('data-theme') === 'dark';
}

function getAdxChartTheme() {
    var dark = isAdxDarkTheme();
    return {
        text: dark ? '#e2e8f0' : '#334155',
        muted: dark ? '#94a3b8' : '#64748b',
        grid: dark ? 'rgba(148, 163, 184, 0.12)' : 'rgba(15, 23, 42, 0.08)',
        tooltipBg: dark ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.98)',
        tooltipBorder: dark ? 'rgba(255,255,255,0.1)' : 'rgba(15, 23, 42, 0.1)'
    };
}

function showAdxTrafficLoader(message) {
    var msg = String(message || 'Memuat data traffic AdX...').trim();
    if (window.HrisLoader && typeof window.HrisLoader.show === 'function') {
        window.HrisLoader.show(msg);
        return;
    }
    var $overlay = $('#overlay');
    if ($overlay.length) {
        $overlay.attr('data-loader-message', msg);
        $overlay.show();
    }
}

function hideAdxTrafficLoader() {
    if (window.HrisLoader && typeof window.HrisLoader.forceHide === 'function') {
        window.HrisLoader.forceHide();
        return;
    }
    $('#overlay').hide();
}

function showAdxTrafficResults() {
    $('#adxTrafficEmptyState').hide();
    $('#adxTrafficResults').show();
}

function hideAdxTrafficResults() {
    $('#adxTrafficResults').hide();
    $('#adxTrafficEmptyState').show();
}

var ADX_ACCOUNT_CHART_VISIBLE_KEY = 'adxTrafficAccountChartVisible';

function isAccountChartVisible() {
    try {
        return localStorage.getItem(ADX_ACCOUNT_CHART_VISIBLE_KEY) !== '0';
    } catch (e) {
        return true;
    }
}

function reflowAccountChart() {
    if (adxTrafficRevenueChart && typeof adxTrafficRevenueChart.reflow === 'function') {
        try { adxTrafficRevenueChart.reflow(); } catch (e) { }
    }
}

function setAccountChartVisible(visible, animate) {
    window.__adxAccountChartVisible = !!visible;
    try {
        localStorage.setItem(ADX_ACCOUNT_CHART_VISIBLE_KEY, visible ? '1' : '0');
    } catch (e) { }

    var $section = $('#revenue_chart_row');
    var $wrap = $section.find('.adx-chart-wrap');
    var $btn = $('#btnToggleAccountChart');
    if (!$section.length || !$wrap.length || !$btn.length) return;

    $btn.attr('aria-expanded', visible ? 'true' : 'false');
    if (visible) {
        $btn.html('<i class="fas fa-eye-slash" aria-hidden="true"></i> Sembunyikan Grafik');
        $section.removeClass('chart-collapsed');
    } else {
        $btn.html('<i class="fas fa-eye" aria-hidden="true"></i> Tampilkan Grafik');
        $section.addClass('chart-collapsed');
    }

    if (animate) {
        if (visible) {
            $wrap.stop(true, true).slideDown(200, reflowAccountChart);
        } else {
            $wrap.stop(true, true).slideUp(200);
        }
        return;
    }

    $wrap.toggle(visible);
    if (visible) reflowAccountChart();
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
    if (window.HrisDatepicker) {
        HrisDatepicker.initRange('#tanggal_dari', '#tanggal_sampai');
    }
    // Initialize Select2 for account filter
    $('#account_filter').select2({
        placeholder: '-- Pilih Account Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    // Select2 untuk Filter Subdomain: searchable + freetext (tagging) + AJAX suggest
    $('#domain_filter').select2({
        placeholder: 'ketik subdomain…',
        allowClear: true,
        width: '100%',
        theme: 'bootstrap4',
        tags: true,
        tokenSeparators: [','],
        minimumInputLength: 1,
        ajax: {
            url: '/management/admin/adx_domain_suggest',
            dataType: 'json',
            delay: 250,
            data: function (params) {
                var selected_account = $('#account_filter').val() || [];
                return {
                    q: params.term || '',
                    start_date: $('#tanggal_dari').val() || '',
                    end_date: $('#tanggal_sampai').val() || '',
                    selected_account: (selected_account && selected_account.length) ? selected_account.join(',') : ''
                };
            },
            processResults: function (data) {
                return { results: (data && data.results) ? data.results : [] };
            },
            cache: true
        },
        createTag: function (params) {
            var term = $.trim(params.term || '');
            if (!term) return null;
            return { id: term, text: term, newTag: true };
        }
    });
    // Load sites list on page load
    hideAdxTrafficResults();

    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domain = normalizeDomainFilter($("#domain_filter").val());
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            showAdxTrafficLoader();
            load_adx_traffic_account_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    $('#btnToggleAccountChart').on('click', function (e) {
        e.preventDefault();
        setAccountChartVisible(!window.__adxAccountChartVisible, true);
    });
    // Filter silang account-domain dinonaktifkan karena domain menggunakan freetext.
});
function load_adx_traffic_account_data(tanggal_dari, tanggal_sampai, selected_account, selectedDomains) {
    // Convert array to comma-separated string for backend
    var accountFilter = '';
    if (selected_account && selected_account.length > 0) {
        accountFilter = selected_account.join(',');
    }
    var domainFilter = normalizeDomainFilter(selectedDomains);
    if ($.fn.DataTable.isDataTable('#table_traffic_account')) {
        $('#table_traffic_account').DataTable().destroy();
    }
    $.ajax({
        url: '/management/admin/page_adx_traffic_account',
        type: 'GET',
        data: {
            'start_date': tanggal_dari,
            'end_date': tanggal_sampai,
            'selected_account': accountFilter,
            'selected_domains': domainFilter
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            hideAdxTrafficLoader();
            if (response && response.status) {
                if (window.HrisLastUpdate) {
                    window.HrisLastUpdate.set('#hrisLastUpdateValue', response.last_update || '');
                }
                showAdxTrafficResults();
                updateSummaryBoxes(response.summary);
                initializeDataTable(response.data);
                if (response.data && response.data.length > 0) {
                    create_revenue_line_chart(response.data);
                    $('#revenue_chart_row').show();
                    $('#btnToggleAccountChart').show();
                    setAccountChartVisible(isAccountChartVisible(), false);
                } else {
                    $('#revenue_chart_row').hide();
                    $('#btnToggleAccountChart').hide();
                }
                showSuccessMessage('Data traffic berhasil dimuat.');
            } else {
                hideAdxTrafficResults();
                alert('Error: ' + (response.error || 'Terjadi kesalahan yang tidak diketahui'));
            }
        },
        error: function (xhr, status, error) {
            hideAdxTrafficLoader();
            hideAdxTrafficResults();
            report_eror(xhr, status);
        }
    });
}
// Fungsi untuk update summary boxes
function updateSummaryBoxes(data) {
    data = data || {};
    $("#total_impressions").text(formatNumber(data.total_impressions || 0));
    $("#total_clicks").text(formatNumber(data.total_clicks || 0));
    $("#avg_cpc").text(formatCurrencyIDR(data.avg_cpc || 0));
    $("#avg_ctr").text(formatNumber(data.avg_ctr || 0, 2) + '%');
    $("#total_revenue").text(formatCurrencyIDR(data.total_revenue || 0));
}

function getSelectedTextList(selector) { 
    var $el = $(selector);
    var items = [];
    try {
        var s2 = $el.select2('data');
        if (Array.isArray(s2) && s2.length) {
            items = s2.map(function (d) {
                return d && d.text ? String(d.text) : '';
            });
        }
    } catch (e) {
        items = [];
    }
    if (!items || items.length === 0) {
        try {
            items = $el.find('option:selected').map(function () {
                return $(this).text();
            }).get();
        } catch (e) {
            items = [];
        }
    }
    return (items || []).map(function (t) {
        return String(t || '').trim();
    }).filter(function (t) {
        return t;
    });
}

function escapeHtml(text) {
    return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function escapeXmlText(text) {
    return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function formatDateID(d) {
    if (!d) return '-';
    var months = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
    try {
        var date = new Date(d + 'T00:00:00');
        return date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear();
    } catch (e) {
        return d;
    }
}

function getExportMetaTrafficAccount() {
    var start = $('#tanggal_dari').val();
    var end = $('#tanggal_sampai').val();
    var titleText = 'Traffic AdX Per Account';
    var periodText = 'Periode: ' + formatDateID(start) + ' s/d ' + formatDateID(end);

    var accounts = getSelectedTextList('#account_filter');
    var domainsRaw = String($('#domain_filter').val() || '').trim();
    var domains = domainsRaw ? domainsRaw.split(',').map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }) : [];

    return {
        titleText: titleText,
        periodText: periodText,
        accountText: accounts.length ? ('Account: ' + accounts.join(', ')) : '',
        domainText: domains.length ? ('Domain: ' + domains.join(', ')) : '',
        filenameBase: 'adx_traffic_account_' + String(start || 'data').replace(/-/g, '') + '_' + String(end || 'data').replace(/-/g, '')
    };
}

function stripHtmlExport(text) {
    return String(text || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function getAdxTrafficAccountExportHeaders() {
    return ['No', 'Tanggal', 'Site Name', 'Klik', 'CPC (Rp)', 'eCPM (Rp)', 'CTR (%)', 'Pendapatan (Rp)'];
}

function collectAdxTrafficAccountExportData(dt) {
    var rows = [];
    if (!dt) return rows;

    var no = 0;
    dt.rows({ search: 'applied', order: 'applied' }).every(function () {
        var data = this.data();
        if (!data || !data.length) return;
        no += 1;
        rows.push([
            String(no),
            stripHtmlExport(data[0]),
            stripHtmlExport(data[1]),
            stripHtmlExport(data[2]),
            stripHtmlExport(data[3]),
            stripHtmlExport(data[4]),
            stripHtmlExport(data[5]),
            stripHtmlExport(data[6])
        ]);
    });
    return rows;
}

function downloadAdxTrafficAccountBlob(filename, mime, content) {
    var blob = new Blob([content], { type: mime });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
}

function escExportCell(v) {
    return String(v || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function exportAdxTrafficAccountExcel() {
    var dt = window.adxTrafficAccountDt;
    if (!dt) {
        alert('Muat data terlebih dahulu.');
        return;
    }

    var meta = getExportMetaTrafficAccount();
    var headers = getAdxTrafficAccountExportHeaders();
    var dataRows = collectAdxTrafficAccountExportData(dt);
    if (!dataRows.length) {
        alert('Tidak ada data untuk diekspor.');
        return;
    }

    var html = '<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel">';
    html += '<head><meta charset="UTF-8"><!--[if gte mso 9]><xml><x:ExcelWorkbook><x:ExcelWorksheets><x:ExcelWorksheet>';
    html += '<x:Name>Traffic AdX</x:Name><x:WorksheetOptions><x:DisplayGridlines/></x:WorksheetOptions>';
    html += '</x:ExcelWorksheet></x:ExcelWorksheets></x:ExcelWorkbook></xml><![endif]--></head><body>';
    html += '<h3>' + escExportCell(meta.titleText) + '</h3>';
    html += '<p>' + escExportCell(meta.periodText) + '</p>';
    if (meta.accountText) html += '<p>' + escExportCell(meta.accountText) + '</p>';
    if (meta.domainText) html += '<p>' + escExportCell(meta.domainText) + '</p>';
    html += '<table border="1" cellspacing="0" cellpadding="4" style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:11pt;">';
    html += '<thead><tr style="background:#1e3a5f;color:#fff;font-weight:bold;">';
    headers.forEach(function (h) {
        html += '<th style="border:1px solid #94a3b8;padding:6px;">' + escExportCell(h) + '</th>';
    });
    html += '</tr></thead><tbody>';

    dataRows.forEach(function (cells, idx) {
        var style = idx % 2 === 0 ? 'background:#ffffff;' : 'background:#f8fafc;';
        html += '<tr style="' + style + '">';
        cells.forEach(function (cell, cellIdx) {
            var align = cellIdx <= 2 ? (cellIdx === 0 ? 'center' : 'left') : 'right';
            html += '<td style="border:1px solid #cbd5e1;padding:5px;text-align:' + align + ';">' + escExportCell(cell) + '</td>';
        });
        html += '</tr>';
    });

    html += '</tbody></table></body></html>';
    downloadAdxTrafficAccountBlob(meta.filenameBase + '.xls', 'application/vnd.ms-excel;charset=utf-8', '\ufeff' + html);
}

function exportAdxTrafficAccountPdf() {
    var dt = window.adxTrafficAccountDt;
    if (!dt) {
        alert('Muat data terlebih dahulu.');
        return;
    }
    if (!(window.pdfMake && typeof pdfMake.createPdf === 'function')) {
        alert('PDF export tidak tersedia.');
        return;
    }

    var meta = getExportMetaTrafficAccount();
    var headers = getAdxTrafficAccountExportHeaders();
    var dataRows = collectAdxTrafficAccountExportData(dt);
    if (!dataRows.length) {
        alert('Tidak ada data untuk diekspor.');
        return;
    }

    var tableBody = [
        headers.map(function (h) {
            return { text: h, style: 'tableHeader', alignment: 'center' };
        })
    ];

    dataRows.forEach(function (cells, idx) {
        tableBody.push(cells.map(function (cell, cellIdx) {
            var item = {
                text: cell,
                style: 'dataCell',
                alignment: cellIdx === 0 ? 'center' : (cellIdx <= 2 ? 'left' : 'right')
            };
            if (idx % 2 === 1) item.fillColor = '#f8fafc';
            return item;
        }));
    });

    var content = [
        { text: meta.titleText, style: 'title', alignment: 'center', margin: [0, 0, 0, 4] },
        { text: meta.periodText, style: 'subtitle', alignment: 'center', margin: [0, 0, 0, 4] }
    ];
    if (meta.accountText) {
        content.push({ text: meta.accountText, style: 'subtitle', alignment: 'center', margin: [0, 0, 0, 2] });
    }
    if (meta.domainText) {
        content.push({ text: meta.domainText, style: 'subtitle', alignment: 'center', margin: [0, 0, 0, 8] });
    } else {
        content[content.length - 1].margin = [0, 0, 0, 10];
    }
    content.push({
        table: {
            headerRows: 1,
            widths: [24, 68, '*', 42, 58, 58, 42, 68],
            body: tableBody
        },
        layout: {
            hLineWidth: function () { return 0.6; },
            vLineWidth: function () { return 0.6; },
            hLineColor: function () { return '#94a3b8'; },
            vLineColor: function () { return '#94a3b8'; },
            paddingLeft: function () { return 5; },
            paddingRight: function () { return 5; },
            paddingTop: function () { return 4; },
            paddingBottom: function () { return 4; }
        }
    });

    pdfMake.createPdf({
        pageOrientation: 'landscape',
        pageSize: 'A4',
        pageMargins: [18, 28, 18, 28],
        content: content,
        styles: {
            title: { fontSize: 13, bold: true },
            subtitle: { fontSize: 9, color: '#475569' },
            tableHeader: { bold: true, fontSize: 8, fillColor: '#1e3a5f', color: '#ffffff' },
            dataCell: { fontSize: 7.5 }
        },
        defaultStyle: { font: 'Roboto' }
    }).download(meta.filenameBase + '.pdf');
}

function exportAdxTrafficAccountCsv() {
    var dt = window.adxTrafficAccountDt;
    if (!dt) {
        alert('Muat data terlebih dahulu.');
        return;
    }

    var meta = getExportMetaTrafficAccount();
    var headers = getAdxTrafficAccountExportHeaders();
    var dataRows = collectAdxTrafficAccountExportData(dt);
    if (!dataRows.length) {
        alert('Tidak ada data untuk diekspor.');
        return;
    }

    function csvEscape(v) {
        var s = String(v || '');
        if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
        return s;
    }

    var lines = [
        csvEscape(meta.titleText),
        csvEscape(meta.periodText)
    ];
    if (meta.accountText) lines.push(csvEscape(meta.accountText));
    if (meta.domainText) lines.push(csvEscape(meta.domainText));
    lines.push('');
    lines.push(headers.map(csvEscape).join(','));
    dataRows.forEach(function (cells) {
        lines.push(cells.map(csvEscape).join(','));
    });

    downloadAdxTrafficAccountBlob(meta.filenameBase + '.csv', 'text/csv;charset=utf-8', '\ufeff' + lines.join('\n'));
}

function initializeDataTable(data) {
    window.__adxTrafficAccountRows = (data && Array.isArray(data)) ? data : [];
    var tableData = [];
    if (window.__adxTrafficAccountRows.length) {
        window.__adxTrafficAccountRows.forEach(function (row, idx) {
            // Format tanggal ke format Indonesia
            var formattedDate = row.date || '-';
            if (row.date && row.date.match(/\d{4}-\d{2}-\d{2}/)) {
                var months = [
                    'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                    'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'
                ];
                var date = new Date(row.date + 'T00:00:00');
                var day = date.getDate();
                var month = months[date.getMonth()];
                var year = date.getFullYear();
                formattedDate = day + ' ' + month + ' ' + year;
            }
            var cellDate = '<span data-order="' + (row.date || '') + '">' + formattedDate + '</span>';
            var siteLabel = escapeHtml(row.site_name || '-');
            var cellSite = '<span class="site-badge" title="' + siteLabel + '">' + siteLabel + '</span>';
            var btnDetail = '<button type="button" class="btn btn-sm btn-outline-primary btn-adx-traffic-account-detail btn-detail-row" data-row-index="' + idx + '" title="Detail">'
                + '<i class="bi bi-eye-fill" aria-hidden="true"></i>'
                + '</button>';

            tableData.push([
                cellDate,
                cellSite,
                formatNumber(row.clicks_adx || 0),
                formatCurrencyIDR(row.cpc_adx || 0),
                formatCurrencyIDR(row.ecpm || 0),
                formatNumber(row.ctr || 0, 2) + ' %',
                formatCurrencyIDR(row.revenue || 0),
                btnDetail
            ]);
        });
    }
    // Destroy existing DataTable if it exists
    if ($.fn.DataTable.isDataTable('#table_traffic_account')) {
        $('#table_traffic_account').DataTable().destroy();
    }
    // Initialize DataTable
    var table = $('#table_traffic_account').DataTable({
        data: tableData,
        responsive: false,
        scrollX: true,
        scrollXInner: '100%',
        scrollCollapse: true,
        autoWidth: false,
        paging: true,
        pageLength: 25,
        lengthChange: true,
        lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "Semua"]],
        searching: true,
        ordering: true,
        order: [[0, 'asc']],
        language: {
            "decimal": ",",
            "thousands": ".",
            "info": "Menampilkan _START_ sampai _END_ dari _TOTAL_ entri",
            "infoEmpty": "Menampilkan 0 sampai 0 dari 0 entri",
            "infoFiltered": "(disaring dari _MAX_ total entri)",
            "lengthMenu": "Tampilkan _MENU_ entri",
            "loadingRecords": "Memuat...",
            "processing": "Memproses...",
            "search": "Cari:",
            "zeroRecords": "Tidak ada data yang cocok",
            "paginate": {
                "first": "Pertama",
                "last": "Terakhir",
                "next": "Selanjutnya",
                "previous": "Sebelumnya"
            }
        },
        dom: 'Blfrtip',
        buttons: [
            {
                text: 'Export Excel',
                className: 'btn btn-success',
                action: function () { exportAdxTrafficAccountExcel(); }
            },
            {
                text: 'Export PDF',
                className: 'btn btn-danger',
                action: function () { exportAdxTrafficAccountPdf(); }
            },
            {
                extend: 'copy',
                text: 'Copy',
                className: 'btn btn-info',
                exportOptions: {
                    columns: ':visible:not(.no-export)',
                    format: {
                        body: function (data) { return stripHtmlExport(data); }
                    }
                },
                customize: function (txt) {
                    var meta = getExportMetaTrafficAccount();
                    var header = meta.titleText + '\n' + meta.periodText;
                    if (meta.accountText) header += '\n' + meta.accountText;
                    if (meta.domainText) header += '\n' + meta.domainText;
                    header += '\n\n';
                    return header + txt;
                }
            },
            {
                text: 'Export CSV',
                className: 'btn btn-primary',
                action: function () { exportAdxTrafficAccountCsv(); }
            },
            {
                extend: 'print',
                text: 'Print',
                className: 'btn btn-warning',
                exportOptions: {
                    columns: ':visible:not(.no-export)',
                    format: {
                        body: function (data) { return stripHtmlExport(data); }
                    }
                },
                title: function () { return getExportMetaTrafficAccount().titleText; },
                messageTop: function () {
                    var meta = getExportMetaTrafficAccount();
                    var html = '<div style="text-align:center;margin-bottom:8px;font-size:12px;">' + escapeHtml(meta.periodText) + '</div>';
                    if (meta.accountText) {
                        html += '<div style="text-align:center;margin-bottom:4px;font-size:11px;color:#475569;">' + escapeHtml(meta.accountText) + '</div>';
                    }
                    if (meta.domainText) {
                        html += '<div style="text-align:center;margin-bottom:8px;font-size:11px;color:#475569;">' + escapeHtml(meta.domainText) + '</div>';
                    }
                    return html;
                }
            },
            {
                extend: 'colvis',
                text: 'Column Visibility',
                className: 'btn btn-default',
                columns: ':not(.no-export)'
            }
        ],
        columnDefs: [
            { targets: [0, 5, 7], className: 'text-center' },
            { targets: 7, orderable: false, searchable: false, className: 'text-center no-export' },
            { targets: [2, 3, 4, 6], className: 'text-right' },
            {
                targets: 2,
                type: 'num',
                render: function (data, type) {
                    if (type === 'sort' || type === 'type') {
                        var v = parseFloat(String(data).replace(/[^0-9.-]/g, ''));
                        return isNaN(v) ? 0 : v;
                    }
                    return data;
                }
            },
            {
                targets: 3,
                type: 'num',
                render: function (data, type) {
                    if (type === 'sort' || type === 'type') {
                        var v = parseFloat(String(data).replace(/[Rp.\s]/g, '').replace(/,/g, ''));
                        return isNaN(v) ? 0 : v;
                    }
                    return data;
                }
            },
            {
                targets: 4,
                type: 'num',
                render: function (data, type) {
                    if (type === 'sort' || type === 'type') {
                        var v = parseFloat(String(data).replace(/[Rp.\s]/g, '').replace(/,/g, ''));
                        return isNaN(v) ? 0 : v;
                    }
                    return data;
                }
            },
            {
                targets: 5,
                type: 'num',
                render: function (data, type) {
                    if (type === 'sort' || type === 'type') {
                        var v = parseFloat(String(data).replace('%', '').trim());
                        return isNaN(v) ? 0 : v;
                    }
                    return data;
                }
            },
            {
                targets: 6,
                type: 'num',
                render: function (data, type) {
                    if (type === 'sort' || type === 'type') {
                        var v = parseFloat(String(data).replace(/[Rp.\s]/g, '').replace(/,/g, ''));
                        return isNaN(v) ? 0 : v;
                    }
                    return data;
                }
            }
        ]
    });
    // Paksa urutan setelah inisialisasi untuk memastikan tidak tertimpa
    table.order([0, 'desc']).draw();
    window.adxTrafficAccountDt = table;

    $('#table_traffic_account tbody')
        .off('click', '.btn-adx-traffic-account-detail')
        .on('click', '.btn-adx-traffic-account-detail', function () {
            var idx = parseInt($(this).attr('data-row-index') || '0', 10);
            var row = (window.__adxTrafficAccountRows || [])[idx] || {};

            $('#adxTrafficAccountDetailDate').text(formatDateID(row.date || ''));
            $('#adxTrafficAccountDetailSite').text(escapeHtml(row.site_name || '-'));
            $('#adxTrafficAccountDetailSiteRaw').text(escapeHtml(row.site_name_raw || '-'));

            $('#adxTrafficAccountDetailImpressions').text(formatNumber(row.impressions_adx || 0));
            $('#adxTrafficAccountDetailClicks').text(formatNumber(row.clicks_adx || 0));
            $('#adxTrafficAccountDetailCtr').text(formatNumber(row.ctr || 0, 2) + ' %');
            $('#adxTrafficAccountDetailCpc').text(formatCurrencyIDR(row.cpc_adx || 0));
            $('#adxTrafficAccountDetailEcpm').text(formatCurrencyIDR(row.ecpm || 0));
            $('#adxTrafficAccountDetailRevenue').text(formatCurrencyIDR(row.revenue || 0));

            $('#adxTrafficAccountDetailTotalRequests').text(formatNumber(row.total_requests || 0));
            $('#adxTrafficAccountDetailResponsesServed').text(formatNumber(row.responses_served || 0));
            $('#adxTrafficAccountDetailMatchRate').text(formatNumber(row.match_rate || 0, 2) + ' %');
            $('#adxTrafficAccountDetailFillRate').text(formatNumber(row.fill_rate || 0, 2) + ' %');
            $('#adxTrafficAccountDetailActiveViewPctViewable').text(formatNumber(row.active_view_pct_viewable || 0, 2) + ' %');
            $('#adxTrafficAccountDetailActiveViewAvgTimeSec').text(formatNumber(row.active_view_avg_time_sec || 0, 2));

            $('#adxTrafficAccountDetailModal').modal('show');
        });

    try {
        table.columns.adjust();
    } catch (e) {}
}

var adxTrafficRevenueChart = null;

function create_revenue_line_chart(data) {
    if (!data || data.length === 0 || typeof Highcharts === 'undefined') return;

    var theme = getAdxChartTheme();
    var dailyRevenue = {};
    data.forEach(function (item) {
        var date = String(item.date || '').slice(0, 10);
        if (!date) return;
        dailyRevenue[date] = (dailyRevenue[date] || 0) + parseFloat(item.revenue || 0);
    });

    var dates = Object.keys(dailyRevenue).sort();
    var revenues = dates.map(function (date) { return dailyRevenue[date]; });
    var formattedDates = dates.map(function (date) {
        var d = new Date(date + 'T00:00:00');
        return d.toLocaleDateString('id-ID', { day: 'numeric', month: 'short' });
    });

    if (adxTrafficRevenueChart && typeof adxTrafficRevenueChart.destroy === 'function') {
        adxTrafficRevenueChart.destroy();
    }

    adxTrafficRevenueChart = Highcharts.chart('revenue_chart', {
        chart: {
            type: 'areaspline',
            backgroundColor: 'transparent',
            style: { fontFamily: 'inherit' },
            spacing: [12, 8, 16, 8]
        },
        title: { text: null },
        credits: { enabled: false },
        xAxis: {
            categories: formattedDates,
            lineColor: theme.grid,
            tickColor: theme.grid,
            labels: { style: { color: theme.muted, fontSize: '11px' } }
        },
        yAxis: {
            title: { text: null },
            gridLineColor: theme.grid,
            labels: {
                style: { color: theme.muted, fontSize: '11px' },
                formatter: function () { return 'Rp ' + formatNumber(this.value, 0); }
            }
        },
        legend: { enabled: false },
        tooltip: {
            backgroundColor: theme.tooltipBg,
            borderColor: theme.tooltipBorder,
            borderRadius: 10,
            style: { color: theme.text },
            formatter: function () {
                return '<b>' + formatDateID(dates[this.point.index]) + '</b><br/>Pendapatan: <b>Rp ' + formatNumber(this.y, 0) + '</b>';
            }
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.18,
                lineWidth: 3,
                marker: { enabled: true, radius: 4, lineWidth: 2, lineColor: '#ffffff' }
            }
        },
        series: [{
            name: 'Pendapatan',
            data: revenues,
            color: '#6366f1',
            fillColor: {
                linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
                stops: [[0, 'rgba(99, 102, 241, 0.35)'], [1, 'rgba(99, 102, 241, 0.02)']]
            }
        }]
    });

    if ($('#btnToggleAccountChart').is(':visible')) {
        setAccountChartVisible(typeof window.__adxAccountChartVisible === 'boolean'
            ? window.__adxAccountChartVisible
            : isAccountChartVisible(), false);
    }
}

// Function to format date for display
function formatDateForDisplay(dateString) {
    if (!dateString) return '';
    
    try {
        var date = new Date(dateString + 'T00:00:00');
        var day = String(date.getDate()).padStart(2, '0');
        var month = String(date.getMonth() + 1).padStart(2, '0');
        var year = date.getFullYear();
        return day + '/' + month + '/' + year;
    } catch (e) {
        return dateString;
    }
}

function formatNumber(num, decimals = 0) {
    if (num === null || num === undefined) return '0';
    return parseFloat(num).toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}
// Fungsi untuk format mata uang IDR
function formatCurrencyIDR(value) {
    // Convert to number, round to remove decimals, then format with Rp
    let numValue = parseFloat(value.toString().replace(/[$,]/g, ''));
    if (isNaN(numValue)) return value;
    
    // Round to remove decimals and format with Indonesian number format
    return 'Rp. ' + Math.round(numValue).toLocaleString('id-ID');
}
function showSuccessMessage(message) {
    var alertHtml = '<div class="alert alert-success alert-dismissible fade show adx-traffic-alert" role="alert">'
        + '<i class="bi bi-check-circle"></i> ' + message
        + '<button type="button" class="close" data-dismiss="alert"><span>&times;</span></button></div>';
    $('.adx-traffic-page .card').first().find('.card-body').prepend(alertHtml);
    setTimeout(function () { $('.adx-traffic-alert').fadeOut('slow', function () { $(this).remove(); }); }, 3000);
}
function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}
const csrftoken = getCookie('csrftoken');