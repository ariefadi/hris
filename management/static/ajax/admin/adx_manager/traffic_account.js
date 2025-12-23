/**
 * Reference Ajax AdX Traffic Per Account
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
    // Initialize date pickers
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    $('#tanggal_dari').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    }).datepicker('setDate', lastWeek);
    $('#tanggal_sampai').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    }).datepicker('setDate', today);
    // Initialize Select2 for account filter
    $('#account_filter').select2({
        placeholder: '-- Pilih Account Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    let allAccountOptions = $('#account_filter').html();  
    // Initialize Select2 for domain filter
    $('#domain_filter').select2({
        placeholder: '-- Pilih Domain Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    let allDomainOptions = $('#domain_filter').html();  
    // Load sites list on page load
    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domain = $("#domain_filter").val();
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            $("#overlay").show();
            load_adx_traffic_account_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    // Flag untuk mencegah infinite loop saat update filter
    var isUpdating = false;
    $('#account_filter').on('change', function () {
        if (isUpdating) return;
        let account = $(this).val();
        if (account && account.length > 0) {
            adx_site_list(); // filter domain by account
        } else {
            // restore semua domain dari template
            isUpdating = true;
            $('#domain_filter')
                .html(allDomainOptions)
                .val(null)
                .trigger('change.select2');
            isUpdating = false;
        }
    });
    function adx_site_list() {
        var selected_account = $("#account_filter").val();
        if (selected_account) {
            selected_account = selected_account.join(',');
        }
        return $.ajax({
            url: '/management/admin/adx_sites_list',
            type: 'GET',
            data: {
                selected_accounts: selected_account
            },
            headers: {
                'X-CSRFToken': csrftoken
            },
            success: function (response) {
                if (response && response.status) {
                    let $domain = $('#domain_filter');
                    let currentSelected = $domain.val(); // Simpan pilihan saat ini

                    isUpdating = true;
                    // 1. Kosongkan option lama
                    $domain.empty();

                    // 2. Tambahkan option baru
                    response.data.forEach(function (domain) {
                        let isSelected = currentSelected && currentSelected.includes(domain);
                        let option = new Option(domain, domain, isSelected, isSelected);
                        $domain.append(option);
                    });

                    // 3. Refresh select2
                    $domain.trigger('change.select2');
                    isUpdating = false;
                }
            },
            error: function (xhr, status, error) {
                report_eror(xhr, error);
            }
        });
    }
    $('#domain_filter').on('change', function () {
        if (isUpdating) return;
        let domain = $(this).val();
        if (domain && domain.length > 0) {
            adx_account_list(); // filter account by domain
        } else {
            // restore semua account dari template
            isUpdating = true;
            $('#account_filter')
                .html(allAccountOptions)
                .val(null)
                .trigger('change.select2');
            isUpdating = false;
        }
    });
    function adx_account_list() {
        var selected_domain = $("#domain_filter").val();
        if (selected_domain) {
            selected_domain = selected_domain.join(',');
        }
        return $.ajax({
            url: '/management/admin/adx_accounts_list',
            type: 'GET',
            data: {
                selected_domains: selected_domain
            },
            headers: {
                'X-CSRFToken': csrftoken
            },
            success: function (response) {
                if (response && response.status) {
                    let $account = $('#account_filter');
                    let currentSelected = $account.val(); // Simpan pilihan saat ini

                    isUpdating = true;
                    // 1. Kosongkan option lama
                    $account.empty();
                    // 2. Tambahkan option baru
                    response.data.forEach(function (account) {
                        let text = account.account_name || account.account_id;
                        // Konversi ke string untuk perbandingan yang aman
                        let accIdStr = String(account.account_id);
                        // let isSelected = currentSelected && currentSelected.includes(accIdStr);
                        // let option = new Option(text, accIdStr, isSelected, isSelected);
                        let isSelected = true;
                        let option = new Option(text, accIdStr, isSelected, isSelected);
                        $account.append(option);
                    });
                    // 3. Refresh select2
                    $account.trigger('change.select2');
                    isUpdating = false;
                }
            },
            error: function (xhr, status, error) {
                report_eror(xhr, error);
            }
        });
    }
});
function load_adx_traffic_account_data(tanggal_dari, tanggal_sampai, selected_account, selectedDomains) {
    // Convert array to comma-separated string for backend
    var accountFilter = '';
    if (selected_account && selected_account.length > 0) {
        accountFilter = selected_account.join(',');
    }
    var domainFilter = '';
    if (selectedDomains && selectedDomains.length > 0) {
        domainFilter = selectedDomains.join(',');
    }
    $("#overlay").show();
    // Destroy existing DataTable if exists
    if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
        $('#table_traffic_account').DataTable().destroy();
    }
    // AJAX Request
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
            if (response && response.status) {
                // Update summary boxes
                updateSummaryBoxes(response.summary);
                $('#summary_boxes').show();
                // Initialize DataTable
                initializeDataTable(response.data);
                // Generate charts if data available
                create_revenue_line_chart(response.data);
                $('#charts_section').show();
                $('#revenue_chart_row').show();
                $('#overlay').hide();
            } else {
                var errorMsg = response.error || 'Terjadi kesalahan yang tidak diketahui';
                console.error('[DEBUG] Response error:', errorMsg);
                alert('Error: ' + errorMsg);
            }
        },
        error: function (xhr, status, error) {
            console.error('[DEBUG] AJAX Error:', {
                xhr: xhr,
                status: status,
                error: error
            });
            $('#overlay').hide();
            report_eror('Terjadi kesalahan saat memuat data: ' + error);
        }
    });
}
// Fungsi untuk update summary boxes
function updateSummaryBoxes(data) {
    var totalClicks = Number(data.total_clicks || 0);
    var avgCpc = Number(data.avg_cpc || 0);
    var avgCtr = Number(data.avg_ctr || 0);
    var totalRevenue = parseFloat(data.total_revenue || 0) || 0;
    $("#total_clicks").text(formatNumber(totalClicks || 0));
    $("#avg_cpc").text(formatCurrencyIDR(avgCpc || 0));
    $("#avg_ctr").text(formatNumber(avgCtr || 0, 2) + '%');
    $("#total_revenue").text(formatCurrencyIDR(totalRevenue || 0));
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
    var periodText = 'Periode ' + formatDateID(start) + ' s/d ' + formatDateID(end);

    var accounts = getSelectedTextList('#account_filter');
    var domains = getSelectedTextList('#domain_filter');

    return {
        titleText: titleText,
        periodText: periodText,
        accountText: accounts.length ? ('Account: ' + accounts.join(', ')) : '',
        domainText: domains.length ? ('Domain: ' + domains.join(', ')) : ''
    };
}

function initializeDataTable(data) {
    var tableData = [];
    if (data && Array.isArray(data)) {
        data.forEach(function (row) {
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
            var cellDate = '<span data-order="' + (row.date || '-') + '">' + formattedDate + '</span>';
            tableData.push([
                cellDate,
                row.site_name || '-',
                formatNumber(row.clicks_adx || 0),
                formatCurrencyIDR(row.cpc_adx || 0),
                formatCurrencyIDR(row.ecpm || 0),
                formatNumber(row.ctr || 0, 2) + ' %',
                formatCurrencyIDR(row.revenue || 0)
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
        responsive: true,
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
                extend: 'excel',
                text: 'Export Excel',
                className: 'btn btn-success',
                exportOptions: { columns: ':visible' },
                title: function () { return 'Traffic AdX Per Account'; },
                customize: function (xlsx) {
                    var meta = getExportMetaTrafficAccount();
                    var headerRows = [meta.titleText, meta.periodText];
                    if (meta.accountText) headerRows.push(meta.accountText);
                    if (meta.domainText) headerRows.push(meta.domainText);

                    var sheet = xlsx.xl.worksheets['sheet1.xml'];
                    var numrows = headerRows.length;

                    $('row', sheet).each(function () {
                        var r = parseInt($(this).attr('r'));
                        $(this).attr('r', r + numrows);
                    });
                    $('row c', sheet).each(function () {
                        var attr = $(this).attr('r');
                        var col = attr.replace(/[0-9]/g, '');
                        var row = parseInt(attr.replace(/[A-Z]/g, ''));
                        $(this).attr('r', col + (row + numrows));
                    });

                    for (var i = headerRows.length; i >= 1; i--) {
                        var txt = escapeXmlText(headerRows[i - 1]);
                        var rowXml = '<row r="' + i + '"><c t="inlineStr" r="A' + i + '" s="51"><is><t>' + txt + '</t></is></c></row>';
                        $('sheetData', sheet).prepend(rowXml);
                    }

                    var merges = $('mergeCells', sheet);
                    var mergeXml = '';
                    for (var m = 1; m <= headerRows.length; m++) {
                        mergeXml += '<mergeCell ref="A' + m + ':G' + m + '"/>';
                    }
                    if (merges.length === 0) {
                        $('worksheet', sheet).append('<mergeCells count="' + headerRows.length + '">' + mergeXml + '</mergeCells>');
                    } else {
                        var c = parseInt(merges.attr('count') || '0');
                        merges.attr('count', c + headerRows.length);
                        merges.append(mergeXml);
                    }
                }
            },
            {
                extend: 'pdf',
                text: 'Export PDF',
                className: 'btn btn-danger',
                exportOptions: { columns: ':visible' },
                title: function () { return 'Traffic AdX Per Account'; },
                customize: function (doc) {
                    var meta = getExportMetaTrafficAccount();
                    var inserts = [];
                    inserts.push({ text: meta.periodText, style: 'header', alignment: 'center', margin: [0, 0, 0, 6] });
                    if (meta.accountText) inserts.push({ text: meta.accountText, alignment: 'center', margin: [0, 0, 0, 4] });
                    if (meta.domainText) inserts.push({ text: meta.domainText, alignment: 'center', margin: [0, 0, 0, 8] });
                    doc.content.splice(1, 0, ...inserts);
                }
            },
            {
                extend: 'copy',
                text: 'Copy',
                className: 'btn btn-info',
                exportOptions: { columns: ':visible' },
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
                extend: 'csv',
                text: 'Export CSV',
                className: 'btn btn-primary',
                exportOptions: { columns: ':visible' },
                customize: function (csv) {
                    var meta = getExportMetaTrafficAccount();
                    var out = meta.titleText + '\n' + meta.periodText;
                    if (meta.accountText) out += '\n' + meta.accountText;
                    if (meta.domainText) out += '\n' + meta.domainText;
                    return out + '\n\n' + csv;
                }
            },
            {
                extend: 'print',
                text: 'Print',
                className: 'btn btn-warning',
                exportOptions: { columns: ':visible' },
                title: function () { return '<h3 style="text-align:center;margin:0">Traffic AdX Per Account</h3>'; },
                messageTop: function () {
                    var meta = getExportMetaTrafficAccount();
                    var html = '<div style="text-align:center;margin-bottom:8px">' + escapeHtml(meta.periodText) + '</div>';
                    if (meta.accountText) html += '<div style="text-align:center;margin-bottom:4px">' + escapeHtml(meta.accountText) + '</div>';
                    if (meta.domainText) html += '<div style="text-align:center;margin-bottom:8px">' + escapeHtml(meta.domainText) + '</div>';
                    return html;
                }
            },
            {
                extend: 'colvis',
                text: 'Column Visibility',
                className: 'btn btn-default',
                exportOptions: { columns: ':visible' }
            }
        ],
        columnDefs: [
            {
                targets: [2, 3, 4, 5], // Kolom numerik ditata kanan
                className: "text-right"
            },
            {
                // Sort numerik untuk CPC (Rp) - kolom index 2
                targets: 2,
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
                // Sort numerik untuk eCPM (Rp) - kolom index 3
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
                // Sort numerik untuk CTR (%) - kolom index 4
                targets: 4,
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
                // Sort numerik untuk Pendapatan (Rp) - kolom index 5
                targets: 5,
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
}

// Function to create revenue line chart (matching adx_summary style)
function create_revenue_line_chart(data) {
    if (!data || data.length === 0) {
        console.log('No data available for chart');
        return;
    }
    
    // Check if Highcharts is available
    if (typeof Highcharts === 'undefined') {
        console.error('Highcharts is not defined. Cannot create chart.');
        return;
    }
    
    // Group data by date and sum revenue
    var dailyRevenue = {};
    data.forEach(function (item) {
        var date = item.date;
        if (!dailyRevenue[date]) {
            dailyRevenue[date] = 0;
        }
        dailyRevenue[date] += parseFloat(item.revenue || 0);
    });
    
    // Convert to arrays for Highcharts
    var dates = Object.keys(dailyRevenue).sort();
    var revenues = dates.map(function (date) {
        return dailyRevenue[date];
    });
    
    // Format dates for display
    var formattedDates = dates.map(function (date) {
        var d = new Date(date + 'T00:00:00');
        return d.toLocaleDateString('id-ID', {
            day: 'numeric',
            month: 'short'
        });
    });
    
    // Create line chart for daily revenue
    Highcharts.chart('revenue_chart', {
        chart: {
            type: 'line'
        },
        title: {
            text: 'Pergerakan Pendapatan Harian'
        },
        xAxis: {
            categories: formattedDates,
            title: {
                text: 'Tanggal'
            }
        },
        yAxis: {
            title: {
                text: 'Pendapatan (Rp)'
            },
            labels: {
                formatter: function () {
                    return 'Rp ' + formatNumber(this.value, 0);
                }
            }
        },
        series: [{
            name: 'Pendapatan Harian',
            data: revenues,
            color: '#28a745',
            lineWidth: 3,
            marker: {
                radius: 5
            }
        }],
        tooltip: {
            formatter: function () {
                var dateIndex = this.point.index;
                var actualDate = dates[dateIndex];
                var formattedDate = formatDateForDisplay(actualDate);
                return '<b>' + this.series.name + '</b><br/>' +
                    'Tanggal: ' + formattedDate + '<br/>' +
                    'Pendapatan: Rp ' + formatNumber(this.y, 2);
            }
        },
        legend: {
            enabled: false
        },
        plotOptions: {
            line: {
                dataLabels: {
                    enabled: false
                },
                enableMouseTracking: true
            }
        }
    });
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
    var alertHtml = '<div class="alert alert-success alert-dismissible fade show" role="alert">';
    alertHtml += '<i class="bi bi-check-circle"></i> ' + message;
    alertHtml += '<button type="button" class="close" data-dismiss="alert" aria-label="Close">';
    alertHtml += '<span aria-hidden="true">&times;</span>';
    alertHtml += '</button>';
    alertHtml += '</div>';
    
    $('.card-body').first().prepend(alertHtml);
    
    setTimeout(function() {
        $('.alert-success').fadeOut('slow', function() {
            $(this).remove();
        });
    }, 3000);
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