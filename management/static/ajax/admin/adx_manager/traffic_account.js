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
                        let isSelected = currentSelected && currentSelected.includes(accIdStr);
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
    // Initialize DataTable
    $('#table_traffic_account').DataTable({
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
                    var start = $('#tanggal_dari').val();
                    var end = $('#tanggal_sampai').val();
                    var months = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
                    function fmt(d) { if (!d) return '-'; try { var date = new Date(d + 'T00:00:00'); return date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear(); } catch(e) { return d; } }
                    var titleText = 'Traffic AdX Per Account';
                    var periodText = 'Periode ' + fmt(start) + ' s/d ' + fmt(end);
                    var sheet = xlsx.xl.worksheets['sheet1.xml'];
                    var numrows = 2;
                    $('row', sheet).each(function () { var r = parseInt($(this).attr('r')); $(this).attr('r', r + numrows); });
                    $('row c', sheet).each(function () { var attr = $(this).attr('r'); var col = attr.replace(/[0-9]/g, ''); var row = parseInt(attr.replace(/[A-Z]/g, '')); $(this).attr('r', col + (row + numrows)); });
                    var row1 = '<row r="1"><c t="inlineStr" r="A1" s="51"><is><t>' + titleText + '</t></is></c></row>';
                    var row2 = '<row r="2"><c t="inlineStr" r="A2" s="51"><is><t>' + periodText + '</t></is></c></row>';
                    $('sheetData', sheet).prepend(row2);
                    $('sheetData', sheet).prepend(row1);
                    var merges = $('mergeCells', sheet);
                    if (merges.length === 0) {
                        $('worksheet', sheet).append('<mergeCells count="2"><mergeCell ref="A1:G1"/><mergeCell ref="A2:G2"/></mergeCells>');
                    } else {
                        var c = parseInt(merges.attr('count') || '0');
                        merges.attr('count', c + 2);
                        merges.append('<mergeCell ref="A1:G1"/>');
                        merges.append('<mergeCell ref="A2:G2"/>');
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
                    var start = $('#tanggal_dari').val();
                    var end = $('#tanggal_sampai').val();
                    var months = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
                    function fmt(d) { if (!d) return '-'; try { var date = new Date(d + 'T00:00:00'); return date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear(); } catch(e) { return d; } }
                    var header = 'Periode ' + fmt(start) + ' s/d ' + fmt(end);
                    doc.content.splice(1, 0, { text: header, style: 'header', alignment: 'center', margin: [0, 0, 0, 12] });
                }
            },
            {
                extend: 'copy',
                text: 'Copy',
                className: 'btn btn-info',
                exportOptions: { columns: ':visible' }
            },
            {
                extend: 'csv',
                text: 'Export CSV',
                className: 'btn btn-primary',
                exportOptions: { columns: ':visible' },
                customize: function (csv) {
                    var start = $('#tanggal_dari').val();
                    var end = $('#tanggal_sampai').val();
                    var months = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
                    function fmt(d) { if (!d) return '-'; try { var date = new Date(d + 'T00:00:00'); return date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear(); } catch(e) { return d; } }
                    var header = 'Periode ' + fmt(start) + ' s/d ' + fmt(end);
                    var titleText = 'Traffic AdX Per Account';
                    return titleText + '\n' + header + '\n\n' + csv;
                }
            },
            {
                extend: 'print',
                text: 'Print',
                className: 'btn btn-warning',
                exportOptions: { columns: ':visible' },
                title: function () { return '<h3 style="text-align:center;margin:0">Traffic AdX Per Account</h3>'; },
                messageTop: function () {
                    var start = $('#tanggal_dari').val();
                    var end = $('#tanggal_sampai').val();
                    var months = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
                    function fmt(d) { if (!d) return '-'; try { var date = new Date(d + 'T00:00:00'); return date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear(); } catch(e) { return d; } }
                    return '<div style="text-align:center;margin-bottom:8px">Periode ' + fmt(start) + ' s/d ' + fmt(end) + '</div>';
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
                if (response.summary) {
                    $("#total_clicks").text(formatNumber(response.summary.total_clicks || 0));
                    $("#avg_cpc").text(formatCurrencyIDR(response.summary.avg_cpc || 0));
                    $("#avg_ecpm").text(formatCurrencyIDR(response.summary.avg_ecpm || 0));
                    $("#avg_ctr").text(formatNumber(response.summary.avg_ctr || 0, 2) + '%');
                    $("#total_revenue").text(formatCurrencyIDR(response.summary.total_revenue || 0));
                    // Show summary boxes
                    $('#summary_boxes').show();
                }
                // Update DataTable
                var table = $('#table_traffic_account').DataTable();
                table.clear();
                if (response.data && response.data.length > 0) {
                    response.data.forEach(function(item) {
                        // Format tanggal ke format Indonesia
                        var formattedDate = item.date || '-';
                        if (item.date && item.date.match(/\d{4}-\d{2}-\d{2}/)) {
                            var months = [
                                'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                                'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'
                            ];
                            var date = new Date(item.date + 'T00:00:00');
                            var day = date.getDate();
                            var month = months[date.getMonth()];
                            var year = date.getFullYear();
                            formattedDate = day + ' ' + month + ' ' + year;
                        }
                        var cellDate = '<span data-order="' + (item.date || '-') + '">' + formattedDate + '</span>';
                        table.row.add([
                            cellDate,
                            item.site_name || '-',
                            formatNumber(item.clicks_adx || 0),
                            formatCurrencyIDR(item.cpc_adx || 0),
                            formatCurrencyIDR(item.ecpm || 0),
                            formatNumber(item.ctr || 0, 2) + ' %',
                            formatCurrencyIDR(item.revenue || 0)
                        ]);
                    });
                    // Create daily revenue line chart
                    create_revenue_line_chart(response.data);
                }
                table.draw();
                showSuccessMessage('Traffic data loaded successfully!');
                $("#overlay").hide();
            } else {
                alert('Error: ' + (response && response.error ? response.error : 'Unknown error occurred'));
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            report_eror(jqXHR, textStatus);
            $("#overlay").hide();
        }
    });
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