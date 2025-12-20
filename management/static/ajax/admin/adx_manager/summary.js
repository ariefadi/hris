/**
 * Reference Ajax AdX Summary
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
    // Configure datepicker with YYYY-MM-DD format
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
    // Initialize Select2 for account
    $('#account_filter').select2({
        placeholder: '-- Pilih Account Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    let allAccountOptions = $('#account_filter').html();  
    // Initialize Select2 for domain
    $('#domain_filter').select2({
        placeholder: '-- Pilih Domain Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    let allDomainOptions = $('#domain_filter').html();  
    // Set default dates (last 7 days)
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    $('#tanggal_dari').val(lastWeek.toISOString().split('T')[0]);
    $('#tanggal_sampai').val(today.toISOString().split('T')[0]);
    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domain = $("#domain_filter").val();
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            $("#overlay").show();
            load_adx_summary_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
            load_adx_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
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
    // Fungsi untuk load data traffic per country
    function load_adx_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain) {
        // Convert array to comma-separated string for backend
        var accountFilter = '';
        if (selected_account && selected_account.length > 0) {
            accountFilter = selected_account.join(',');
        }
        var domainFilter = '';
        if (selected_domain && selected_domain.length > 0) {
            domainFilter = selected_domain.join(',');
        }
        // AJAX request
        $.ajax({
            url: '/management/admin/page_adx_traffic_country',
            type: 'GET',
            data: {
                start_date: tanggal_dari,
                end_date: tanggal_sampai,
                selected_account: accountFilter,
                selected_domains: domainFilter
            },
            headers: {
                'X-CSRFToken': csrftoken
            },
            success: function (response) {
                if (response && response.status) {
                    // Generate charts if data available
                    if (response.data && response.data.length > 0) {
                        generateTrafficCountryCharts(response.data);
                        $('#charts_section').show();
                    } else {
                        $('#charts_section').hide();
                    }
                    // Show success message
                    if (response.data && response.data.length > 0) {
                        console.log('Data berhasil dimuat: ' + response.data.length + ' negara');
                    } else {
                        console.log('Tidak ada data untuk periode yang dipilih');
                    }
                    // Hide overlay after country data is loaded   
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
                report_eror('Terjadi kesalahan saat memuat data: ' + error);
            }
        });
    }
});

function load_adx_summary_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain) {
    // Convert array to comma-separated string for backend
    var accountFilter = '';
    if (selected_account && selected_account.length > 0) {
        accountFilter = selected_account.join(',');
    }
    var domainFilter = '';
    if (selected_domain && selected_domain.length > 0) {
        domainFilter = selected_domain.join(',');
    }
    $.ajax({
        url: '/management/admin/page_adx_summary',
        type: 'GET',
        data: {
            'start_date': tanggal_dari,
            'end_date': tanggal_sampai,
            'selected_account': accountFilter,
            'selected_domain': domainFilter
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            if (response && response.status) {
                // Show summary boxes
                $("#summary_boxes").show();
                // Show revenue chart row
                $("#revenue_chart_row").show();
                // Update summary boxes
                $("#total_clicks").text(formatNumber(response.summary.total_clicks));
                $("#total_revenue").text('Rp ' + formatNumber(response.summary.total_revenue, 0));
                $("#avg_cpc").text('Rp ' + formatNumber(response.summary.avg_cpc, 2));
                $("#avg_ctr").text(formatNumber(response.summary.avg_ctr, 2) + '%');
                // Show and update today traffic data
                if (response.today_traffic) {
                    $("#today_traffic").show();
                    $("#today_impressions").text(formatNumber(response.today_traffic.impressions));
                    $("#today_clicks").text(formatNumber(response.today_traffic.clicks));
                    $("#today_revenue").text('Rp ' + formatNumber(response.today_traffic.revenue, 0));
                    $("#today_ctr").text(formatNumber(response.today_traffic.ctr, 2) + '%');
                }
                // Create revenue line chart
                if (response.data && response.data.length > 0) {
                    // Check if Highcharts is loaded before creating chart
                    if (typeof Highcharts !== 'undefined') {
                        create_revenue_line_chart(response.data);
                    } else {
                        console.error('Highcharts is not loaded yet. Retrying in 1 second...');
                        setTimeout(function () {
                            if (typeof Highcharts !== 'undefined') {
                                create_revenue_line_chart(response.data);
                            } else {
                                console.error('Highcharts failed to load after retry.');
                            }

                        }, 1000);
                    }
                }
                // Hide overlay after all data is loaded
                $("#overlay").hide();
            } else {
                alert('Error: ' + (response && response.error ? response.error : 'Unknown error occurred'));
                $("#overlay").hide();
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            report_eror(jqXHR, textStatus);
            $("#overlay").hide();
        }
    });
}

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
                var formattedDate = formatDate(actualDate);
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

let trafficCharts = {
    impressions: null,
    revenue: null
};

function generateTrafficCountryCharts(data) {
    if (!data || data.length === 0) return;
    // Sort data by impressions and take top 10
    var sortedData = data.sort(function (a, b) {
        return (b.impressions || 0) - (a.impressions || 0);
    }).slice(0, 10);
    // Prepare data for charts
    var countries = sortedData.map(item => item.country_name || 'Unknown');
    var impressions = sortedData.map(item => item.impressions || 0);
    var clicks = sortedData.map(item => item.clicks || 0);
    var revenue = sortedData.map(item => item.revenue || 0);
    if (typeof Chart !== 'undefined') {

        // ---------------------------
        // IMPRESSIONS CHART (BAR)
        // ---------------------------
        var ctx1 = document.getElementById('impressionsChart');

        if (ctx1) {
            // destroy old chart if exists
            if (trafficCharts.impressions !== null) {
                trafficCharts.impressions.destroy();
            }

            trafficCharts.impressions = new Chart(ctx1, {
                type: 'bar',
                data: {
                    labels: countries,
                    datasets: [{
                        label: 'Total Impresi',
                        data: impressions,
                        backgroundColor: 'rgba(54, 162, 235, 0.6)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });
        }
        // ---------------------------
        // REVENUE CHART (DOUGHNUT)
        // ---------------------------
        var ctx2 = document.getElementById('revenueChart');
        if (ctx2) {
            // destroy old chart if exists
            if (trafficCharts.revenue !== null) {
                trafficCharts.revenue.destroy();
            }
            trafficCharts.revenue = new Chart(ctx2, {
                type: 'doughnut',
                data: {
                    labels: countries,
                    datasets: [{
                        label: 'Total Pendapatan',
                        data: revenue,
                        backgroundColor: [
                            '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
                            '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#FF6384'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    aspectRatio: 2
                }
            });
        }
    }
}

function formatDate(dateString) {
    if (!dateString) return 'N/A';
    var months = [
        'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
        'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'
    ];
    var date = new Date(dateString + 'T00:00:00');
    var day = date.getDate();
    var month = months[date.getMonth()];
    var year = date.getFullYear();
    return day + ' ' + month + ' ' + year;
}

function formatNumber(num, decimals = 0) {
    if (num === null || num === undefined || isNaN(num)) {
        return '0';
    }
    return parseFloat(num).toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
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