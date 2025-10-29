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
    $('#tanggal_dari').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true,
        orientation: 'bottom auto'
    });
    $('#tanggal_sampai').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true,
        orientation: 'bottom auto'
    });
    // Initialize Select2 for account
    $('#account_filter').select2({
        placeholder: '-- Pilih Akun Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    // Initialize Select2 for site filter
    $('#site_filter').select2({
        placeholder: '-- Pilih Domain --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    // Set default dates (last 7 days)
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    $('#tanggal_dari').val(lastWeek.toISOString().split('T')[0]);
    $('#tanggal_sampai').val(today.toISOString().split('T')[0]);
    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_accounts = $("#account_filter").val();
        var selected_sites = $("#site_filter").val();
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            loadSitesList(selected_accounts);
            load_adx_summary_data(tanggal_dari, tanggal_sampai, selected_accounts, selected_sites);
            load_adx_traffic_country_data();
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    function loadSitesList(selected_accounts) {
        $("#overlay").show();
        var selectedAccounts = selected_accounts;
        $.ajax({
            url: '/management/admin/adx_sites_list',
            type: 'GET',
            dataType: 'json',
            data: {
                'selected_accounts': selectedAccounts
            },
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function(response) {
                $("#overlay").hide();
                if (response.status) {
                    var select_site = $("#site_filter");
                    select_site.empty();
                    $.each(response.data, function(index, site) {
                        select_site.append(new Option(site, site, false, false));
                    });
                    // Jangan trigger change di sini untuk menghindari loop
                    select_site.trigger('change');
                } 
            },
            error: function(xhr, status, error) {
                console.error('Error loading sites:', error);
                console.error('Status:', status);
                console.error('Response:', xhr.responseText);
                $("#overlay").hide();
            }
        });
    }

    // Fungsi untuk load data traffic per country
    function load_adx_traffic_country_data() {
        var startDate = $('#tanggal_dari').val();
        var endDate = $('#tanggal_sampai').val();
        var selectedAccounts = $("#account_filter").val();
        var selectedSites = $("#site_filter").val();
        // Convert array to comma-separated string for backend
        var siteFilter = '';
        if (selectedSites && selectedSites.length > 0) {
            siteFilter = selectedSites.join(',');
        }
        $("#overlay").show();
        // AJAX request
        $.ajax({
            url: '/management/admin/page_adx_traffic_country',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                selected_accounts: selectedAccounts,
                selected_sites: siteFilter
            },
            headers: {
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function(response) {
                $("#overlay").hide();
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
                    $("#overlay").hide();
                } else {
                    var errorMsg = response.error || 'Terjadi kesalahan yang tidak diketahui';
                    console.error('[DEBUG] Response error:', errorMsg);
                    alert('Error: ' + errorMsg);
                    $("#overlay").hide();
                }
            },
            error: function(xhr, status, error) {
                console.error('[DEBUG] AJAX Error:', {
                    xhr: xhr,
                    status: status,
                    error: error
                });
                report_eror('Terjadi kesalahan saat memuat data: ' + error);
                $("#overlay").hide();
            }
        });
    }
});

function load_adx_summary_data(tanggal_dari, tanggal_sampai, selectedAccounts, selectedSites) {
    $("#overlay").show();
    $.ajax({
        url: '/management/admin/page_adx_summary',
        type: 'GET',
        data: {
            'start_date': tanggal_dari,
            'end_date': tanggal_sampai,
            'selected_accounts': selectedAccounts,
            'selected_sites': selectedSites
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            $("#overlay").hide();
            if (response && response.status) {
                // Show summary boxes
                $("#summary_boxes").show();
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
// Fungsi untuk generate charts
function generateTrafficCountryCharts(data) {
    if (!data || data.length === 0) return;
    // Sort data by impressions and take top 10
    var sortedData = data.sort(function (a, b) {
        return (b.impressions || 0) - (a.impressions || 0);
    }).slice(0, 10);
    // Prepare data for charts
    var countries = sortedData.map(function (item) {
        return item.country_name || 'Unknown';
    });
    var impressions = sortedData.map(function (item) {
        return item.impressions || 0;
    });
    var clicks = sortedData.map(function (item) {
        return item.clicks || 0;
    });
    var revenue = sortedData.map(function (item) {
        return item.revenue || 0;
    });
    // Create charts if Chart.js is available
    if (typeof Chart !== 'undefined') {
        // Impressions Chart
        var ctx1 = document.getElementById('impressionsChart');
        if (ctx1) {
            new Chart(ctx1, {
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
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        }
        // Revenue Chart
        var ctx2 = document.getElementById('revenueChart');
        if (ctx2) {
            new Chart(ctx2, {
                type: 'doughnut',
                data: {
                    labels: countries,
                    datasets: [{
                        label: 'Total Pendapatan',
                        data: revenue,
                        backgroundColor: [
                            '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
                            '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#FF6384',
                            '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40'
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