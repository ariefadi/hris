/**
 * AdX Summary — dashboard charts & data loader
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
        dark: dark,
        text: dark ? '#e2e8f0' : '#334155',
        muted: dark ? '#94a3b8' : '#64748b',
        grid: dark ? 'rgba(148, 163, 184, 0.12)' : 'rgba(15, 23, 42, 0.08)',
        bg: dark ? '#1e293b' : '#ffffff',
        tooltipBg: dark ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.98)',
        tooltipBorder: dark ? 'rgba(255,255,255,0.1)' : 'rgba(15, 23, 42, 0.1)',
        palette: ['#6366f1', '#8b5cf6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444', '#ec4899', '#14b8a6', '#f97316', '#3b82f6']
    };
}

function showAdxSummaryLoader(message) {
    var msg = String(message || 'Memuat data AdX Summary...').trim();
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

function hideAdxSummaryLoader() {
    if (window.HrisLoader && typeof window.HrisLoader.forceHide === 'function') {
        window.HrisLoader.forceHide();
        return;
    }
    $('#overlay').hide();
}

function showAdxSummaryResults() {
    $('#adxSummaryEmptyState').hide();
    $('#adxSummaryResults').show();
}

function hideAdxSummaryResults() {
    $('#adxSummaryResults').hide();
    $('#adxSummaryEmptyState').show();
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

    $('#account_filter').select2({
        placeholder: '-- Pilih Account Terdaftar --',
        allowClear: true,
        width: '100%',
        theme: 'bootstrap4'
    });

    $('#domain_filter').select2({
        placeholder: 'Ketik subdomain…',
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

    hideAdxSummaryResults();

    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domain = normalizeDomainFilter($("#domain_filter").val());
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            showAdxSummaryLoader();
            load_adx_summary_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
            load_adx_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });

    function load_adx_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain) {
        var accountFilter = '';
        if (selected_account && selected_account.length > 0) {
            accountFilter = selected_account.join(',');
        }
        var domainFilter = normalizeDomainFilter(selected_domain);
        $.ajax({
            url: '/management/admin/page_adx_traffic_country',
            type: 'GET',
            data: {
                start_date: tanggal_dari,
                end_date: tanggal_sampai,
                selected_account: accountFilter,
                selected_domains: domainFilter
            },
            headers: { 'X-CSRFToken': csrftoken },
            success: function (response) {
                if (response && response.status) {
                    if (Array.isArray(response.domain_suggestions) && response.domain_suggestions.length) {
                        applyDomainSuggestions(response.domain_suggestions);
                    }
                    if (response.data && response.data.length > 0) {
                        generateTrafficCountryCharts(response.data);
                        $('#charts_section').show();
                    } else {
                        $('#charts_section').hide();
                    }
                } else {
                    var errorMsg = response.error || 'Terjadi kesalahan yang tidak diketahui';
                    console.error('[AdX Summary] Country error:', errorMsg);
                }
            },
            error: function (xhr, status, error) {
                console.error('[AdX Summary] Country AJAX error:', error);
            }
        });
    }
});

function applyDomainSuggestions(domains) {
    var $domain = $('#domain_filter');
    if (!$domain.length || !Array.isArray(domains)) return;
    var currentVals = $domain.val() || [];
    var selectedMap = {};
    currentVals.forEach(function (v) { selectedMap[String(v)] = true; });
    var uniq = {};
    domains.forEach(function (d) {
        var v = String(d || '').trim();
        if (!v) return;
        var k = v.toLowerCase();
        if (uniq[k]) return;
        uniq[k] = true;
        if ($domain.find('option[value="' + v.replace(/"/g, '\\"') + '"]').length === 0) {
            $domain.append(new Option(v, v, false, !!selectedMap[v]));
        }
    });
    $domain.trigger('change.select2');
}

function load_adx_summary_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain) {
    var accountFilter = '';
    if (selected_account && selected_account.length > 0) {
        accountFilter = selected_account.join(',');
    }
    var domainFilter = normalizeDomainFilter(selected_domain);
    $.ajax({
        url: '/management/admin/page_adx_summary',
        type: 'GET',
        data: {
            start_date: tanggal_dari,
            end_date: tanggal_sampai,
            selected_account: accountFilter,
            selected_domain: domainFilter
        },
        headers: { 'X-CSRFToken': csrftoken },
        success: function (response) {
            hideAdxSummaryLoader();
            if (response && response.status) {
                showAdxSummaryResults();

                var summary = response.summary || {};
                $("#total_impressions").text(formatNumber(summary.total_impressions || 0));
                $("#total_clicks").text(formatNumber(summary.total_clicks || 0));
                $("#total_revenue").text('Rp ' + formatNumber(summary.total_revenue || 0, 0));
                $("#avg_cpc").text('Rp ' + formatNumber(summary.avg_cpc || 0, 2));
                $("#avg_ctr").text(formatNumber(summary.avg_ctr || 0, 2) + '%');

                if (response.today_traffic) {
                    $("#today_traffic").show();
                    $("#today_impressions").text(formatNumber(response.today_traffic.impressions || 0));
                    $("#today_clicks").text(formatNumber(response.today_traffic.clicks || 0));
                    $("#today_revenue").text('Rp ' + formatNumber(response.today_traffic.revenue || 0, 0));
                    $("#today_ctr").text(formatNumber(response.today_traffic.ctr || 0, 2) + '%');
                } else {
                    $("#today_traffic").hide();
                }

                if (response.data && response.data.length > 0) {
                    $("#revenue_chart_row").show();
                    if (typeof Highcharts !== 'undefined') {
                        create_revenue_line_chart(response.data);
                    } else {
                        setTimeout(function () {
                            if (typeof Highcharts !== 'undefined') create_revenue_line_chart(response.data);
                        }, 1000);
                    }
                } else {
                    $("#revenue_chart_row").hide();
                }
            } else {
                hideAdxSummaryResults();
                alert('Error: ' + (response && response.error ? response.error : 'Unknown error occurred'));
            }
        },
        error: function (jqXHR, textStatus) {
            hideAdxSummaryLoader();
            hideAdxSummaryResults();
            report_eror(jqXHR, textStatus);
        }
    });
}

var adxRevenueChart = null;

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

    if (adxRevenueChart && typeof adxRevenueChart.destroy === 'function') {
        adxRevenueChart.destroy();
    }

    adxRevenueChart = Highcharts.chart('revenue_chart', {
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
                var dateIndex = this.point.index;
                return '<b>' + formatDate(dates[dateIndex]) + '</b><br/>' +
                    'Pendapatan: <b>Rp ' + formatNumber(this.y, 0) + '</b>';
            }
        },
        plotOptions: {
            areaspline: {
                fillOpacity: 0.18,
                lineWidth: 3,
                marker: {
                    enabled: true,
                    radius: 4,
                    symbol: 'circle',
                    lineWidth: 2,
                    lineColor: '#ffffff'
                },
                states: { hover: { lineWidth: 3 } }
            }
        },
        series: [{
            name: 'Pendapatan',
            data: revenues,
            color: '#6366f1',
            fillColor: {
                linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
                stops: [
                    [0, 'rgba(99, 102, 241, 0.35)'],
                    [1, 'rgba(99, 102, 241, 0.02)']
                ]
            }
        }]
    });
}

var trafficCharts = { impressions: null, revenue: null };

function generateTrafficCountryCharts(data) {
    if (!data || data.length === 0 || typeof Chart === 'undefined') return;

    var theme = getAdxChartTheme();
    var sortedData = data.slice().sort(function (a, b) {
        return (b.impressions || 0) - (a.impressions || 0);
    }).slice(0, 10);

    var countries = sortedData.map(function (item) {
        var name = item.country_name || 'Unknown';
        return name.length > 18 ? name.slice(0, 16) + '…' : name;
    });
    var impressions = sortedData.map(function (item) { return item.impressions || 0; });
    var revenue = sortedData.map(function (item) { return item.revenue || 0; });

    var chartDefaults = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                labels: { color: theme.text, font: { size: 11, weight: '600' } }
            },
            tooltip: {
                backgroundColor: theme.tooltipBg,
                titleColor: theme.text,
                bodyColor: theme.muted,
                borderColor: theme.tooltipBorder,
                borderWidth: 1,
                padding: 12,
                cornerRadius: 8
            }
        },
        scales: {
            x: {
                ticks: { color: theme.muted, maxRotation: 45, minRotation: 0, font: { size: 10 } },
                grid: { color: theme.grid, drawBorder: false }
            },
            y: {
                ticks: { color: theme.muted, font: { size: 10 } },
                grid: { color: theme.grid, drawBorder: false }
            }
        }
    };

    var ctx1 = document.getElementById('impressionsChart');
    if (ctx1) {
        if (trafficCharts.impressions) trafficCharts.impressions.destroy();
        trafficCharts.impressions = new Chart(ctx1, {
            type: 'bar',
            data: {
                labels: countries,
                datasets: [{
                    label: 'Impresi',
                    data: impressions,
                    backgroundColor: 'rgba(99, 102, 241, 0.75)',
                    borderColor: '#6366f1',
                    borderWidth: 0,
                    borderRadius: 8,
                    borderSkipped: false,
                    maxBarThickness: 42
                }]
            },
            options: Object.assign({}, chartDefaults, {
                plugins: Object.assign({}, chartDefaults.plugins, {
                    legend: { display: false }
                })
            })
        });
    }

    var ctx2 = document.getElementById('revenueChart');
    if (ctx2) {
        if (trafficCharts.revenue) trafficCharts.revenue.destroy();
        var doughnutColors = theme.palette.slice(0, countries.length);
        trafficCharts.revenue = new Chart(ctx2, {
            type: 'doughnut',
            data: {
                labels: countries,
                datasets: [{
                    label: 'Pendapatan',
                    data: revenue,
                    backgroundColor: doughnutColors.map(function (c) { return c + 'cc'; }),
                    borderColor: theme.bg,
                    borderWidth: 3,
                    hoverOffset: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '62%',
                plugins: {
                    legend: {
                        position: 'right',
                        labels: {
                            color: theme.text,
                            font: { size: 10 },
                            boxWidth: 12,
                            padding: 10
                        }
                    },
                    tooltip: {
                        backgroundColor: theme.tooltipBg,
                        titleColor: theme.text,
                        bodyColor: theme.muted,
                        borderColor: theme.tooltipBorder,
                        borderWidth: 1,
                        callbacks: {
                            label: function (ctx) {
                                var val = ctx.raw || 0;
                                return ' Rp ' + formatNumber(val, 0);
                            }
                        }
                    }
                }
            }
        });
    }
}

function formatDate(dateString) {
    if (!dateString) return 'N/A';
    var months = [
        'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
        'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'
    ];
    var date = new Date(dateString + 'T00:00:00');
    return date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear();
}

function formatNumber(num, decimals) {
    decimals = (decimals === undefined) ? 0 : decimals;
    if (num === null || num === undefined || isNaN(num)) return '0';
    return parseFloat(num).toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

var csrftoken = getCookie('csrftoken');
