/**
 * Reference Ajax ROI Summary
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
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    // Inisialisasi Select2 untuk country filter
    $('#country_filter').select2({
        placeholder: '-- Pilih Negara --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4',
        multiple: true
    });
    // Set default dates (last 7 days)
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    $('#tanggal_dari').val(lastWeek.toISOString().split('T')[0]);
    $('#tanggal_sampai').val(today.toISOString().split('T')[0]);
    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account_adx = $("#account_filter").val() || "";
        if (tanggal_dari != "" && tanggal_sampai != "") {
            // Reset status fetch sebelum mulai menarik data
            window.fetchStatus = { summary: false, country: false };
            $('#overlay').show();
            load_ROI_traffic_country_data();
            load_ROI_summary_data(tanggal_dari, tanggal_sampai, selected_account_adx);
            loadSitesList();
            load_country_options();
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    // Load data situs untuk select2
    function loadSitesList() {
        var selectedAccounts = $("#account_filter").val() || "";
        // Simpan pilihan domain yang sudah dipilih sebelumnya
        var previouslySelected = $("#site_filter").val() || [];
        
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
            success: function (response) {
                if (response.status) {
                    var select_site = $("#site_filter");
                    select_site.empty();
                    
                    // Tambahkan opsi baru dan pertahankan pilihan sebelumnya jika masih tersedia
                    var validPreviousSelections = [];
                    $.each(response.data, function (index, site) {
                        var isSelected = previouslySelected.includes(site);
                        if (isSelected) {
                            validPreviousSelections.push(site);
                        }
                        select_site.append(new Option(site, site, false, isSelected));
                    });
                    
                    // Set nilai yang dipilih kembali
                    if (validPreviousSelections.length > 0) {
                        select_site.val(validPreviousSelections);
                    }
                    
                    select_site.trigger('change');
                }
            },
            error: function (xhr, status, error) {
                report_eror(xhr, status);
            }
        });
    }
    // Load data saat halaman pertama kali dibuka
    function load_country_options() {
        var selectedAccounts = $("#account_filter").val() || "";
        // Simpan pilihan country yang sudah dipilih sebelumnya
        var previouslySelected = $("#country_filter").val() || [];
        
        $.ajax({
            url: '/management/admin/get_countries_adx',
            type: 'GET',
            dataType: 'json',
            data: {
                'selected_accounts': selectedAccounts
            },
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function (response) {
                if (response.status) {
                    var select_country = $('#country_filter');
                    select_country.empty();
                    
                    // Tambahkan opsi baru dan pertahankan pilihan sebelumnya jika masih tersedia
                    var validPreviousSelections = [];
                    $.each(response.countries, function (index, country) {
                        var isSelected = previouslySelected.includes(country.code);
                        if (isSelected) {
                            validPreviousSelections.push(country.code);
                        }
                        select_country.append(new Option(country.name, country.code, false, isSelected));
                    });
                    
                    // Set nilai yang dipilih kembali
                    if (validPreviousSelections.length > 0) {
                        select_country.val(validPreviousSelections);
                    }
                    
                    select_country.trigger('change');
                }
            },
            error: function (xhr, status, error) {
                report_eror(xhr, status);
            }
        });
    }
    // Fungsi untuk load data traffic per country
    function load_ROI_traffic_country_data() {
        var startDate = $('#tanggal_dari').val();
        var endDate = $('#tanggal_sampai').val();
        var selectedAccountAdx = $('#account_filter').val();
        var selectedSites = $('#site_filter').val();
        var selectedAccountAds = $('#select_account').val();
        var selectedCountries = $('#country_filter').val();
        var selectedCountriesStr = Array.isArray(selectedCountries) ? selectedCountries.join(',') : (selectedCountries || '');
        var selectedSitesStr = Array.isArray(selectedSites) ? selectedSites.join(',') : (selectedSites || '');
        // AJAX request
        $.ajax({
            url: '/management/admin/page_roi_traffic_country',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                selected_account_adx: selectedAccountAdx,
                selected_sites: selectedSitesStr,
                selected_account: selectedAccountAds,
                selected_countries: selectedCountriesStr
            },
            headers: {
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function (response) {
                // Tandai selesai tarik data country
                window.fetchStatus = window.fetchStatus || { summary: false, country: false };
                window.fetchStatus.country = true;
                if (response && response.status) {
                    // Update summary boxes
                    updateSummaryBoxes(response.data);
                    // Generate charts if data available
                    generateTrafficCountryCharts(response.data);
                    $('#charts_section').show();
                } else {
                    var errorMsg = (response && response.error) || 'Terjadi kesalahan yang tidak diketahui';
                    alert('Error: ' + errorMsg);
                }
            },
            error: function (xhr, status, error) {
                // Tandai selesai tarik data country meskipun error
                window.fetchStatus = window.fetchStatus || { summary: false, country: false };
                window.fetchStatus.country = true;
                report_eror('Terjadi kesalahan saat memuat data: ' + error);
            }
        });
    }
});

// Fungsi untuk update summary boxes
function updateSummaryBoxes(data) {
    if (!data || !Array.isArray(data)) return;
    // Hitung summary dari data
    var totalImpressions = 0;
    var totalSpend = 0;
    var totalClicks = 0;
    var totalRevenue = 0;
    var totalROI = 0;
    var validROICount = 0;
    data.forEach(function (item) {
        totalImpressions += item.impressions || 0;
        totalSpend += item.spend || 0;
        totalClicks += item.clicks || 0;
        totalRevenue += item.revenue || 0;

        if (item.roi && item.roi !== 0) {
            totalROI += item.roi;
            validROICount++;
        }
    });
    var averageCTR = totalImpressions > 0 ? (totalClicks / totalImpressions * 100) : 0;
    var averageROI = validROICount > 0 ? (totalROI / validROICount) : 0;
    $('#total_impressions').text(totalImpressions.toLocaleString('id-ID'));
    $('#total_spend').text(formatCurrencyIDR(totalSpend));
    // Jika belum memilih domain, set Total Klik & Total Pendapatan ke 0
    var selectedSites = $('#site_filter').val();
    var noSiteSelected = !selectedSites || selectedSites.length === 0;
    if (noSiteSelected) {
        $('#total_clicks').text(formatNumber(0));
        $('#total_revenue').text(formatCurrencyIDR(0));
    } else {
        $('#total_clicks').text(totalClicks.toLocaleString('id-ID'));
        $('#total_revenue').text(formatCurrencyIDR(totalRevenue));
    }
    // Isi ROI Nett sesuai elemen template
    $('#roi_nett').text(averageROI.toFixed(2) + '%');
    // Elemen total_ctr mungkin tidak ada di template; abaikan jika tidak ada
    $('#total_ctr').text(averageCTR.toFixed(2) + '%');
    // Tampilkan summary boxes
    $('#summary_boxes').show();
}

// Fungsi untuk format mata uang IDR
function formatCurrencyIDR(value) {
    // Convert to number, round to remove decimals, then format with Rp
    let numValue = parseFloat(value.toString().replace(/[$,]/g, ''));
    if (isNaN(numValue)) return value;
    // Round to remove decimals and format with Indonesian number format
    return 'Rp. ' + Math.round(numValue).toLocaleString('id-ID');
}

function create_roi_daily_chart(data) {
    // Check if Chart.js is available
    if (typeof Chart === 'undefined') {
        console.error('Chart.js is not loaded!');
        return;
    }
    // Check if canvas element exists
    var canvas = document.getElementById('chart_roi_daily');
    if (!canvas) {
        console.error('Canvas element chart_roi_daily not found!');
        return;
    }
    // Destroy existing chart if it exists
    if (window.dailyRoiChart && typeof window.dailyRoiChart.destroy === 'function') {
        window.dailyRoiChart.destroy();
    }

    // Group data by date and calculate ROI
    var dailyData = {};
    data.forEach(function (item) {
        var date = item.date;
        var revenue = parseFloat(item.revenue || 0);
        var spend = parseFloat(item.spend || 0);
        var other_costs = parseFloat(item.other_costs || 0);
        var total_costs = spend + other_costs;

        // Calculate ROI: ((revenue - total_costs) / total_costs) * 100
        var roi = 0;
        if (total_costs > 0) {
            roi = ((revenue - total_costs) / total_costs) * 100;
        }

        if (!date) {
            console.warn('No date found in item:', item);
            return;
        }

        if (!dailyData[date]) {
            dailyData[date] = {
                roi_values: [],
                revenue: 0,
                spend: 0,
                other_costs: 0
            };
        }

        dailyData[date].roi_values.push(roi);
        dailyData[date].revenue += revenue;
        dailyData[date].spend += spend;
        dailyData[date].other_costs += other_costs;
    });

    // Convert to arrays and calculate average ROI per date
    var dates = Object.keys(dailyData).sort();
    var roiData = [];
    var revenueData = [];
    var spendData = [];

    dates.forEach(function (date) {
        var dayData = dailyData[date];
        // Calculate average ROI for the day
        var avgROI = 0;
        if (dayData.roi_values.length > 0) {
            avgROI = dayData.roi_values.reduce(function (sum, roi) { return sum + roi; }, 0) / dayData.roi_values.length;
        }

        roiData.push(avgROI.toFixed(2));
        revenueData.push(dayData.revenue);
        spendData.push(dayData.spend + dayData.other_costs);
    });
    // Format dates for display
    var formattedDates = dates.map(function (date) {
        var d = new Date(date + 'T00:00:00');
        return d.toLocaleDateString('id-ID', {
            day: 'numeric',
            month: 'short'
        });
    });
    // Destroy existing chart if it exists
    var existingChart = Chart.getChart('chart_roi_daily');
    if (existingChart) {
        existingChart.destroy();
    }

    // Create Chart.js line chart
    var ctx = document.getElementById('chart_roi_daily').getContext('2d');
    window.dailyRoiChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: formattedDates,
            datasets: [{
                label: 'ROI (%)',
                data: roiData,
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.2)',
                borderWidth: 3,
                fill: true,
                tension: 0.4,
                pointBackgroundColor: 'rgb(75, 192, 192)',
                pointBorderColor: '#fff',
                pointBorderWidth: 2,
                pointRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Tanggal',
                        font: {
                            weight: 'bold'
                        }
                    },
                    grid: {
                        display: true,
                        color: 'rgba(0, 0, 0, 0.1)'
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'ROI (%)',
                        font: {
                            weight: 'bold'
                        }
                    },
                    ticks: {
                        callback: function (value) {
                            return value.toFixed(1) + '%';
                        }
                    },
                    grid: {
                        display: true,
                        color: 'rgba(0, 0, 0, 0.1)'
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Tren ROI Harian',
                    font: {
                        size: 16,
                        weight: 'bold'
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function (context) {
                            var dataIndex = context.dataIndex;
                            var roi = context.parsed.y;
                            var revenue = revenueData[dataIndex];
                            var spend = spendData[dataIndex];

                            return [
                                'ROI: ' + roi + '%',
                                'Revenue: ' + formatCurrencyIDR(revenue),
                                'Spend: ' + formatCurrencyIDR(spend)
                            ];
                        }
                    }
                },
                legend: {
                    display: true,
                    position: 'top'
                }
            }
        }
    });
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

// Utility: normalize date to 'YYYY-MM-DD'
function normalizeDateStr(val) {
    if (val === null || val === undefined) { return ''; }
    try {
        if (typeof val === 'string') {
            var s = val.trim();
            // Remove time part if present
            var tIndex = s.indexOf('T');
            if (tIndex > 0) {
                s = s.substring(0, tIndex);
            }
            // Match YYYY-MM-DD or YYYY/MM/DD
            var m = s.match(/^(\d{4})[-\/]?(\d{2})[-\/]?(\d{2})$/);
            if (m) {
                return m[1] + '-' + m[2] + '-' + m[3];
            }
            // Try parseable date string
            var d1 = new Date(s);
            if (!isNaN(d1.getTime())) {
                return d1.toISOString().slice(0, 10);
            }
            return s; // fallback to original string
        } else if (val instanceof Date) {
            return val.toISOString().slice(0, 10);
        } else {
            var d2 = new Date(val);
            if (!isNaN(d2.getTime())) {
                return d2.toISOString().slice(0, 10);
            }
            return String(val);
        }
    } catch (e) {
        return typeof val === 'string' ? val : String(val);
    }
}
// Fungsi untuk generate charts
function generateTrafficCountryCharts(data) {
    // Bersihkan chart jika data kosong dan sembunyikan section chart
    if (!data || data.length === 0) {
        if (window.roiChartInstance) {
            try { window.roiChartInstance.destroy(); } catch (e) { console.warn('Failed to destroy ROI chart:', e); }
            window.roiChartInstance = null;
        }
        // Tampilkan pesan kosong pada card chart negara
        var msgId = 'roiChartEmptyMsg';
        var $canvas = $('#roiChart');
        if (!$('#' + msgId).length && $canvas.length) {
            $canvas.parent().append('<div id="' + msgId + '" class="text-muted">Tidak ada data ROI per negara untuk periode/filters ini.</div>');
        }
        $canvas.hide();
        return;
    }
    // Hapus pesan kosong jika sebelumnya ada
    $('#roiChartEmptyMsg').remove();
    $('#roiChart').show();

    // Sort data by ROI and take top 10
    var sortedData = data.sort(function (a, b) {
        return (b.roi || 0) - (a.roi || 0);
    }).slice(0, 10);

    // Prepare data for charts
    var countries = sortedData.map(function (item) {
        return item.country || 'Unknown';
    });

    var roi = sortedData.map(function (item) {
        return item.roi || 0;
    });

    // Create charts if Chart.js is available
    if (typeof Chart !== 'undefined') {
        // ROI Chart
        var canvasEl = document.getElementById('roiChart');
        if (canvasEl) {
            var ctx = canvasEl.getContext('2d');
            // Hancurkan chart sebelumnya jika ada untuk mencegah error canvas in use
            if (window.roiChartInstance) {
                try { window.roiChartInstance.destroy(); } catch (e) { console.warn('Failed to destroy ROI chart:', e); }
                window.roiChartInstance = null;
            }
            window.roiChartInstance = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: countries,
                    datasets: [{
                        label: 'ROI (%)',
                        data: roi,
                        backgroundColor: [
                            'rgba(255, 99, 132, 0.8)',
                            'rgba(54, 162, 235, 0.8)',
                            'rgba(255, 205, 86, 0.8)',
                            'rgba(75, 192, 192, 0.8)',
                            'rgba(153, 102, 255, 0.8)',
                            'rgba(255, 159, 64, 0.8)',
                            'rgba(199, 199, 199, 0.8)',
                            'rgba(83, 102, 255, 0.8)',
                            'rgba(255, 99, 255, 0.8)',
                            'rgba(99, 255, 132, 0.8)'
                        ],
                        borderColor: [
                            'rgba(255, 99, 132, 1)',
                            'rgba(54, 162, 235, 1)',
                            'rgba(255, 205, 86, 1)',
                            'rgba(75, 192, 192, 1)',
                            'rgba(153, 102, 255, 1)',
                            'rgba(255, 159, 64, 1)',
                            'rgba(199, 199, 199, 1)',
                            'rgba(83, 102, 255, 1)',
                            'rgba(255, 99, 255, 1)',
                            'rgba(99, 255, 132, 1)'
                        ],
                        borderWidth: 1
                    }]
                },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    scales: {
                        x: {
                            beginAtZero: true,
                            ticks: {
                                callback: function (value) {
                                    return value + '%';
                                }
                            }
                        }
                    },
                    plugins: {
                        tooltip: {
                            callbacks: {
                                label: function (context) {
                                    return context.dataset.label + ': ' + context.parsed.x + '%';
                                }
                            }
                        }
                    }
                }
            });
        }
    }
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

// Memuat data ROI Summary (ringkasan & grafik ROI harian)
function load_ROI_summary_data(startDate, endDate, selectedAccountAdx) {
    var selectedSites = $('#site_filter').val();
    var selectedAccount = $('#select_account').val();
    var siteFilter = '';
    if (selectedSites && selectedSites.length > 0) {
        siteFilter = selectedSites.join(',');
    }
    $.ajax({
        url: '/management/admin/page_roi_traffic_domain',
        type: 'GET',
        data: {
            start_date: startDate,
            end_date: endDate,
            selected_account_adx: selectedAccountAdx,
            selected_sites: siteFilter,
            selected_account: selectedAccount
        },
        headers: {
            'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
        },
        success: function (response) {
            // Tandai selesai tarik data summary
            window.fetchStatus = window.fetchStatus || { summary: false, country: false };
            window.fetchStatus.summary = true;
            if (response && response.status) {
                // Perbarui summary jika tersedia
                if (response.summary) {
                    if (siteFilter) {
                        $('#total_clicks').text(formatNumber(response.summary.total_clicks || 0));
                        $('#total_spend').text(formatCurrencyIDR(response.summary.total_spend || 0));
                        $('#roi_nett').text(formatNumber(response.summary.roi_nett || 0, 2) + '%');
                        $('#total_revenue').text(formatCurrencyIDR(response.summary.total_revenue || 0));
                    } else {
                        // Jika belum memilih domain, set Total Klik & Total Pendapatan ke 0
                        $('#total_clicks').text(formatNumber(0));
                        $('#total_revenue').text(formatCurrencyIDR(0));
                    }
                    $('#summary_boxes').show();
                }
                // Tampilkan chart ROI harian jika ada data
                if (response.data && response.data.length > 0) {
                    $('#charts_section').show();
                    create_roi_daily_chart(response.data);
                    // Tentukan tanggal yang akan digunakan untuk "Hari Ini": gunakan endDate yang dipilih, fallback ke hari terakhir di dataset
                    var targetDayStr = endDate || new Date().toISOString().split('T')[0];
                    targetDayStr = normalizeDateStr(targetDayStr);
                    var normalizedData = response.data.map(function (item) {
                        return {
                            date: normalizeDateStr(item.date),
                            spend: parseFloat(item.spend || 0),
                            clicks: parseInt(item.clicks || 0),
                            revenue: parseFloat(item.revenue || 0),
                            other_costs: parseFloat(item.other_costs || 0)
                        };
                    });
                    var todayItems = normalizedData.filter(function (item) { return item.date === targetDayStr; });
                    // Jika tidak ada data untuk targetDayStr, gunakan tanggal maksimum yang tersedia pada data
                    if (todayItems.length === 0) {
                        var maxDate = normalizedData.reduce(function (max, curr) { return (!max || curr.date > max) ? curr.date : max; }, null);
                        todayItems = normalizedData.filter(function (item) { return item.date === maxDate; });
                    }
                    // Hitung dan tampilkan data traffic untuk tanggal terpilih
                    if (todayItems.length > 0) {
                        var todaySpend = 0, todayClicks = 0, todayRevenue = 0, todayOtherCosts = 0;
                        todayItems.forEach(function (item) {
                            todaySpend += item.spend;
                            todayClicks += item.clicks;
                            todayRevenue += item.revenue;
                            todayOtherCosts += item.other_costs;
                        });
                        var todayTotalCosts = todaySpend + todayOtherCosts;
                        var todayRoi = todayTotalCosts > 0 ? ((todayRevenue - todayTotalCosts) / todayTotalCosts) * 100 : 0;
                        // Jika filter domain belum dipilih, nol-kan Klik Hari Ini & Pendapatan Hari Ini
                        var siteSelected = !!siteFilter;
                        var displayClicks = siteSelected ? todayClicks : 0;
                        var displayRevenue = siteSelected ? todayRevenue : 0;
                        $('#today_spend').text(formatCurrencyIDR(todaySpend));
                        $('#today_clicks').text(formatNumber(displayClicks));
                        $('#today_roi').text(todayRoi.toFixed(2) + '%');
                        $('#today_revenue').text(formatCurrencyIDR(displayRevenue));
                        $('#today_traffic').show();
                        $('#overlay').hide();
                    } else {
                        // Sembunyikan chart & tampilkan panel today dengan nilai 0 jika tidak ada data
                        try {
                            var existingChart = Chart.getChart('chart_roi_daily');
                            if (existingChart) existingChart.destroy();
                        } catch (e) { console.warn('Failed to destroy chart_roi_daily:', e); }
                        $('#charts_section').hide();
                        $('#today_spend').text(formatCurrencyIDR(0));
                        $('#today_clicks').text(formatNumber(0));
                        $('#today_roi').text('0.00%');
                        $('#today_revenue').text(formatCurrencyIDR(0));
                        $('#today_traffic').show();
                        $('#overlay').hide();
                    }
                }
            } else {
                alert('Error: ' + (response && response.error ? response.error : 'Unknown error occurred'));
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            report_eror(jqXHR, textStatus);
        }
    });
}