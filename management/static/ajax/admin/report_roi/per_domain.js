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
    $('#btn_load_data').click(function (e) {
        var selected_account_adx = $("#account_filter").val();
        if (!selected_account_adx) {
            alert('Mohon pilih akun terdaftar.');
            return;
        }
        $('#overlay').show();
        loadSitesList(selected_account_adx);
        load_adx_traffic_account_data();
    });
    // Load sites list on page load
    function loadSitesList(selected_account_adx) {
        var selectedAccounts = selected_account_adx;
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
                    $.each(response.data, function (index, site) {
                        select_site.append(new Option(site, site, false, false));
                    });
                    // Jangan trigger change di sini untuk menghindari loop
                    select_site.trigger('change');
                }
            },
            error: function (xhr, status, error) {
                console.error('Error loading sites:', error);
                console.error('Status:', status);
                console.error('Response:', xhr.responseText);
            }
        });
    }
    // Initialize DataTable
    $('#table_traffic_account').DataTable({
        "paging": true,
        "pageLength": 25,
        "lengthChange": true,
        "searching": true,
        "ordering": true,
        "columnDefs": [
            {
                "targets": [2, 3, 4, 5, 6, 7, 8], // Numeric columns
                "className": "text-right"
            }
        ]
    });
});

function load_adx_traffic_account_data() {
    var tanggal_dari = $('#tanggal_dari').val();
    var tanggal_sampai = $('#tanggal_sampai').val();
    var selectedAccounts = $('#account_filter').val();
    var selectedSites = $('#site_filter').val();
    var selectedAccount = $('#select_account').val();
    if (!tanggal_dari || !tanggal_sampai) {
        alert('Please select both start and end dates.');
        return;
    }
    // Convert array to comma-separated string for backend
    var siteFilter = '';
    if (selectedSites && selectedSites.length > 0) {
        siteFilter = selectedSites.join(',');
    }
    $.ajax({
        url: '/management/admin/page_roi_traffic_domain',
        type: 'GET',
        data: {
            start_date: tanggal_dari,
            end_date: tanggal_sampai,
            selected_account_adx: selectedAccounts,
            selected_sites: siteFilter,
            selected_account: selectedAccount,
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            if (response && response.status) {
                // Update summary boxes
                if (response.summary) {
                    $("#total_clicks").text(formatNumber(response.summary.total_clicks || 0));
                    $("#total_spend").text(formatCurrencyIDR(response.summary.total_spend || 0));
                    $("#roi_nett").text(formatNumber(response.summary.roi_nett || 0, 2) + '%');
                    $("#total_revenue").text(formatCurrencyIDR(response.summary.total_revenue || 0));
                    // Show summary boxes
                    $('#summary_boxes').show();
                }
                // Create ROI Daily Chart
                if (response.data && response.data.length > 0) {
                    // Pastikan section chart terlihat sebelum inisialisasi chart
                    $('#charts_section').show();
                    createROIDailyChart(response.data);
                    // Resize chart setelah dibuat untuk memastikan tampil dengan benar
                    if (roiChart && typeof roiChart.resize === 'function') {
                        roiChart.resize();
                    }
                } else {
                    // Jika tidak ada data, hancurkan chart sebelumnya dan sembunyikan bagian chart
                    if (roiChart) {
                        roiChart.destroy();
                        roiChart = null;
                    }
                    $('#charts_section').hide();
                }
                // Update DataTable
                var table = $('#table_traffic_account').DataTable();
                table.clear();
                if (response.data && response.data.length > 0) {
                    response.data.forEach(function (item) {
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

                        table.row.add([
                            item.site_name || '-',
                            formattedDate,
                            formatCurrencyIDR(item.spend || 0),
                            formatNumber(item.clicks || 0),
                            formatNumber(item.ctr || 0, 2) + ' %',
                            formatCurrencyIDR(item.cpc || 0),
                            formatCurrencyIDR(item.ecpm || 0),
                            formatNumber(item.roi || 0, 2) + ' %',
                            formatCurrencyIDR(item.revenue || 0)
                        ]);
                    });
                }
                table.draw();
                showSuccessMessage('Traffic data loaded successfully!');
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
    setTimeout(function () {
        $('.alert-success').fadeOut('slow', function () {
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
// Global variable to store chart instance
let roiChart = null;
// Function to create ROI Daily Chart
function createROIDailyChart(data) {
    // Check if Chart.js is loaded
    if (typeof Chart === 'undefined') {
        console.error('Chart.js is not loaded!');
        return;
    }
    // Destroy existing chart if it exists
    if (roiChart) {
        roiChart.destroy();
    }
    // Group data by date and calculate average ROI per date
    const dailyROI = {};
    const dailyRevenue = {};
    const dailySpend = {};
    data.forEach(item => {
        const date = item.date;
        const roi = parseFloat(item.roi || 0);
        const revenue = parseFloat(item.revenue || 0);
        const spend = parseFloat(item.spend || 0);
        if (!dailyROI[date]) {
            dailyROI[date] = [];
            dailyRevenue[date] = 0;
            dailySpend[date] = 0;
        }
        dailyROI[date].push(roi);
        dailyRevenue[date] += revenue;
        dailySpend[date] += spend;
    });
    // Calculate average ROI per date and sort by date
    const sortedDates = Object.keys(dailyROI).sort();
    const avgROIData = [];
    const revenueData = [];
    const spendData = [];
    const labels = [];
    sortedDates.forEach(date => {
        const roiValues = dailyROI[date];
        const avgROI = roiValues.reduce((sum, roi) => sum + roi, 0) / roiValues.length;
        // Format date for display
        const dateObj = new Date(date + 'T00:00:00');
        const formattedDate = dateObj.toLocaleDateString('id-ID', {
            day: '2-digit',
            month: 'short'
        });
        labels.push(formattedDate);
        // Pastikan nilai ROI berupa number, bukan string
        avgROIData.push(parseFloat(avgROI.toFixed(2)));
        revenueData.push(dailyRevenue[date]);
        spendData.push(dailySpend[date]);
    });
    // Buat chart baru
    const canvasElement = document.getElementById('chart_roi_daily');
    if (!canvasElement) {
        console.error('Canvas element with id "chart_roi_daily" not found!');
        return;
    }
    if (canvasElement.tagName !== 'CANVAS') {
        console.error('Element with id "chart_roi_daily" is not a canvas element! It is:', canvasElement.tagName);
        return;
    }
    // Pastikan ukuran canvas memadai
    canvasElement.style.height = '300px';
    canvasElement.style.width = '100%';
    // Hapus atribut width/height bawaan agar tidak bentrok dengan style
    canvasElement.removeAttribute('width');
    canvasElement.removeAttribute('height');
    const ctx = canvasElement.getContext('2d');
    if (!ctx) {
        console.error('Failed to get 2D context from canvas element!');
        return;
    }
    // Create the chart
    roiChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'ROI Harian (%)',
                data: avgROIData,
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.1)',
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
            plugins: {
                title: {
                    display: true,
                    text: 'Tren ROI Harian',
                    font: {
                        size: 16,
                        weight: 'bold'
                    }
                },
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function (context) {
                            const dataIndex = context.dataIndex;
                            const roi = context.parsed.y;
                            const revenue = revenueData[dataIndex];
                            const spend = spendData[dataIndex];

                            return [
                                `ROI: ${Number(roi).toFixed(2)}%`,
                                `Revenue: ${formatCurrencyIDR(revenue)}`,
                                `Spend: ${formatCurrencyIDR(spend)}`
                            ];
                        }
                    }
                }
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
                    grid: {
                        display: true,
                        color: 'rgba(0, 0, 0, 0.1)'
                    },
                    ticks: {
                        callback: function (value) {
                            return value + '%';
                        }
                    }
                }
            },
            interaction: {
                mode: 'nearest',
                axis: 'x',
                intersect: false
            }
        }
    });
}