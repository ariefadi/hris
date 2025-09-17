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

    // Ensure date inputs use YYYY-MM-DD format and disable any global datepicker
    $('#tanggal_dari, #tanggal_sampai').datepicker('destroy');

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

    // Set default dates (last 7 days)
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    $('#tanggal_dari').val(lastWeek.toISOString().split('T')[0]);
    $('#tanggal_sampai').val(today.toISOString().split('T')[0]);

    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();

        if (tanggal_dari != "" && tanggal_sampai != "") {
            load_ROI_summary_data(tanggal_dari, tanggal_sampai);
            load_ROI_traffic_country_data();
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });

    // Auto load data on page load
    var tanggal_dari = $("#tanggal_dari").val();
    var tanggal_sampai = $("#tanggal_sampai").val();
    if (tanggal_dari != "" && tanggal_sampai != "") {
        load_ROI_summary_data(tanggal_dari, tanggal_sampai);
    }

    load_ROI_traffic_country_data();

    // Fungsi untuk load data traffic per country
    function load_ROI_traffic_country_data() {
        var startDate = $('#tanggal_dari').val();
        var endDate = $('#tanggal_sampai').val();
        var countryFilter = $('#country_filter').val();

        // AJAX request
        $.ajax({
            url: '/management/admin/page_roi_traffic_country',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                country_filter: countryFilter
            },
            headers: {
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function(response) {
                console.log('[DEBUG] AJAX Success Response:', response);
                $('#overlay').hide();
                
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
                } else {
                    var errorMsg = response.error || 'Terjadi kesalahan yang tidak diketahui';
                    console.error('[DEBUG] Response error:', errorMsg);
                    alert('Error: ' + errorMsg);
                }
            },
            error: function(xhr, status, error) {
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

});

function load_ROI_summary_data(tanggal_dari, tanggal_sampai) {
    $("#overlay").show();

    $.ajax({
        url: '/management/admin/page_roi_summary',
        type: 'GET',
        data: {
            'start_date': tanggal_dari,
            'end_date': tanggal_sampai
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
                $("#total_revenue").text('Rp ' + formatNumber(response.summary.total_revenue, 2));
                $("#avg_cpc").text('Rp ' + formatNumber(response.summary.avg_cpc, 2));
                $("#avg_ctr").text(formatNumber(response.summary.avg_ctr, 2) + '%');
                
                // Update new boxes with spend, costs, and ROI data
                if (response.summary.total_spend !== undefined) {
                    $("#total_spend").text(formatCurrencyIDR(response.summary.total_spend));
                }
                if (response.summary.total_other_costs !== undefined) {
                    $("#other_costs").text(formatCurrencyIDR(response.summary.total_other_costs));
                }
                if (response.summary.roi_nett !== undefined) {
                    $("#roi_nett").text(formatNumber(response.summary.roi_nett, 2) + '%');
                }
                
                // Show and update ROI Nett box (legacy support)
                if (response.summary.roi_nett !== undefined) {
                    $("#roi_nett_box").show();
                }

                // Show and update today traffic data
                if (response.today_traffic) {
                    $("#today_traffic").show();
                    $("#today_spend").text(formatCurrencyIDR(response.today_traffic.spend));
                    $("#today_clicks").text(formatNumber(response.today_traffic.clicks));
                    $("#today_revenue").text(formatCurrencyIDR(response.today_traffic.revenue));
                    // Calculate and display today's ROI if spend data is available
                    if (response.today_traffic.spend && response.today_traffic.spend > 0) {
                        var todayROI = ((response.today_traffic.revenue - response.today_traffic.spend) / response.today_traffic.spend) * 100;
                        $("#today_roi").text(formatNumber(todayROI, 2) + '%');
                    } else if (response.today_traffic.roi !== undefined) {
                        $("#today_roi").text(formatNumber(response.today_traffic.roi, 2) + '%');
                    } else {
                        $("#today_roi").text('0%');
                    }
                }

                // Create Traffic ROI Daily chart
                console.log('Checking data for chart:', response.data);
                if (response.data && response.data.length > 0) {
                    console.log('Creating ROI daily chart with', response.data.length, 'data points');
                    create_roi_daily_chart(response.data);
                    $("#charts_section").show();
                    console.log('Charts section shown');
                } else {
                    console.log('No data available for ROI chart');
                }

            } else {
                alert('Error: ' + (response && response.error ? response.error : 'Unknown error occurred'));
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#overlay").hide();
            report_eror(jqXHR, textStatus);
        }
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

function create_roi_daily_chart(data) {
    console.log('create_roi_daily_chart called with data:', data.length, 'items');
    
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
    
    console.log('Canvas element found:', canvas);

    // Destroy existing chart if it exists
    if (window.roiChart) {
        window.roiChart.destroy();
    }

    // Group data by date and calculate ROI
    var dailyData = {};
    
    console.log('Sample data item:', data[0]);

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
    
    dates.forEach(function(date) {
        var dayData = dailyData[date];
        // Calculate average ROI for the day
        var avgROI = 0;
        if (dayData.roi_values.length > 0) {
            avgROI = dayData.roi_values.reduce(function(sum, roi) { return sum + roi; }, 0) / dayData.roi_values.length;
        }
        
        roiData.push(avgROI.toFixed(2));
        revenueData.push(dayData.revenue);
        spendData.push(dayData.spend + dayData.other_costs);
    });
    
    console.log('Processed dates:', dates);
    console.log('ROI data:', roiData);
    console.log('Revenue data:', revenueData);
    console.log('Spend data:', spendData);

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
    window.roiChart = new Chart(ctx, {
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
                        callback: function(value) {
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
                        label: function(context) {
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
    console.log('Chart created successfully');
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