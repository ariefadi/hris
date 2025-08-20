/**
 * Reference Ajax AdX Traffic Per Country
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
    
    // Initialize Select2 for country filter
    $('#country_filter').select2({
        placeholder: 'Select Country (Optional)',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    
    $('#btn_load_data').click(function (e) {
        load_adx_traffic_country_data();
    });
    
    // Initialize DataTable
    $('#traffic_table').DataTable({
        "responsive": true,
        "scrollX": true,
        "autoWidth": false,
        "responsive": true,
        "lengthChange": false,
        "autoWidth": false,
        "pageLength": 25,
        "order": [[1, "desc"]], // Order by impressions desc
        "columnDefs": [
            {
                "targets": [1, 2, 3, 4, 5], // Numeric columns
                "className": "text-right"
            }
        ]
    });
});

function load_adx_traffic_country_data() {
    var start_date = $('#tanggal_dari').val();
    var end_date = $('#tanggal_sampai').val();
    var country_filter = $('#country_filter').val();
    
    if (!start_date || !end_date) {
        alert('Please select both start and end dates.');
        return;
    }
    
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/page_adx_traffic_country',
        type: 'GET',
        data: {
            'start_date': start_date,
            'end_date': end_date,
            'country_filter': country_filter
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            $("#overlay").hide();
            
            if (response.status) {
                // Update summary boxes
                if (response.summary) {
                    $("#total_impressions").text(formatNumber(response.summary.total_impressions || 0));
                    $("#total_clicks").text(formatNumber(response.summary.total_clicks || 0));
                    $("#total_revenue").text('$' + formatNumber(response.summary.total_revenue || 0, 2));
                    $("#total_requests").text(formatNumber(response.summary.total_requests || 0));
                    $("#total_matched_requests").text(formatNumber(response.summary.total_matched_requests || 0));
                    
                    // Calculate and display CTR
                    var ctr = response.summary.total_impressions > 0 ? 
                        (response.summary.total_clicks / response.summary.total_impressions * 100) : 0;
                    $("#ctr").text(ctr.toFixed(2) + '%');
                }
                
                // Update DataTable
                var table = $('#traffic_table').DataTable();
                table.clear();
                
                if (response.data && response.data.length > 0) {
                    response.data.forEach(function(item) {
                        var ctr = item.impressions > 0 ? (item.clicks / item.impressions * 100) : 0;
                        var ecpm = item.impressions > 0 ? (item.revenue / item.impressions * 1000) : 0;
                        
                        table.row.add([
                            getCountryFlag(item.country_code) + ' ' + (item.country_name || item.country_code || '-'),
                            formatNumber(item.impressions || 0),
                            formatNumber(item.clicks || 0),
                            '$' + formatNumber(item.revenue || 0, 2),
                            formatNumber(item.requests || 0),
                            formatNumber(item.matched_requests || 0),
                            ctr.toFixed(2) + '%',
                            '$' + ecpm.toFixed(2)
                        ]);
                    });
                }
                
                table.draw();
                
                // Generate charts if data is available
                if (response.data && response.data.length > 0) {
                    generateTrafficCountryCharts(response.data);
                }
                
                showSuccessMessage('Country traffic data loaded successfully!');
            } else {
                alert('Error: ' + response.error);
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#overlay").hide();
            report_eror(jqXHR, textStatus);
        }
    });
}

function generateTrafficCountryCharts(data) {
    // Prepare data for charts
    var countries = [];
    var impressions = [];
    var clicks = [];
    var revenue = [];
    
    // Sort data by impressions (descending) and take top 15
    var sortedData = data.sort((a, b) => (b.impressions || 0) - (a.impressions || 0)).slice(0, 15);
    
    sortedData.forEach(function(item) {
        var countryName = item.country_name || item.country_code || 'Unknown';
        countries.push(countryName);
        impressions.push(item.impressions || 0);
        clicks.push(item.clicks || 0);
        revenue.push(parseFloat(item.revenue || 0));
    });
    
    // Chart 1: Top Countries by Impressions (World Map would be ideal, but using bar chart)
    Highcharts.chart('chart_impressions', {
        chart: {
            type: 'bar',
            height: 500
        },
        title: {
            text: 'Top 15 Countries by Impressions'
        },
        xAxis: {
            categories: countries,
            labels: {
                style: {
                    fontSize: '11px'
                }
            }
        },
        yAxis: {
            title: {
                text: 'Impressions'
            }
        },
        series: [{
            name: 'Impressions',
            data: impressions,
            color: '#007bff',
            dataLabels: {
                enabled: true,
                formatter: function() {
                    return formatNumber(this.y);
                }
            }
        }],
        legend: {
            enabled: false
        }
    });
    
    // Chart 2: Revenue Distribution by Country
    var pieData = countries.map(function(name, index) {
        return {
            name: name,
            y: revenue[index]
        };
    }).filter(item => item.y > 0).slice(0, 10); // Top 10 countries with revenue
    
    Highcharts.chart('chart_revenue', {
        chart: {
            type: 'pie',
            height: 400
        },
        title: {
            text: 'Revenue Distribution by Country (Top 10)'
        },
        series: [{
            name: 'Revenue',
            data: pieData,
            dataLabels: {
                formatter: function() {
                    return this.point.name + '<br>$' + formatNumber(this.y, 2) + 
                           ' (' + this.percentage.toFixed(1) + '%)';
                }
            }
        }],
        plotOptions: {
            pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: true,
                    distance: 15
                },
                showInLegend: false
            }
        }
    });
    
    // Chart 3: Geographic Performance Heatmap (simplified as column chart)
    var performanceData = sortedData.slice(0, 10).map(function(item) {
        var ctr = item.impressions > 0 ? (item.clicks / item.impressions * 100) : 0;
        var ecpm = item.impressions > 0 ? (item.revenue / item.impressions * 1000) : 0;
        return {
            name: item.country_name || item.country_code || 'Unknown',
            ctr: ctr,
            ecpm: ecpm
        };
    });
    
    Highcharts.chart('chart_performance', {
        chart: {
            type: 'column',
            height: 400
        },
        title: {
            text: 'Performance Metrics by Country (Top 10)'
        },
        xAxis: {
            categories: performanceData.map(item => item.name),
            labels: {
                rotation: -45,
                style: {
                    fontSize: '10px'
                }
            }
        },
        yAxis: [{
            title: {
                text: 'CTR (%)',
                style: {
                    color: '#28a745'
                }
            },
            labels: {
                style: {
                    color: '#28a745'
                }
            }
        }, {
            title: {
                text: 'eCPM ($)',
                style: {
                    color: '#ffc107'
                }
            },
            labels: {
                style: {
                    color: '#ffc107'
                }
            },
            opposite: true
        }],
        series: [{
            name: 'CTR (%)',
            data: performanceData.map(item => item.ctr),
            color: '#28a745',
            yAxis: 0
        }, {
            name: 'eCPM ($)',
            data: performanceData.map(item => item.ecpm),
            color: '#ffc107',
            yAxis: 1
        }],
        plotOptions: {
            column: {
                dataLabels: {
                    enabled: false
                }
            }
        }
    });
}

function getCountryFlag(countryCode) {
    if (!countryCode || countryCode.length !== 2) return '🌍';
    
    // Convert country code to flag emoji
    const flagMap = {
        'US': '🇺🇸', 'GB': '🇬🇧', 'CA': '🇨🇦', 'AU': '🇦🇺', 'DE': '🇩🇪',
        'FR': '🇫🇷', 'IT': '🇮🇹', 'ES': '🇪🇸', 'NL': '🇳🇱', 'SE': '🇸🇪',
        'NO': '🇳🇴', 'DK': '🇩🇰', 'FI': '🇫🇮', 'JP': '🇯🇵', 'KR': '🇰🇷',
        'CN': '🇨🇳', 'IN': '🇮🇳', 'BR': '🇧🇷', 'MX': '🇲🇽', 'AR': '🇦🇷',
        'ID': '🇮🇩', 'MY': '🇲🇾', 'SG': '🇸🇬', 'TH': '🇹🇭', 'VN': '🇻🇳',
        'PH': '🇵🇭', 'RU': '🇷🇺', 'PL': '🇵🇱', 'CZ': '🇨🇿', 'HU': '🇭🇺'
    };
    
    return flagMap[countryCode.toUpperCase()] || '🌍';
}

function formatNumber(num, decimals = 0) {
    if (num === null || num === undefined) return '0';
    return parseFloat(num).toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
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