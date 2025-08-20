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
        placeholder: 'Select Ad Unit (Optional)',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    
    $('#btn_load_data').click(function (e) {
        load_adx_traffic_account_data();
    });
    
    // Initialize DataTable
    $('#traffic_table').DataTable({
        "responsive": true,
        "lengthChange": false,
        "autoWidth": false,
        "pageLength": 25,
        "order": [[0, "asc"]],
        "columnDefs": [
            {
                "targets": [2, 3, 4, 5, 6], // Numeric columns
                "className": "text-right"
            }
        ]
    });
});

function load_adx_traffic_account_data() {
    var start_date = $('#tanggal_dari').val();
    var end_date = $('#tanggal_sampai').val();
    var account_filter = $('#account_filter').val();
    
    if (!start_date || !end_date) {
        alert('Please select both start and end dates.');
        return;
    }
    
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/page_adx_traffic_account',
        type: 'GET',
        data: {
            'start_date': start_date,
            'end_date': end_date,
            'account_filter': account_filter
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
                            item.ad_unit_name || '-',
                            item.ad_unit_id || '-',
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
                    generateTrafficAccountCharts(response.data);
                }
                
                showSuccessMessage('Traffic data loaded successfully!');
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

function generateTrafficAccountCharts(data) {
    // Prepare data for charts
    var adUnits = [];
    var impressions = [];
    var clicks = [];
    var revenue = [];
    
    // Sort data by impressions (descending) and take top 10
    var sortedData = data.sort((a, b) => (b.impressions || 0) - (a.impressions || 0)).slice(0, 10);
    
    sortedData.forEach(function(item) {
        adUnits.push(item.ad_unit_name || 'Unknown');
        impressions.push(item.impressions || 0);
        clicks.push(item.clicks || 0);
        revenue.push(parseFloat(item.revenue || 0));
    });
    
    // Chart 1: Top Ad Units by Impressions
    Highcharts.chart('chart_impressions', {
        chart: {
            type: 'column',
            height: 400
        },
        title: {
            text: 'Top 10 Ad Units by Impressions'
        },
        xAxis: {
            categories: adUnits,
            labels: {
                rotation: -45,
                style: {
                    fontSize: '10px'
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
            color: '#007bff'
        }],
        legend: {
            enabled: false
        },
        plotOptions: {
            column: {
                dataLabels: {
                    enabled: true,
                    formatter: function() {
                        return formatNumber(this.y);
                    }
                }
            }
        }
    });
    
    // Chart 2: Revenue Distribution
    Highcharts.chart('chart_revenue', {
        chart: {
            type: 'pie',
            height: 400
        },
        title: {
            text: 'Revenue Distribution by Ad Unit'
        },
        series: [{
            name: 'Revenue',
            data: adUnits.map(function(name, index) {
                return {
                    name: name,
                    y: revenue[index]
                };
            }),
            dataLabels: {
                formatter: function() {
                    return this.point.name + '<br>$' + formatNumber(this.y, 2);
                }
            }
        }]
    });
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