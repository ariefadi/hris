/**
 * Reference Ajax AdX Traffic Per Campaign
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
    
    // Initialize Select2 for campaign filter
    $('#campaign_filter').select2({
        placeholder: 'Select Campaign (Optional)',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    
    $('#btn_load_data').click(function (e) {
        load_adx_traffic_campaign_data();
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
        "order": [[2, "desc"]], // Order by impressions desc
        "columnDefs": [
            {
                "targets": [2, 3, 4, 5, 6], // Numeric columns
                "className": "text-right"
            }
        ]
    });
});

function load_adx_traffic_campaign_data() {
    var start_date = $('#tanggal_dari').val();
    var end_date = $('#tanggal_sampai').val();
    var campaign_filter = $('#campaign_filter').val();
    
    if (!start_date || !end_date) {
        alert('Please select both start and end dates.');
        return;
    }
    
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/page_adx_traffic_campaign',
        type: 'GET',
        data: {
            'start_date': start_date,
            'end_date': end_date,
            'campaign_filter': campaign_filter
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
                            item.order_name || '-',
                            item.line_item_name || '-',
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
                    generateTrafficCampaignCharts(response.data);
                }
                
                showSuccessMessage('Campaign traffic data loaded successfully!');
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

function generateTrafficCampaignCharts(data) {
    // Prepare data for charts
    var campaigns = [];
    var impressions = [];
    var clicks = [];
    var revenue = [];
    
    // Sort data by impressions (descending) and take top 10
    var sortedData = data.sort((a, b) => (b.impressions || 0) - (a.impressions || 0)).slice(0, 10);
    
    sortedData.forEach(function(item) {
        var campaignName = (item.order_name || 'Unknown') + ' - ' + (item.line_item_name || 'Unknown');
        campaigns.push(campaignName.length > 30 ? campaignName.substring(0, 30) + '...' : campaignName);
        impressions.push(item.impressions || 0);
        clicks.push(item.clicks || 0);
        revenue.push(parseFloat(item.revenue || 0));
    });
    
    // Chart 1: Top Campaigns by Performance
    Highcharts.chart('chart_performance', {
        chart: {
            type: 'column',
            height: 400
        },
        title: {
            text: 'Top 10 Campaigns by Impressions'
        },
        xAxis: {
            categories: campaigns,
            labels: {
                rotation: -45,
                style: {
                    fontSize: '9px'
                }
            }
        },
        yAxis: [{
            title: {
                text: 'Impressions',
                style: {
                    color: '#007bff'
                }
            },
            labels: {
                style: {
                    color: '#007bff'
                }
            }
        }, {
            title: {
                text: 'Clicks',
                style: {
                    color: '#28a745'
                }
            },
            labels: {
                style: {
                    color: '#28a745'
                }
            },
            opposite: true
        }],
        series: [{
            name: 'Impressions',
            data: impressions,
            color: '#007bff',
            yAxis: 0
        }, {
            name: 'Clicks',
            data: clicks,
            color: '#28a745',
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
    
    // Chart 2: Revenue Distribution by Campaign
    var pieData = campaigns.map(function(name, index) {
        return {
            name: name,
            y: revenue[index]
        };
    }).filter(item => item.y > 0); // Only show campaigns with revenue
    
    Highcharts.chart('chart_revenue', {
        chart: {
            type: 'pie',
            height: 400
        },
        title: {
            text: 'Revenue Distribution by Campaign'
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
                    distance: 20
                },
                showInLegend: false
            }
        }
    });
    
    // Chart 3: CTR Analysis
    var ctrData = sortedData.map(function(item) {
        var ctr = item.impressions > 0 ? (item.clicks / item.impressions * 100) : 0;
        return {
            name: (item.order_name || 'Unknown') + ' - ' + (item.line_item_name || 'Unknown'),
            y: ctr
        };
    });
    
    Highcharts.chart('chart_ctr', {
        chart: {
            type: 'bar',
            height: 400
        },
        title: {
            text: 'Click-Through Rate (CTR) by Campaign'
        },
        xAxis: {
            categories: campaigns,
            labels: {
                style: {
                    fontSize: '9px'
                }
            }
        },
        yAxis: {
            title: {
                text: 'CTR (%)'
            }
        },
        series: [{
            name: 'CTR (%)',
            data: ctrData.map(item => item.y),
            color: '#ffc107',
            dataLabels: {
                enabled: true,
                formatter: function() {
                    return this.y.toFixed(2) + '%';
                }
            }
        }],
        legend: {
            enabled: false
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