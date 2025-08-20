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
    
    $('#tanggal_dari').datepicker({
      format: 'yyyy-mm-dd',
      autoclose: true,
      todayHighlight: true
    });
    
    $('#tanggal_sampai').datepicker({
      format: 'yyyy-mm-dd',
      autoclose: true,
      todayHighlight: true
    });
    
    // Set default dates (last 7 days)
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    $('#tanggal_dari').val(lastWeek.toISOString().split('T')[0]);
    $('#tanggal_sampai').val(today.toISOString().split('T')[0]);
    
    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        
        if(tanggal_dari != "" && tanggal_sampai != "") {
            load_adx_summary_data(tanggal_dari, tanggal_sampai);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    
    // Auto load data on page load
    var tanggal_dari = $("#tanggal_dari").val();
    var tanggal_sampai = $("#tanggal_sampai").val();
    if(tanggal_dari != "" && tanggal_sampai != "") {
        load_adx_summary_data(tanggal_dari, tanggal_sampai);
    }
});

function load_adx_summary_data(tanggal_dari, tanggal_sampai) {
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/page_adx_summary',
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
            
            if (response.status) {
                // Update summary boxes
                $("#impressions").text(formatNumber(response.summary.impressions));
                $("#clicks").text(formatNumber(response.summary.clicks));
                $("#revenue").text('$' + formatNumber(response.summary.revenue, 2));
                $("#requests").text(formatNumber(response.summary.requests));
                $("#matched_requests").text(formatNumber(response.summary.matched_requests));
                
                // Calculate and display derived metrics
                var ctr = response.summary.impressions > 0 ? 
                    (response.summary.clicks / response.summary.impressions * 100) : 0;
                var match_rate = response.summary.requests > 0 ? 
                    (response.summary.matched_requests / response.summary.requests * 100) : 0;
                var ecpm = response.summary.impressions > 0 ? 
                    (response.summary.revenue / response.summary.impressions * 1000) : 0;
                
                $("#ctr").text(formatNumber(ctr, 2) + '%');
                $("#match_rate").text(formatNumber(match_rate, 2) + '%');
                $("#ecpm").text('$' + formatNumber(ecpm, 2));
                
                // Create charts if data exists
                if (response.data && response.data.length > 0) {
                    create_adx_summary_charts(response.data);
                }
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

function create_adx_summary_charts(data) {
    // Prepare data for charts
    var dates = [];
    var impressions = [];
    var clicks = [];
    var revenue = [];
    var requests = [];
    var matched_requests = [];
    
    data.forEach(function(item) {
        dates.push(item.date);
        impressions.push(parseInt(item.impressions));
        clicks.push(parseInt(item.clicks));
        revenue.push(parseFloat(item.revenue));
        requests.push(parseInt(item.requests));
        matched_requests.push(parseInt(item.matched_requests));
    });
    
    // Create main performance chart
    Highcharts.chart('container', {
        chart: {
            type: 'line'
        },
        title: {
            text: 'AdX Performance Over Time'
        },
        xAxis: {
            categories: dates,
            title: {
                text: 'Date'
            }
        },
        yAxis: [{
            title: {
                text: 'Impressions / Clicks / Requests',
                style: {
                    color: Highcharts.getOptions().colors[0]
                }
            }
        }, {
            title: {
                text: 'Revenue ($)',
                style: {
                    color: Highcharts.getOptions().colors[1]
                }
            },
            opposite: true
        }],
        series: [{
            name: 'Impressions',
            data: impressions,
            yAxis: 0
        }, {
            name: 'Clicks',
            data: clicks,
            yAxis: 0
        }, {
            name: 'Requests',
            data: requests,
            yAxis: 0
        }, {
            name: 'Matched Requests',
            data: matched_requests,
            yAxis: 0
        }, {
            name: 'Revenue',
            data: revenue,
            yAxis: 1,
            type: 'column'
        }],
        tooltip: {
            shared: true
        },
        legend: {
            layout: 'horizontal',
            align: 'center',
            verticalAlign: 'bottom'
        }
    });
    
    // Create activity chart (requests vs matched requests)
    Highcharts.chart('container-activity', {
        chart: {
            type: 'column'
        },
        title: {
            text: 'Request Activity'
        },
        xAxis: {
            categories: dates
        },
        yAxis: {
            title: {
                text: 'Count'
            }
        },
        series: [{
            name: 'Total Requests',
            data: requests
        }, {
            name: 'Matched Requests',
            data: matched_requests
        }],
        tooltip: {
            shared: true
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