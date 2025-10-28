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
    // Initialize Select2 for site filter
    $('#site_filter').select2({
        placeholder: '-- Pilih Domain --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    // Load sites list on page load
    loadSitesList();
    function loadSitesList() {
        $("#overlay").show();
        $.ajax({
            url: '/management/admin/adx_sites_list',
            type: 'GET',
            dataType: 'json',
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
            }
        });
    }
    $('#btn_load_data').click(function (e) {
        load_adx_traffic_account_data();
    });
    // Initialize DataTable
    $('#table_traffic_account').DataTable({
        "paging": true,
        "pageLength": 25,
        "lengthChange": true,
        "searching": true,
        "ordering": true,
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
    var selectedSites = $('#site_filter').val();
    if (!start_date || !end_date) {
        alert('Please select both start and end dates.');
        return;
    }
    // Convert array to comma-separated string for backend
    var siteFilter = '';
    if (selectedSites && selectedSites.length > 0) {
        siteFilter = selectedSites.join(',');
    }
    $("#overlay").show();
    $.ajax({
        url: '/management/admin/page_adx_traffic_account',
        type: 'GET',
        data: {
            'start_date': start_date,
            'end_date': end_date,
            'selected_sites': siteFilter
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            if (response && response.status) {
                // Update summary boxes
                if (response.summary) {
                    $("#total_clicks").text(formatNumber(response.summary.total_clicks || 0));
                    $("#avg_cpc").text(formatCurrencyIDR(response.summary.avg_cpc || 0));
                    $("#avg_ecpm").text(formatCurrencyIDR(response.summary.avg_ecpm || 0));
                    $("#avg_ctr").text(formatNumber(response.summary.avg_ctr || 0, 2) + '%');
                    $("#total_revenue").text(formatCurrencyIDR(response.summary.total_revenue || 0));
                    // Show summary boxes
                    $('#summary_boxes').show();
                }
                // Update DataTable
                var table = $('#table_traffic_account').DataTable();
                table.clear();
                if (response.data && response.data.length > 0) {
                    response.data.forEach(function(item) {
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
                            formatNumber(item.clicks || 0),
                            formatCurrencyIDR(item.cpc || 0),
                            formatCurrencyIDR(item.ecpm || 0),
                            formatNumber(item.ctr || 0, 2) + ' %',
                            formatCurrencyIDR(item.revenue || 0)
                        ]);
                    });
                }
                table.draw();
                showSuccessMessage('Traffic data loaded successfully!');
                $("#overlay").hide();
            } else {
                alert('Error: ' + (response && response.error ? response.error : 'Unknown error occurred'));
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