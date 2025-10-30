/**
 * Reference Ajax AdSense Traffic Per Account
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
    
    // Initialize Select2 for domain filter
    $('#site_filter').select2({
        placeholder: 'Pilih Domain (Opsional)',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });

    // Auto load data on page load
    var tanggal_dari = $("#tanggal_dari").val();
    var tanggal_sampai = $("#tanggal_sampai").val();
    if (tanggal_dari != "" && tanggal_sampai != "") {
        load_adsense_traffic_account_data(tanggal_dari, tanggal_sampai);
    }
    
    // Load domains list on page load
    loadDomainsList();
    
    function loadDomainsList() {
        $.ajax({
            url: '/management/admin/adsense_sites_list',
            type: 'GET',
            dataType: 'json',
            success: function(response) {
                if (response.status) {
                    // Clear existing options except the first one
                    $('#site_filter').empty().append('<option value="">Semua Domain</option>');
                    
                    // Add domains to dropdown
                    response.data.forEach(function(domain) {
                        $('#site_filter').append('<option value="' + domain + '">' + domain + '</option>');
                    });
                    
                    // Refresh Select2
                    $('#site_filter').trigger('change');
                } else {
                    console.error('Failed to load domains:', response.error);
                }
            },
            error: function(xhr, status, error) {
                console.error('Error loading domains:', error);
            }
        });
    }
    
    $('#btn_load_data').click(function (e) {
        load_adsense_traffic_account_data();
    });
    
    // Initialize DataTable with error handling
    var dataTable;
    function initializeDataTable() {
        if ($.fn.DataTable.isDataTable('#table_traffic_account')) {
            $('#table_traffic_account').DataTable().destroy();
        }
        
        dataTable = $('#table_traffic_account').DataTable({
            "responsive": true,
            "lengthChange": false,
            "autoWidth": false,
            "buttons": ["copy", "csv", "excel", "pdf", "print", "colvis"],
            "columnDefs": [
                {
                    "targets": [3, 4, 5, 6, 7, 8], // Numeric columns (Klik, Tayangan, CTR, CPM, CPC, Pendapatan)
                    "className": "text-right"
                }
            ],
            "language": {
                "emptyTable": "Tidak ada data yang tersedia",
                "info": "Menampilkan _START_ sampai _END_ dari _TOTAL_ entri",
                "infoEmpty": "Menampilkan 0 sampai 0 dari 0 entri",
                "infoFiltered": "(disaring dari _MAX_ total entri)",
                "lengthMenu": "Tampilkan _MENU_ entri",
                "loadingRecords": "Memuat...",
                "processing": "Sedang memproses...",
                "search": "Cari:",
                "zeroRecords": "Tidak ditemukan data yang sesuai"
            }
        });
        
        if (dataTable && dataTable.buttons) {
            dataTable.buttons().container().appendTo('#table_traffic_account_wrapper .col-md-6:eq(0)');
        }
    }
    
    // Initialize DataTable on page load
    initializeDataTable();
});

function load_adsense_traffic_account_data() {
    var start_date = $('#tanggal_dari').val();
    var end_date = $('#tanggal_sampai').val();
    var site_filter = $('#site_filter').val();
    
    if (!start_date || !end_date) {
        alert('Please select both start and end dates.');
        return;
    }
    
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/adsense_traffic_account_data',
        type: 'POST',
        data: {
            'start_date': start_date,
            'end_date': end_date,
            'site_filter': site_filter
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            $("#overlay").hide();
            
            if (response && response.status) {
                // Update summary boxes
                if (response.summary) {
                    $("#total_impressions").text(formatNumber(response.summary.total_impressions || 0));
                    $("#total_clicks").text(formatNumber(response.summary.total_clicks || 0));
                    $("#total_revenue").text(formatCurrencyIDR(response.summary.total_revenue || 0));
                    $("#avg_ctr").text(formatNumber(response.summary.avg_ctr || 0, 2) + '%');
                    $("#average_cpc").text(formatCurrencyIDR(response.summary.avg_cpc || 0));
                    $("#avg_cpm").text(formatCurrencyIDR(response.summary.avg_cpm || 0));
                    
                    // Show summary boxes
                    $('#summary_boxes').show();
                    $('#summary_boxes_2').show();
                }
                
                // Update DataTable
                var table = $('#table_traffic_account').DataTable();
                table.clear();
                
                // Check if there's a message (no data scenario)
                if (response.message) {
                    // Show informative message for no data
                    table.row.add([
                        '',
                        '',
                        '<div class="alert alert-info" style="margin: 20px; text-align: left;"><h5><i class="fas fa-info-circle"></i> Tidak Ada Data AdSense</h5><p>' + response.message + '</p><hr><small><strong>Saran:</strong><br>• Periksa <a href="https://www.google.com/adsense/" target="_blank">Google AdSense Dashboard</a> untuk memverifikasi data<br>• Pastikan kode iklan AdSense sudah terpasang dengan benar di website<br>• Coba periode tanggal yang berbeda jika akun baru saja aktif</small></div>',
                        '', '', '', '', '', '', ''
                    ]);
                }
                // Populate table with data
                else if (response.data && response.data.length > 0) {
                    var rowNumber = 1;
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
                            rowNumber++,
                            formattedDate,
                            item.site_name || item.ad_unit_name || '-',
                            formatNumber(item.clicks || 0),
                            formatNumber(item.impressions || 0),
                            formatNumber(item.ctr || 0, 2) + '%',
                            formatCurrencyIDR(item.cpm || 0),
                            formatCurrencyIDR(item.cpc || 0),
                            formatCurrencyIDR(item.revenue || 0)
                        ]);
                    });
                } else {
                    table.row.add([
                        '', '', 'Tidak ada data tersedia untuk periode ini',
                        '', '', '', '', '', '', ''
                    ]);
                }
                
                table.draw();
                
                // Reinitialize DataTable after data is loaded
                initializeDataTable();
                
                showSuccessMessage('Traffic data loaded successfully!');
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