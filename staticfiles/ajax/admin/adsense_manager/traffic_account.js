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
    // Initialize date pickers using Flatpickr like Summary page
    var toISO = function(d) {
        var pad = function(x) { return String(x).padStart(2, '0'); };
        return d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate());
    };
    var today = new Date();
    var start = new Date(today);
    start.setDate(today.getDate() - 6);
    
    if (typeof flatpickr !== 'undefined') {
        flatpickr('#tanggal_dari', { dateFormat: 'Y-m-d' });
        flatpickr('#tanggal_sampai', { dateFormat: 'Y-m-d' });
        // Set default dates and sync with flatpickr instances if present
        var dariEl = document.getElementById('tanggal_dari');
        var sampaiEl = document.getElementById('tanggal_sampai');
        dariEl.value = toISO(start);
        sampaiEl.value = toISO(today);
        if (dariEl._flatpickr) dariEl._flatpickr.setDate(dariEl.value, true);
        if (sampaiEl._flatpickr) sampaiEl._flatpickr.setDate(sampaiEl.value, true);
    } else {
        // Fallback: set plain input values
        $('#tanggal_dari').val(toISO(start));
        $('#tanggal_sampai').val(toISO(today));
    }
    
    // Initialize Select2 for account filter
    $('#account_filter').select2({
        placeholder: 'Pilih Akun',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });

    // Auto load accounts list on page load after date defaults set
    loadAccountsList();
    // Load accounts list on page load
    function loadAccountsList() {
        $.ajax({
            url: '/management/admin/adsense_credentials_list',
            type: 'GET',
            dataType: 'json',
            success: function(response) {
                if (response.status) {
                    // Clear existing options and set default
                    $('#account_filter').empty().append('<option value="">Semua Akun</option>');

                    // Add accounts to dropdown (value: user_mail, text: account_name)
                    response.data.forEach(function(account) {
                        $('#account_filter').append('<option value="' + account.user_mail + '">' + (account.account_name || account.user_mail) + '</option>');
                    });

                    // Refresh Select2
                    $('#account_filter').trigger('change');
                } else {
                    console.error('Failed to load accounts:', response.error);
                }
            },
            error: function(xhr, status, error) {
                console.error('Error loading accounts:', error);
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
    var account_filter = $('#account_filter').val();
    
    if (!start_date || !end_date) {
        alert('Please select both start and end dates.');
        return;
    }
    // Validasi: wajib pilih akun terlebih dahulu
    if (!account_filter) {
        alert('Filter Account harus dipilih terlebih dahulu');
        return;
    }
    
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/adsense_traffic_account_data',
        type: 'POST',
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
            
            if (response && response.status) {
                // Tampilkan section chart
                $('#charts_section').show();
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
                        '', '', 
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
                
                // Render line chart untuk pendapatan harian
                try {
                    // Pastikan Highcharts ter-load, jika belum, tunggu sebentar
                    if (typeof Highcharts === 'undefined') {
                        console.warn('Highcharts belum tersedia, menunggu 300ms...');
                        setTimeout(function(){
                            load_adsense_traffic_account_data();
                        }, 300);
                        return;
                    }
                    // Helper: peta nama bulan Indonesia -> index (0-11)
                    var monthMap = {
                        'januari': 0, 'jan': 0,
                        'februari': 1, 'feb': 1,
                        'maret': 2, 'mar': 2,
                        'april': 3, 'apr': 3,
                        'mei': 4,
                        'juni': 5, 'jun': 5,
                        'juli': 6, 'jul': 6,
                        'agustus': 7, 'agu': 7,
                        'september': 8, 'sep': 8,
                        'oktober': 9, 'okt': 9,
                        'november': 10, 'nov': 10,
                        'desember': 11, 'des': 11
                    };
                    function parseIndoDateToISO(text, yearHint) {
                        if (!text) return null;
                        var m = text.trim().match(/(\d{1,2})\s+([A-Za-zÀ-ÿ]+)\s*(\d{4})?/i);
                        if (!m) return null;
                        var day = parseInt(m[1], 10);
                        var monthName = (m[2] || '').toLowerCase();
                        var monthIdx = monthMap.hasOwnProperty(monthName) ? monthMap[monthName] : null;
                        if (monthIdx === null) return null;
                        var year = m[3] ? parseInt(m[3], 10) : (yearHint || (new Date()).getFullYear());
                        var yyyy = String(year).padStart(4, '0');
                        var mm = String(monthIdx + 1).padStart(2, '0');
                        var dd = String(day).padStart(2, '0');
                        return yyyy + '-' + mm + '-' + dd;
                    }

                    var dailyMap = {};
                    var apiIsoSet = new Set();
                    var apiYears = [];
                    // Ambil dari response API
                    if (response.data && response.data.length > 0) {
                        response.data.forEach(function(item) {
                            var d = item.date;
                            var rev = parseFloat(item.revenue || 0) || 0;
                            if (d) {
                                // Pastikan key dalam ISO yyyy-mm-dd
                                var isoKey = d.match(/\d{4}-\d{2}-\d{2}/) ? d : null;
                                if (isoKey) {
                                    dailyMap[isoKey] = (dailyMap[isoKey] || 0) + rev;
                                    apiIsoSet.add(isoKey);
                                    try { apiYears.push(new Date(isoKey + 'T00:00:00').getFullYear()); } catch(e) {}
                                }
                            }
                        });
                    }
                    var yearHint = apiYears.length ? apiYears[0] : null;
                    // Gabungkan dari tabel (fallback + pelengkap)
                    $('#table_traffic_account tbody tr').each(function() {
                        var dateText = $(this).find('td:nth-child(2)').text().trim();
                        var revenueText = $(this).find('td:nth-child(9)').text().trim();
                        if (dateText) {
                            var rv = parseInt((revenueText || '').replace(/[^0-9]/g, '')) || 0;
                            if (rv > 0) {
                                var isoFromTable = parseIndoDateToISO(dateText, yearHint);
                                // Hindari duplikasi jika API sudah menyediakan tanggal tsb
                                if (isoFromTable) {
                                    if (!apiIsoSet.has(isoFromTable)) {
                                        dailyMap[isoFromTable] = (dailyMap[isoFromTable] || 0) + rv;
                                    }
                                }
                            }
                        }
                    });
                    // Jika summary memiliki daily breakdown di masa depan
                    if (response.daily && response.daily.length > 0) {
                        response.daily.forEach(function(day) {
                            var d = day.date || day.day || day.date_str;
                            var rev = parseFloat(day.revenue || day.total_revenue || 0) || 0;
                            if (d) {
                                var isoFromDaily = d.match(/\d{4}-\d{2}-\d{2}/) ? d : parseIndoDateToISO(d, yearHint);
                                if (isoFromDaily) {
                                    dailyMap[isoFromDaily] = (dailyMap[isoFromDaily] || 0) + rev;
                                }
                            }
                        });
                    }
                    // Bangun titik data [timestamp, value] dan urutkan naik
                    var seriesData = Object.keys(dailyMap).map(function(iso) {
                        var dt = new Date(iso + 'T00:00:00');
                        var ts = Date.UTC(dt.getFullYear(), dt.getMonth(), dt.getDate());
                        return [ts, Math.round(dailyMap[iso])];
                    }).sort(function(a, b){ return a[0] - b[0]; });
                    // Jika tetap tidak ada data, tampilkan pesan di dalam container
                    if (seriesData.length === 0) {
                        var container = document.getElementById('chart_daily_revenue');
                        if (container) {
                            container.innerHTML = '<div class="alert alert-info">Tidak ada data pendapatan harian untuk periode ini.</div>';
                        }
                    }
                    // Render dengan sedikit delay untuk memastikan container sudah terlihat
                    setTimeout(function() {
                        Highcharts.chart('chart_daily_revenue', {
                            chart: { type: 'line', backgroundColor: 'transparent' },
                            title: { text: 'Pendapatan Harian' },
                            xAxis: {
                                type: 'datetime',
                                tickPixelInterval: 50,
                                dateTimeLabelFormats: {
                                    day: '%e %b %Y',
                                    week: '%e %b %Y',
                                    month: '%b %Y'
                                }
                            },
                            yAxis: {
                                title: { text: 'Pendapatan (Rp)' },
                                labels: {
                                    formatter: function() {
                                        return 'Rp ' + (this.value || 0).toLocaleString('id-ID');
                                    }
                                }
                            },
                            tooltip: {
                                backgroundColor: 'rgba(0,0,0,0.85)',
                                borderColor: '#333',
                                style: { color: '#fff' },
                                xDateFormat: '%e %b %Y',
                                formatter: function() {
                                    var val = Math.round(this.point.y || 0);
                                    var dateLabel = Highcharts.dateFormat('%e %b %Y', this.x);
                                    return '<b>' + dateLabel + '</b><br/>Pendapatan: <b>Rp ' + val.toLocaleString('id-ID') + '</b>';
                                }
                            },
                            legend: { enabled: false },
                            series: [{ name: 'Pendapatan', data: seriesData, color: '#1f77b4' }],
                            credits: { enabled: false }
                        });
                        // Paksa reflow agar chart menyesuaikan ukuran container
                        try { window.dispatchEvent(new Event('resize')); } catch(e) {}
                    }, 500);
                } catch (e) {
                    console.error('Failed to render daily revenue chart:', e);
                }
                
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