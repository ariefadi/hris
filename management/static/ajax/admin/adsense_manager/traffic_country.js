/**
 * Reference Ajax AdSense Traffic Per Country
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
    
    // Initialize date pickers (Flatpickr first, fallback ke jQuery datepicker)
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);

    if (typeof flatpickr !== 'undefined') {
        flatpickr('#tanggal_dari', {
            dateFormat: 'Y-m-d',
            defaultDate: lastWeek
        });
        flatpickr('#tanggal_sampai', {
            dateFormat: 'Y-m-d',
            defaultDate: today
        });
    } else {
        // Fallback: gunakan jQuery datepicker jika Flatpickr tidak tersedia
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
    }
    
    // Initialize Select2 for account filter
    $('#account_filter').select2({
        placeholder: 'Pilih Akun (Opsional)',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });

    // Initialize Select2 for country filter (multi-select with tags)
    $('#country_filter').select2({
        placeholder: 'Pilih Negara (Opsional)',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4',
        tags: true,
        tokenSeparators: [','],
        closeOnSelect: false
    });

    // Populate country options based on selected account and date range
    function populateCountryOptions() {
        var start_date = $('#tanggal_dari').val();
        var end_date = $('#tanggal_sampai').val();
        var account_filter = $('#account_filter').val();

        if (!start_date || !end_date) {
            return; // need dates to query countries
        }

        // Require account selection before loading countries to avoid backend errors
        if (!account_filter) {
            $('#country_filter').prop('disabled', true);
            $('#country_filter').empty().append('<option value="">Pilih akun terlebih dahulu</option>');
            $('#country_filter').trigger('change');
            return;
        }

        // Disable while loading and show temporary message
        $('#country_filter').prop('disabled', true);
        $('#country_filter').empty().append('<option value="">Memuat daftar negara...</option>');
        $('#country_filter').trigger('change');

        $.ajax({
            url: '/management/admin/adsense_traffic_country_data',
            type: 'POST',
            data: {
                'start_date': start_date,
                'end_date': end_date,
                'account_filter': account_filter || '',
                'country_filter': '' // request all, server supports filtering later
            },
            success: function (response) {
                var countriesSet = new Set();
                var countries = [];

                if (response && response.status && Array.isArray(response.data)) {
                    response.data.forEach(function(item) {
                        var name = (item.country || '').trim();
                        if (name && !countriesSet.has(name)) {
                            countriesSet.add(name);
                            countries.push(name);
                        }
                    });
                }

                // Sort alphabetically
                countries.sort(function(a, b){return a.localeCompare(b);});

                // Rebuild options
                $('#country_filter').empty();
                // Keep a default empty option for clear
                $('#country_filter').append('<option value="">Semua Negara</option>');
                countries.forEach(function(name){
                    $('#country_filter').append('<option value="' + name + '">' + name + '</option>');
                });
            },
            error: function () {
                // On error, clear options gracefully
                $('#country_filter').empty().append('<option value="">Semua Negara</option>');
            },
            complete: function () {
                $('#country_filter').prop('disabled', false).trigger('change');
            }
        });
    }

    // Auto load data on page load
    var tanggal_dari = $("#tanggal_dari").val();
    var tanggal_sampai = $("#tanggal_sampai").val();
    if (tanggal_dari != "" && tanggal_sampai != "") {
        loadAccountsList();
        // Populate countries once dates are set
        populateCountryOptions();
    }

    // Re-populate country options when account or dates change
    $('#account_filter').on('change', function(){
        populateCountryOptions();
    });
    $('#tanggal_dari, #tanggal_sampai').on('change', function(){
        populateCountryOptions();
    });
    
    // Load accounts list on page load
    function loadAccountsList() {
        $.ajax({
            url: '/management/admin/adsense_credentials_list',
            type: 'GET',
            dataType: 'json',
            success: function(response) {
                if (response.status) {
                    // Clear existing options except the first one
                    $('#account_filter').empty().append('<option value="">Semua Akun</option>');
                    
                    // Add accounts to dropdown (value: user_mail, text: account_name)
                    response.data.forEach(function(account) {
                        $('#account_filter').append('<option value="' + account.user_mail + '">' + (account.account_name || account.user_mail) + '</option>');
                    });
                    
                    // Refresh Select2
                    $('#account_filter').trigger('change');

                    // Populate countries based on newly loaded accounts
                    populateCountryOptions();
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
        load_adsense_traffic_country_data();
    });
    
    // Initialize DataTable on page load
    initializeDataTable();
});

// Initialize DataTable with error handling - Global function
var dataTable;
function initializeDataTable() {
    if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
        $('#table_traffic_country').DataTable().destroy();
    }
    
    dataTable = $('#table_traffic_country').DataTable({
        "responsive": true,
        "lengthChange": false,
        "autoWidth": false,
        "buttons": ["copy", "csv", "excel", "pdf", "print", "colvis"],
        "columnDefs": [
            {
                "targets": [2, 3, 4, 5, 6, 7], // Numeric columns (Klik, Tayangan, CTR, CPM, CPC, Pendapatan)
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
        dataTable.buttons().container().appendTo('#table_traffic_country_wrapper .col-md-6:eq(0)');
    }
}

function load_adsense_traffic_country_data() {
    var start_date = $('#tanggal_dari').val();
    var end_date = $('#tanggal_sampai').val();
    var account_filter = $('#account_filter').val();
    var country_filter = $('#country_filter').val();
    // If multiple selected values (array), send as comma-separated string
    if (Array.isArray(country_filter)) {
        country_filter = country_filter.join(',');
    }
    
    if (!start_date || !end_date) {
        alert('Please select both start and end dates.');
        return;
    }

    // Require account selection to minimize server errors
    if (!account_filter) {
        // Tampilkan peringatan yang jelas dan fokuskan ke filter akun
        alert('Filter Account harus dipilih terlebih dahulu.');
        $('#account_filter').select2('open');
        return;
    }
    
    $("#overlay").show();
    
    $.ajax({
        url: '/management/admin/adsense_traffic_country_data',
        type: 'POST',
        data: {
            'start_date': start_date,
            'end_date': end_date,
            'account_filter': account_filter,
            'country_filter': country_filter
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
                var table = $('#table_traffic_country').DataTable();
                table.clear();
                
                // Check if there's a message (no data scenario)
                if (response.message) {
                    // Show informative message for no data
                    table.row.add([
                        '',
                        '',
                        '<div class="alert alert-info" style="margin: 20px; text-align: left;"><h5><i class="fas fa-info-circle"></i> Tidak Ada Data AdSense</h5><p>' + response.message + '</p><hr><small><strong>Saran:</strong><br>• Periksa <a href="https://www.google.com/adsense/" target="_blank">Google AdSense Dashboard</a> untuk memverifikasi data<br>• Pastikan kode iklan AdSense sudah terpasang dengan benar di website<br>• Coba periode tanggal yang berbeda jika akun baru saja aktif</small></div>',
                        '', '', '', '', '', ''
                    ]);
                }
                // Populate table with data
                else if (response.data && response.data.length > 0) {
                    var rowNumber = 1;
                    response.data.forEach(function(item) {
                        table.row.add([
                            rowNumber++,
                            item.country || '-',
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
                        '', '', '', '', '', ''
                    ]);
                }
                
                table.draw();
                
                // Reinitialize DataTable after data is loaded
                initializeDataTable();

                // Render world map based on impressions per country
                try {
                    var mapData = Array.isArray(response.data) ? response.data : [];
                    renderWorldMap(mapData);
                } catch (e) {
                    console.error('Failed to render world map:', e);
                }
                
                showSuccessMessage('Traffic data loaded successfully!');
            } else {
                // Tampilkan error yang lebih informatif bila akun tidak memiliki AdSense
                var errMsg = (response && response.error ? response.error : 'Unknown error occurred');
                alert('Info: ' + errMsg);
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            $("#overlay").hide();
            report_eror(jqXHR, textStatus);
        }
    });
}

// Render Highcharts world map with impressions as value
function renderWorldMap(countries) {
    if (typeof Highcharts === 'undefined' || !Highcharts.mapChart) {
        console.warn('Highcharts Maps is not loaded. Skipping map rendering.');
        return;
    }

    if (!Highcharts.maps || !Highcharts.maps['custom/world']) {
        console.warn('World map data not available. Ensure world.js is loaded.');
        return;
    }

    // Prepare data similar to AdX: use revenue, join by hc-key
    var mapData = [];
    (countries || []).forEach(function(item) {
        var code = (item.country_code || '').toLowerCase();
        var revenue = parseFloat(item.revenue || 0) || 0;
        if (!code) return; // skip if no code
        mapData.push({
            'hc-key': code,
            code: item.country_code || '',
            name: item.country || 'Unknown',
            value: revenue,
            impressions: item.impressions || 0,
            clicks: item.clicks || 0,
            ctr: item.ctr || 0,
            cpm: item.cpm || 0,
            cpc: item.cpc || 0
        });
    });

    // Define fixed ranges identical to AdX
    var ranges = [
        { from: null, to: null, color: '#E6E7E8', name: 'Tidak ada data' },
        { from: 0, to: 50000, color: '#E6F2FF', name: 'Rp.0 - Rp.50.000' },
        { from: 50000, to: 100000, color: '#CDE7FF', name: 'Lebih dari Rp.50.000 - Rp.100.000' },
        { from: 100000, to: 500000, color: '#9FD0FF', name: 'Lebih dari Rp.100.000 - Rp.500.000' },
        { from: 500000, to: 1000000, color: '#6FB8FF', name: 'Lebih dari Rp.500.000 - Rp.1.000.000' },
        { from: 1000000, to: 5000000, color: '#3FA0FF', name: 'Lebih dari Rp.1.000.000 - Rp.5.000.000' },
        { from: 5000000, to: 10000000, color: '#0077CC', name: 'Lebih dari Rp.5.000.000 - Rp.10.000.000' },
        { from: 10000000, to: Infinity, color: '#004080', name: '> Rp.10.000.000' }
    ];

    try {
        window.adsenseMapInstance && window.adsenseMapInstance.destroy && window.adsenseMapInstance.destroy();
    } catch (e) { console.warn('Failed to destroy previous map:', e); }
    window.adsenseMapInstance = null;

    try {
        window.adsenseMapInstance = Highcharts.mapChart('worldMapAdsense', {
            chart: {
                map: 'custom/world',
                backgroundColor: 'transparent',
                style: { fontFamily: 'Arial, sans-serif' }
            },
            title: {
                text: 'Pendapatan AdSense Per Negara',
                style: { fontSize: '16px', fontWeight: '600', color: '#333' }
            },
            subtitle: {
                text: 'Berdasarkan data traffic dan revenue',
                style: { fontSize: '12px', color: '#666' }
            },
            mapNavigation: {
                enabled: true,
                buttonOptions: {
                    verticalAlign: 'bottom',
                    theme: {
                        fill: 'white',
                        'stroke-width': 1,
                        stroke: 'silver',
                        r: 0,
                        states: {
                            hover: { fill: '#a4edba' },
                            select: { stroke: '#039', fill: '#a4edba' }
                        }
                    }
                }
            },
            colorAxis: {
                min: 0,
                minColor: '#E6F2FF',
                maxColor: '#004080',
                dataClasses: ranges.map(function(range){
                    return { from: range.from, to: range.to, color: range.color, name: range.name };
                })
            },
            legend: {
                title: { text: 'Tingkat Pendapatan', style: { color: '#333', fontSize: '12px' } },
                align: 'left',
                verticalAlign: 'bottom',
                floating: true,
                layout: 'vertical',
                valueDecimals: 0,
                backgroundColor: 'rgba(255,255,255,0.9)',
                symbolRadius: 0,
                symbolHeight: 14
            },
            series: [{
                name: 'Negara',
                data: mapData,
                joinBy: ['hc-key', 'hc-key'],
                nullColor: '#E6E7E8',
                tooltip: {
                    backgroundColor: 'rgba(0,0,0,0.85)',
                    style: { color: 'white' },
                    pointFormatter: function() {
                        var formattedValue = 'Rp ' + Math.round(this.value).toLocaleString('id-ID');
                        return '<b>' + this.name + '</b><br>' +
                               'Kode: ' + (this.code || '-') + '<br>' +
                               'Pendapatan: <b>' + formattedValue + '</b><br>';
                    },
                    nullFormat: '<b>{point.name}</b><br>Tidak ada data traffic'
                },
                borderColor: '#606060',
                borderWidth: 0.5,
                states: { hover: { color: '#FFD700' } },
                allAreas: true
            }],
            exporting: {
                enabled: true,
                buttons: {
                    contextButton: {
                        theme: {
                            fill: 'white',
                            'stroke-width': 1,
                            stroke: 'silver',
                            r: 0,
                            states: { hover: { fill: '#a4edba' } }
                        },
                        menuItems: ['viewFullscreen', 'separator', 'downloadPNG', 'downloadJPEG', 'downloadPDF', 'downloadSVG']
                    }
                }
            },
            credits: { enabled: false }
        });
    } catch (error) {
        console.error('[ERROR] Failed to create AdSense map:', error);
        alert('Error creating map: ' + error.message);
        $('#worldMapAdsense').html('<div style="text-align: center; padding: 50px; color: #666;">Error loading map: ' + error.message + '</div>');
    }
}

function formatNumber(num, decimals = 0) {
    if (num === null || num === undefined || isNaN(num)) return '0';
    return parseFloat(num).toLocaleString('id-ID', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

function formatCurrencyIDR(value) {
    if (value === null || value === undefined || isNaN(value)) return 'Rp 0';
    return 'Rp ' + Math.round(parseFloat(value)).toLocaleString('id-ID');
}

function showSuccessMessage(message) {
    // Create success message element
    var successDiv = $('<div class="alert alert-success alert-dismissible fade show" role="alert">' +
        '<i class="fas fa-check-circle"></i> ' + message +
        '<button type="button" class="close" data-dismiss="alert" aria-label="Close">' +
        '<span aria-hidden="true">&times;</span>' +
        '</button>' +
        '</div>');
    
    // Remove any existing success messages
    $('.alert-success').remove();
    
    // Add to top of content
    $('.content-wrapper .content-header').after(successDiv);
    
    // Auto hide after 3 seconds
    setTimeout(function() {
        successDiv.fadeOut();
    }, 3000);
}

// CSRF token handling is now done in the template
// This file no longer needs to handle CSRF token directly