$(document).ready(function () {
    // Inisialisasi datepicker
    $('.datepicker-input').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    });
    // Set tanggal default (7 hari terakhir)
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    $('#tanggal_dari').val(formatDateForInput(lastWeek));
    $('#tanggal_sampai').val(formatDateForInput(today));
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
    // Inisialisasi Select2 untuk country filter
    $('#country_filter').select2({
        placeholder: '-- Pilih Negara --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4',
        multiple: true
    });
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    // Event handler untuk tombol Load
    $('#btn_load_data').click(function () {
        var selected_account_adx = $("#account_filter").val();
        $('#overlay').show();
        loadSitesList(selected_account_adx);
        load_country_options(selected_account_adx);
        load_adx_traffic_country_data();
    });
    // Load data situs untuk select2
    function loadSitesList(selected_account_adx) {
        var selectedAccounts = selected_account_adx;
        // Simpan pilihan domain yang sudah dipilih sebelumnya
        var previouslySelected = $("#site_filter").val() || [];
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
                    
                    // Tambahkan opsi baru dan pertahankan pilihan sebelumnya jika masih tersedia
                    var validPreviousSelections = [];
                    $.each(response.data, function (index, site) {
                        var isSelected = previouslySelected.includes(site);
                        if (isSelected) {
                            validPreviousSelections.push(site);
                        }
                        select_site.append(new Option(site, site, false, isSelected));
                    });
                    
                    // Set nilai yang dipilih kembali
                    if (validPreviousSelections.length > 0) {
                        select_site.val(validPreviousSelections);
                    }
                    
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
    // Load data saat halaman pertama kali dibuka
    function load_country_options(selected_account_adx) {
        var selectedAccounts = selected_account_adx;
        // Simpan pilihan negara yang sudah dipilih sebelumnya
        var previouslySelected = $('#country_filter').val() || [];
        
        $.ajax({
            url: '/management/admin/get_countries_adx',
            type: 'GET',
            dataType: 'json',
            data: {
                'selected_accounts': selectedAccounts
            },
            success: function (response) {
                if (response.status) {
                    var select_country = $('#country_filter');
                    select_country.empty();
                    
                    // Tambahkan opsi baru dan pertahankan pilihan sebelumnya jika masih tersedia
                    var validPreviousSelections = [];
                    $.each(response.countries, function (index, country) {
                        var isSelected = previouslySelected.includes(country.code);
                        if (isSelected) {
                            validPreviousSelections.push(country.code);
                        }
                        select_country.append(new Option(country.name, country.code, false, isSelected));
                    });
                    
                    // Set nilai yang dipilih kembali
                    if (validPreviousSelections.length > 0) {
                        select_country.val(validPreviousSelections);
                    }
                    
                    select_country.trigger('change');
                }
            },
            error: function (xhr, status, error) {
                console.error('Error loading countries:', error);
                console.error('Status:', status);
                console.error('Response:', xhr.responseText);
            }
        });
    }
    // Fungsi untuk format tanggal ke format input (YYYY-MM-DD)
    function formatDateForInput(date) {
        var year = date.getFullYear();
        var month = String(date.getMonth() + 1).padStart(2, '0');
        var day = String(date.getDate()).padStart(2, '0');
        return year + '-' + month + '-' + day;
    }
    // Fungsi untuk format mata uang IDR
    function formatCurrencyIDR(value) {
        // Handle null, undefined, or non-numeric values
        if (value === null || value === undefined || value === '') {
            return 'Rp 0';
        }
        
        // Handle set objects or other complex objects
        if (typeof value === 'object' && value !== null) {
            // If it's a set-like object, try to get the first value or return 0
            if (value.constructor && value.constructor.name === 'Set') {
                return 'Rp 0';
            }
            // For other objects, try to convert to string first
            value = String(value);
        }
        
        // Convert to string and remove currency symbols and commas
        let stringValue = String(value);
        let numValue = parseFloat(stringValue.replace(/[$,]/g, ''));
        
        if (isNaN(numValue)) return 'Rp 0';
        
        // Round to remove decimals and format with Indonesian number format
        return 'Rp ' + Math.round(numValue).toLocaleString('id-ID');
    }
    // Fungsi untuk load data traffic per country
    function load_adx_traffic_country_data() {
        var startDate = $('#tanggal_dari').val();
        var endDate = $('#tanggal_sampai').val();
        var selectedAccountAdx = $('#account_filter').val();
        var selectedSites = $('#site_filter').val();
        var selectedAccount = $('#select_account').val();
        var selectedCountries = $('#country_filter').val();
        if (!startDate || !endDate) {
            alert('Silakan pilih rentang tanggal');
            return;
        }
        // Convert array to comma-separated string for backend
        var siteFilter = '';
        if (selectedSites && selectedSites.length > 0) {
            siteFilter = selectedSites.join(',');
        }
        // Convert array to comma-separated string for backend
        var countryFilter = '';
        if (selectedCountries && selectedCountries.length > 0) {
            countryFilter = selectedCountries.join(',');
        }
        // Destroy existing DataTable if exists
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().destroy();
        }
        // AJAX request
        $.ajax({
            url: '/management/admin/page_roi_traffic_country',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                selected_account_adx: selectedAccountAdx,
                selected_sites: siteFilter,
                selected_account: selectedAccount,
                selected_countries: countryFilter
            },
            headers: {
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function (response) {
                if (response && response.status) {
                    // Update summary boxes
                    updateSummaryBoxes(response.data);
                    $('#summary_boxes').show();
                    // Initialize DataTable
                    initializeDataTable(response.data);
                    // Generate charts if data available
                    generateTrafficCountryCharts(response.data);
                } else {
                    var errorMsg = response.error || 'Terjadi kesalahan yang tidak diketahui';
                    console.error('[DEBUG] Response error:', errorMsg);
                    alert('Error: ' + errorMsg);
                }
                $('#overlay').hide();
            },
            error: function (xhr, status, error) {
                console.error('[DEBUG] AJAX Error:', {
                    xhr: xhr,
                    status: status,
                    error: error
                });
                report_eror('Terjadi kesalahan saat memuat data: ' + error);
                $('#overlay').hide();
            }
        });
    }
    // Fungsi untuk update summary boxes
    function updateSummaryBoxes(data) {
        if (!data || !Array.isArray(data)) return;
        // Hitung summary dari data
        var totalImpressions = 0;
        var totalSpend = 0;
        var totalClicks = 0;
        var totalRevenue = 0;
        var totalROI = 0;
        var validROICount = 0;
        data.forEach(function (item) {
            totalImpressions += item.impressions || 0;
            totalSpend += item.spend || 0;
            totalClicks += item.clicks || 0;
            totalRevenue += item.revenue || 0;

            if (item.roi && item.roi !== 0) {
                totalROI += item.roi;
                validROICount++;
            }
        });
        var averageCTR = totalImpressions > 0 ? (totalClicks / totalImpressions * 100) : 0;
        var averageROI = validROICount > 0 ? (totalROI / validROICount) : 0;
        $('#total_impressions').text(totalImpressions.toLocaleString('id-ID'));
        $('#total_spend').text(formatCurrencyIDR(totalSpend));
        $('#total_clicks').text(totalClicks.toLocaleString('id-ID'));
        $('#total_ctr').text(averageCTR.toFixed(2) + '%');
        $('#average_roi').text(averageROI.toFixed(2) + '%');
        $('#total_revenue').text(formatCurrencyIDR(totalRevenue));
    }
    // Fungsi untuk inisialisasi DataTable
    function initializeDataTable(data) {
        var tableData = [];
        if (data && Array.isArray(data)) {
            data.forEach(function (row) {
                // Get country flag
                var countryFlag = '';
                if (row.country_code) {
                    countryFlag = '<img src="https://flagcdn.com/16x12/' + row.country_code.toLowerCase() + '.png" alt="' + row.country_code + '" style="margin-right: 5px;"> ';
                }
                tableData.push([
                    countryFlag + (row.country || ''),
                    row.country_code || '',
                    row.impressions ? row.impressions.toLocaleString('id-ID') : '0',
                    formatCurrencyIDR(row.spend || 0),
                    row.clicks ? row.clicks.toLocaleString('id-ID') : '0',
                    row.ctr ? row.ctr.toFixed(2) + '%' : '0%',
                    formatCurrencyIDR(row.cpc || 0),
                    formatCurrencyIDR(row.ecpm || 0),
                    row.roi ? row.roi.toFixed(2) + '%' : '0%',
                    formatCurrencyIDR(row.revenue || 0)
                ]);
            });
        }
        // Destroy existing DataTable if it exists
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().destroy();
        }
        $('#table_traffic_country').DataTable({
            data: tableData,
            responsive: true,
            pageLength: 25,
            lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "Semua"]],
            language: {
                "decimal": ",",
                "thousands": ".",
                "info": "Menampilkan _START_ sampai _END_ dari _TOTAL_ entri",
                "infoEmpty": "Menampilkan 0 sampai 0 dari 0 entri",
                "infoFiltered": "(disaring dari _MAX_ total entri)",
                "lengthMenu": "Tampilkan _MENU_ entri",
                "loadingRecords": "Memuat...",
                "processing": "Memproses...",
                "search": "Cari:",
                "zeroRecords": "Tidak ada data yang cocok",
                "paginate": {
                    "first": "Pertama",
                    "last": "Terakhir",
                    "next": "Selanjutnya",
                    "previous": "Sebelumnya"
                }
            },
            dom: 'Bfrtip',
            buttons: [
                {
                    extend: 'excel',
                    text: 'Export Excel',
                    className: 'btn btn-success'
                },
                {
                    extend: 'pdf',
                    text: 'Export PDF',
                    className: 'btn btn-danger'
                }
            ],
            order: [[2, 'desc']] // Sort by impressions descending
        });
    }
    // Fungsi untuk generate charts (hanya world map)
    function generateTrafficCountryCharts(data) {
        // Bersihkan chart jika data kosong dan sembunyikan section chart
        if (!data || data.length === 0) {
            $('#charts_section').hide();
            return;
        }
        
        // Tampilkan section charts dan buat world map
        $('#charts_section').show();
        createWorldMap(data);
    }
    
    // Fungsi untuk membuat world map dengan Highcharts Maps (sama seperti ADX Traffic Country)
    function createWorldMap(data) {
        console.log('[DEBUG] createWorldMap called with data length:', data ? data.length : 0);
        
        // Jika tidak ada data, pastikan charts dibersihkan dan section disembunyikan
        if (!data || data.length === 0) {
            if (window.worldMapInstance) {
                try { window.worldMapInstance.destroy(); } catch (e) { console.warn('Failed to destroy world map:', e); }
                window.worldMapInstance = null;
            }
            $('#charts_section').hide();
            return;
        }

        // Prepare data for Highcharts Maps
        var mapData = [];
        var maxROI = 0;
        var minROI = Infinity;
        
        // Country code mapping (ISO 2-letter codes) - sama seperti ADX
        var countryCodeMap = {
            'Indonesia': 'id',
            'United States': 'us',
            'United Kingdom': 'gb',
            'Germany': 'de',
            'France': 'fr',
            'Japan': 'jp',
            'China': 'cn',
            'India': 'in',
            'Brazil': 'br',
            'Australia': 'au',
            'Canada': 'ca',
            'Italy': 'it',
            'Spain': 'es',
            'Netherlands': 'nl',
            'South Korea': 'kr',
            'Mexico': 'mx',
            'Russia': 'ru',
            'Turkey': 'tr',
            'Saudi Arabia': 'sa',
            'South Africa': 'za',
            'Argentina': 'ar',
            'Thailand': 'th',
            'Malaysia': 'my',
            'Singapore': 'sg',
            'Philippines': 'ph',
            'Vietnam': 'vn',
            'Egypt': 'eg',
            'Nigeria': 'ng',
            'Kenya': 'ke',
            'Morocco': 'ma',
            'Chile': 'cl',
            'Peru': 'pe',
            'Colombia': 'co',
            'Venezuela': 've',
            'Ecuador': 'ec',
            'Uruguay': 'uy',
            'Paraguay': 'py',
            'Bolivia': 'bo',
            'Guyana': 'gy',
            'Suriname': 'sr',
            'French Guiana': 'gf'
        };
        
        // Process data and find max ROI for color scaling
        data.forEach(function(item) {
            var roiValue = parseFloat(item.roi) || 0;
            if (roiValue > 0) {
                if (roiValue > maxROI) {
                    maxROI = roiValue;
                }
                if (roiValue < minROI) {
                    minROI = roiValue;
                }
                
                // Map country codes to Highcharts format
                var countryName = item.country || 'Unknown';
                var countryCode = countryCodeMap[countryName] || countryName.toLowerCase().substring(0, 2);
                
                mapData.push({
                    'hc-key': countryCode,
                    code: countryCode.toUpperCase(),
                    name: countryName,
                    value: roiValue,
                    impressions: item.impressions || 0,
                    clicks: item.clicks || 0,
                    spend: item.spend || 0,
                    revenue: item.revenue || 0
                });
            }
        });

        // Create fixed color ranges with more vibrant colors and informative labels (sama seperti ADX)
        var ranges = [
            { from: null, to: null, color: '#E6E7E8', name: 'Tidak ada data' },
            { from: 0, to: 50, color: '#FFF2CC', name: '0% - 50%' },
            { from: 50, to: 100, color: '#FFE066', name: 'Lebih dari 50% - 100%' },
            { from: 100, to: 200, color: '#FFCC02', name: 'Lebih dari 100% - 200%' },
            { from: 200, to: 500, color: '#FF9500', name: 'Lebih dari 200% - 500%' },
            { from: 500, to: 1000, color: '#FF6B35', name: 'Lebih dari 500% - 1000%' },
            { from: 1000, to: 2000, color: '#E63946', name: 'Lebih dari 1000% - 2000%' },
            { from: 2000, to: Infinity, color: '#A4161A', name: '> 2000%' }
        ];

        // Destroy existing chart if any
        if (window.worldMapInstance) {
            try { window.worldMapInstance.destroy(); } catch (e) { console.warn('Failed to destroy world map:', e); }
            window.worldMapInstance = null;
        }

        // Create Highcharts Map
        try {
            // Check if Highcharts is available
            if (typeof Highcharts === 'undefined' || !Highcharts.mapChart) {
                throw new Error('Highcharts Maps library not loaded');
            }
            
            // Ensure map container is visible and has proper dimensions
            $('#worldMap').css({
                'height': '500px',
                'width': '100%',
                'display': 'block',
                'visibility': 'visible'
            });
            
            // Check if we have data to display
            if (mapData.length === 0) {
                $('#worldMap').html('<div style="text-align: center; padding: 100px; color: #666; font-size: 16px;">Tidak ada data untuk ditampilkan.<br>Silakan pilih tanggal dan akun, lalu klik Load Data.</div>');
                return;
            }
            
            window.worldMapInstance = Highcharts.mapChart('worldMap', {
                chart: {
                    map: 'custom/world',
                    backgroundColor: 'transparent',
                    style: {
                        fontFamily: 'Arial, sans-serif'
                    }
                },
                title: {
                    text: 'ROI Per Negara',
                    style: {
                        fontSize: '16px',
                        fontWeight: '600',
                        color: '#333'
                    }
                },
                subtitle: {
                    text: 'Berdasarkan data traffic dan revenue',
                    style: {
                        fontSize: '12px',
                        color: '#666'
                    }
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
                                hover: {
                                    fill: '#a4edba'
                                },
                                select: {
                                    stroke: '#039',
                                    fill: '#a4edba'
                                }
                            }
                        }
                    }
                },
                colorAxis: {
                    min: 0,
                    minColor: '#FFF2CC', // Warna kuning muda untuk ROI terendah
                    maxColor: '#A4161A', // Warna merah tua untuk ROI tertinggi
                    dataClasses: ranges.map(function(range) {
                        return {
                            from: range.from,
                            to: range.to,
                            color: range.color,
                            name: range.name
                        };
                    })
                },
                legend: {
                    title: {
                        text: 'Tingkat ROI',
                        style: {
                            color: '#333',
                            fontSize: '12px'
                        }
                    },
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
                    nullColor: '#E6E7E8', // Warna abu-abu untuk negara tanpa data
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.85)',
                        style: {
                            color: 'white'
                        },
                        pointFormat: '<b>{point.name}</b><br>' +
                                    'Kode: {point.code}<br>' +
                                    'ROI: <b>{point.value:.2f}%</b><br>',
                        pointFormatter: function() {
                            var formattedValue = this.value.toFixed(2) + '%';
                            return '<b>' + this.name + '</b><br>' +
                                   'Kode: ' + this.code + '<br>' +
                                   'ROI: <b>' + formattedValue + '</b><br>' +
                                   'Impressions: <b>' + this.impressions.toLocaleString('id-ID') + '</b><br>' +
                                   'Clicks: <b>' + this.clicks.toLocaleString('id-ID') + '</b><br>' +
                                   'Spend: <b>Rp ' + Math.round(this.spend).toLocaleString('id-ID') + '</b><br>' +
                                   'Revenue: <b>Rp ' + Math.round(this.revenue).toLocaleString('id-ID') + '</b><br>';
                        },
                        nullFormat: '<b>{point.name}</b><br>Tidak ada data ROI'
                    },
                    borderColor: '#606060',
                    borderWidth: 0.5,
                    states: {
                        hover: {
                            color: '#a4edba'
                        }
                    }
                }]
            });
            
            console.log('World map created successfully with', mapData.length, 'countries');
            
        } catch (error) {
            console.error('Error creating world map:', error);
            $('#worldMap').html('<div class="text-center p-4"><h5 class="text-danger">Error loading map: ' + error.message + '</h5></div>');
        }
    }
    
    // Fungsi untuk report error (jika ada)
    function report_eror(message) {
        console.error('Error:', message);
        alert(message);
    }
});