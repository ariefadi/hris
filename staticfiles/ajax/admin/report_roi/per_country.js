$(document).ready(function() {
    // Global chart variables untuk mengelola instance chart
    var impressionsChartInstance = null;
    var revenueChartInstance = null;
    var roiChartInstance = null;
    
    // Inisialisasi datepicker
    $('.datepicker-input').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    });

    // Inisialisasi Select2 untuk country filter
    $('#country_filter').select2({
        placeholder: '-- Pilih Negara --',
        allowClear: true,
        width: '100%',
        theme: 'bootstrap4',
        multiple: true
    });

    // Load daftar negara untuk Select2
    loadCountriesForSelect2();

    // Set tanggal default (7 hari terakhir)
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    
    $('#tanggal_dari').val(formatDateForInput(lastWeek));
    $('#tanggal_sampai').val(formatDateForInput(today));

    // Event handler untuk tombol Load
    $('#btn_load_data').click(function() {
        load_adx_traffic_country_data();
    });

    // Event handler untuk perubahan country filter
    $('#country_filter').on('change', function() {
        // Auto-reload data ketika filter negara berubah
        var startDate = $('#tanggal_dari').val();
        var endDate = $('#tanggal_sampai').val();
        if (startDate && endDate) {
            load_adx_traffic_country_data();
        }
    });

    // Load data saat halaman pertama kali dibuka dengan delay untuk memastikan tanggal sudah ter-set
    setTimeout(function() {
        load_adx_traffic_country_data();
    }, 100);

    // Fungsi untuk format tanggal ke format input (YYYY-MM-DD)
    function formatDateForInput(date) {
        var year = date.getFullYear();
        var month = String(date.getMonth() + 1).padStart(2, '0');
        var day = String(date.getDate()).padStart(2, '0');
        return year + '-' + month + '-' + day;
    }

    // Fungsi untuk format mata uang Rupiah
    function formatRupiah(value) {
        // Convert to number, round to remove decimals, then format with Rp
        let numValue = parseFloat(value.toString().replace(/[$,]/g, ''));
        if (isNaN(numValue)) return value;
        
        // Round to remove decimals and format with Indonesian number format
        return 'Rp ' + Math.round(numValue).toLocaleString('id-ID');
    }

    // Fungsi untuk load data traffic per country
    function load_adx_traffic_country_data() {
        var startDate = $('#tanggal_dari').val();
        var endDate = $('#tanggal_sampai').val();
        var selectedCountries = $('#country_filter').val(); // Array of selected countries

        if (!startDate || !endDate) {
            alert('Silakan pilih rentang tanggal');
            return;
        }

        console.log('[DEBUG] Loading country data with params:', {
            start_date: startDate,
            end_date: endDate,
            selected_countries: selectedCountries
        });

        // Tampilkan overlay loading
        $('#overlay').show();
        $('#summary_boxes').hide();

        // Destroy existing DataTable if exists
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().clear().destroy();
            $('#table_traffic_country').empty();
        }

        // AJAX request
        $.ajax({
            url: '/management/admin/page_roi_traffic_country',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                selected_countries: selectedCountries ? selectedCountries.join(',') : ''
            },
            headers: {
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function(response) {
                console.log('[DEBUG] AJAX Success Response:', response);
                $('#overlay').hide();
                
                if (response && response.status) {
                    // Update summary boxes using data array instead of response.summary
                    updateSummaryBoxes(response.data);

                    // Initialize DataTable
                    initializeDataTable(response.data);
                    
                    // Generate charts if data available
                    if (response.data && response.data.length > 0) {
                        generateTrafficCountryCharts(response.data);
                        $('#charts_section').show();
                    } else {
                        $('#charts_section').hide();
                    }
                    
                    $('#summary_boxes').show();
                } else {
                    console.error('[ERROR] Invalid response format:', response);
                    alert('Error: Invalid response format');
                }
            },
            error: function(xhr, status, error) {
                console.error('[ERROR] AJAX Error:', error);
                console.error('[ERROR] Status:', status);
                console.error('[ERROR] Response:', xhr.responseText);
                $('#overlay').hide();
                alert('Error loading data: ' + error);
            }
        });
    }

    // Fungsi untuk update summary boxes
    function updateSummaryBoxes(data) {
        if (!data || !Array.isArray(data)) {
            // Reset to zero if no data
            $('#total_impressions').text('0');
            $('#total_spend').text('Rp 0');
            $('#total_clicks').text('0');
            $('#total_ctr').text('0.00%');
            $('#average_roi').text('0.00%');
            $('#total_revenue').text('Rp 0');
            return;
        }
        
        // Hitung summary dari data
        var totalImpressions = 0;
        var totalSpend = 0;
        var totalClicks = 0;
        var totalRevenue = 0;
        var totalROI = 0;
        var validROICount = 0;
        
        data.forEach(function(item) {
            totalImpressions += item.impressions || 0;
            totalSpend += item.spend || 0;
            totalClicks += item.clicks || 0;
            totalRevenue += item.revenue || 0;
            
            if (item.roi !== undefined && item.roi !== null) {
                totalROI += item.roi;
                validROICount++;
            }
        });
        
        var averageCTR = totalImpressions > 0 ? (totalClicks / totalImpressions * 100) : 0;
        var averageROI = validROICount > 0 ? (totalROI / validROICount) : 0;
        
        $('#total_impressions').text(totalImpressions.toLocaleString('id-ID'));
        $('#total_spend').text(formatRupiah(totalSpend));
        $('#total_clicks').text(totalClicks.toLocaleString('id-ID'));
        $('#total_ctr').text(averageCTR.toFixed(2) + '%');
        $('#average_roi').text(averageROI.toFixed(2) + '%');
        $('#total_revenue').text(formatRupiah(totalRevenue));
    }

    // Fungsi untuk inisialisasi DataTable
    function initializeDataTable(data) {
        // Destroy existing DataTable if exists (double check)
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().clear().destroy();
            $('#table_traffic_country').empty(); // Clear HTML content
        }
        
        // Add small delay to ensure DOM is ready
        setTimeout(function() {
            $('#table_traffic_country').DataTable({
            data: data,
            columns: [
                { 
                    data: 'country',
                    title: 'Negara'
                },
                { 
                    data: 'impressions',
                    title: 'Impressions',
                    render: function(data) {
                        return data ? data.toLocaleString() : '0';
                    }
                },
                { 
                    data: 'clicks',
                    title: 'Klik',
                    render: function(data) {
                        return data ? data.toLocaleString() : '0';
                    }
                },
                { 
                    data: 'ctr',
                    title: 'CTR (%)',
                    render: function(data) {
                        return data ? data.toFixed(2) + '%' : '0.00%';
                    }
                },
                { 
                    data: 'cpc',
                    title: 'CPC',
                    render: function(data) {
                        return data ? formatRupiah(data) : 'Rp 0';
                    }
                },
                { 
                    data: 'ecpm',
                    title: 'eCPM',
                    render: function(data) {
                        return data ? formatRupiah(data) : 'Rp 0';
                    }
                },
                { 
                    data: 'revenue',
                    title: 'Pendapatan',
                    render: function(data) {
                        return data ? formatRupiah(data) : 'Rp 0';
                    }
                }
            ],
            responsive: true,
            lengthChange: false,
            autoWidth: false,
            buttons: ["copy", "csv", "excel", "pdf", "print", "colvis"],
            order: [[1, 'desc']], // Order by impressions descending
            pageLength: 25,
            language: {
                url: '//cdn.datatables.net/plug-ins/1.10.24/i18n/Indonesian.json'
            }
        }).buttons().container().appendTo('#table_traffic_country_wrapper .col-md-6:eq(0)');
        }, 100); // End setTimeout
    }

    // Fungsi untuk generate charts
    function generateTrafficCountryCharts(data) {
        // Destroy existing charts
        if (impressionsChartInstance) {
            impressionsChartInstance.destroy();
        }
        if (revenueChartInstance) {
            revenueChartInstance.destroy();
        }
        if (roiChartInstance) {
            roiChartInstance.destroy();
        }

        // Sort data by impressions for better visualization
        var sortedData = data.slice().sort((a, b) => b.impressions - a.impressions);
        
        // Take top 10 countries for charts
        var topCountries = sortedData.slice(0, 10);
        
        var countries = topCountries.map(item => item.country);
        var impressions = topCountries.map(item => item.impressions);
        var revenues = topCountries.map(item => item.revenue);

        // Impressions Chart
        var impressionsCtx = document.getElementById('impressionsChart').getContext('2d');
        impressionsChartInstance = new Chart(impressionsCtx, {
            type: 'bar',
            data: {
                labels: countries,
                datasets: [{
                    label: 'Impressions',
                    data: impressions,
                    backgroundColor: 'rgba(54, 162, 235, 0.8)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return value.toLocaleString();
                            }
                        }
                    }
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': ' + context.parsed.y.toLocaleString();
                            }
                        }
                    }
                }
            }
        });

        // Revenue Chart
        var revenueCtx = document.getElementById('revenueChart').getContext('2d');
        revenueChartInstance = new Chart(revenueCtx, {
            type: 'horizontalBar',
            data: {
                labels: countries,
                datasets: [{
                    label: 'Pendapatan',
                    data: revenues,
                    backgroundColor: 'rgba(255, 99, 132, 0.8)',
                    borderColor: 'rgba(255, 99, 132, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return formatRupiah(value);
                            }
                        }
                    }
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': ' + formatRupiah(context.parsed.x);
                            }
                        }
                    }
                }
            }
        });
    }

    // Fungsi untuk load daftar negara ke Select2
    function loadCountriesForSelect2() {
        // Tampilkan loading state
        $('#country_filter').html('<option value="">Loading countries...</option>');
        
        $.ajax({
            url: '/management/admin/get_countries',
            type: 'GET',
            dataType: 'json',
            success: function(response) {
                console.log('[DEBUG] Countries loaded:', response);
                if (response && response.status && response.countries) {
                    var options = '<option value="">Select Country (All if none selected)</option>';
                    response.countries.forEach(function(country) {
                        options += '<option value="' + country.name + '">' + country.name + ' (' + country.code + ')</option>';
                    });
                    $('#country_filter').html(options);
                    
                    // Tampilkan pesan sukses
                    console.log('[INFO] Successfully loaded ' + response.countries.length + ' countries from AdX data');
                } else {
                    console.warn('[WARNING] Invalid response format, using fallback countries');
                    loadFallbackCountries();
                }
            },
            error: function(xhr, status, error) {
                console.error('[ERROR] Failed to load countries from server:', error);
                console.error('[ERROR] Status:', status, 'XHR:', xhr);
                loadFallbackCountries();
            }
        });
    }
    
    function loadFallbackCountries() {
        console.log('[INFO] Loading fallback countries list');
        // Fallback dengan daftar negara umum
        var commonCountries = [
            {name: 'Indonesia', code: 'ID'},
            {name: 'United States', code: 'US'},
            {name: 'Malaysia', code: 'MY'},
            {name: 'Singapore', code: 'SG'},
            {name: 'Thailand', code: 'TH'},
            {name: 'Philippines', code: 'PH'},
            {name: 'Vietnam', code: 'VN'},
            {name: 'India', code: 'IN'},
            {name: 'Australia', code: 'AU'},
            {name: 'Japan', code: 'JP'},
            {name: 'South Korea', code: 'KR'},
            {name: 'China', code: 'CN'},
            {name: 'Hong Kong', code: 'HK'},
            {name: 'Taiwan', code: 'TW'},
            {name: 'United Kingdom', code: 'GB'},
            {name: 'Germany', code: 'DE'},
            {name: 'France', code: 'FR'},
            {name: 'Italy', code: 'IT'},
            {name: 'Spain', code: 'ES'},
            {name: 'Netherlands', code: 'NL'},
            {name: 'Canada', code: 'CA'},
            {name: 'Brazil', code: 'BR'}
        ];
        
        var options = '<option value="">Select Country (All if none selected)</option>';
        commonCountries.forEach(function(country) {
            options += '<option value="' + country.name + '">' + country.name + ' (' + country.code + ')</option>';
        });
        $('#country_filter').html(options);
        console.log('[INFO] Fallback countries loaded successfully');
    }
});