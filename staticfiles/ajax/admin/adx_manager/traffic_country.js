$(document).ready(function() {
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

    // Inisialisasi Select2 untuk site filter
    $('#site_filter').select2({
        placeholder: '-- Pilih Site --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4',
        multiple: true
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

    // Event handler untuk tombol Load
    $('#btn_load_data').click(function() {
        load_adx_traffic_country_data();
    });

    // Event handler untuk site filter change
    $('#site_filter').change(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var country_filter = $("#country_filter").val();
        if(tanggal_dari && tanggal_sampai && country_filter) {
            load_adx_traffic_country_data();
        }    
    });

    // Load data situs untuk select2
    loadSitesList();
    function loadSitesList() {
        $.ajax({
            url: '/management/admin/adx_sites_list',
            type: 'GET',
            dataType: 'json',
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function(response) {
                if (response.status) {
                    var select_site = $("#site_filter").val();
                    select_site.empty();

                    $.each(response.data, function(index, site) {
                        select_site.append(new Option(site, site, false, false));
                    });
                    select_site.trigger('change');
                    
                    // Load data awal setelah site options dimuat
                    var today = new Date();
                    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
                    
                    load_adx_traffic_country_data();
                } 
            },
            error: function(xhr, status, error) {
                console.error('Error loading sites:', error);
                console.error('Status:', status);
                console.error('Response:', xhr.responseText);
            }
        });
    }

    // Event handler untuk country filter change
    $('#country_filter').change(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var site_filter = $("#site_filter").val();
        if(tanggal_dari && tanggal_sampai && site_filter) {
            load_adx_traffic_country_data();
        }    
    });

    // Load data negara untuk select2
    load_country_options();
    // Fungsi untuk memuat opsi negara ke select2
    function load_country_options() {
        $.ajax({
            url: '/management/admin/get_countries_adx',
            type: 'GET',
            dataType: 'json',
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function(response) {
                if(response.status) {
                    var select_country = $('#country_filter');
                    select_country.empty();
                    
                    $.each(response.countries, function(index, country) {
                        select_country.append(new Option(country.name, country.code, false, false));
                    });
                    
                    select_country.trigger('change');
                    
                    // Load data awal setelah country options dimuat
                    var today = new Date();
                    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
                    
                    load_adx_traffic_country_data();
                }
            },
            error: function(xhr, status, error) {
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
        var selectedSites = $('#site_filter').val();
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
        // Tampilkan overlay loading
        $('#overlay').show();
        // Destroy existing DataTable if exist
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().destroy();
        }
        // AJAX request
        $.ajax({
            url: '/management/admin/page_adx_traffic_country',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                selected_sites: siteFilter,
                selected_countries: countryFilter
            },
            headers: {
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function(response) {
                $('#overlay').hide();
                if (response && response.status) {
                    // Update summary boxes
                    updateSummaryBoxes(response.summary);
                    $('#summary_boxes').show();
                    // Initialize DataTable
                    initializeDataTable(response.data);
                    // Generate charts if data available
                    generateTrafficCountryCharts(response.data);
                    $('#charts_section').show();
                } else {
                    var errorMsg = response.error || 'Terjadi kesalahan yang tidak diketahui';
                    console.error('[DEBUG] Response error:', errorMsg);
                    alert('Error: ' + errorMsg);
                }
            },
            error: function(xhr, status, error) {
                console.error('[DEBUG] AJAX Error:', {
                    xhr: xhr,
                    status: status,
                    error: error
                });
                $('#overlay').hide();
                report_eror('Terjadi kesalahan saat memuat data: ' + error);
            }
        });
    }

    // Fungsi untuk update summary boxes
    function updateSummaryBoxes(summary) {
        if (!summary) return;
        $('#total_impressions').text(summary.total_impressions ? summary.total_impressions.toLocaleString('id-ID') : '0');
        $('#total_clicks').text(summary.total_clicks ? summary.total_clicks.toLocaleString('id-ID') : '0');
        $('#total_ctr').text(summary.total_ctr ? summary.total_ctr.toFixed(2) + '%' : '0%');
        $('#total_revenue').text(formatCurrencyIDR(summary.total_revenue || 0));
    }

    // Fungsi untuk inisialisasi DataTable
    function initializeDataTable(data) {
        var tableData = [];
        if (data && Array.isArray(data)) {
            data.forEach(function(row) {
                // Get country flag
                var countryFlag = '';
                if (row.country_code) {
                    countryFlag = '<img src="https://flagcdn.com/16x12/' + row.country_code.toLowerCase() + '.png" alt="' + row.country_code + '" style="margin-right: 5px;"> ';
                }
                tableData.push([
                    countryFlag + (row.country_name || ''),
                    row.country_code || '',
                    row.impressions ? row.impressions.toLocaleString('id-ID') : '0',
                    row.clicks ? row.clicks.toLocaleString('id-ID') : '0',
                    row.ctr ? row.ctr.toFixed(2) + '%' : '0%',
                    formatCurrencyIDR(row.cpc || 0),
                    formatCurrencyIDR(row.ecpm || 0),
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

    // Fungsi untuk generate charts
    function generateTrafficCountryCharts(data) {
        if (!data || data.length === 0) return;
        
        // Sort data by impressions and take top 10
        var sortedData = data.sort(function(a, b) {
            return (b.impressions || 0) - (a.impressions || 0);
        }).slice(0, 10);
        
        // Prepare data for charts
        var countries = sortedData.map(function(item) {
            return item.country_name || 'Unknown';
        });
        
        var impressions = sortedData.map(function(item) {
            return item.impressions || 0;
        });
        
        var clicks = sortedData.map(function(item) {
            return item.clicks || 0;
        });
        
        var revenue = sortedData.map(function(item) {
            return item.revenue || 0;
        });
        
        // Create charts if Chart.js is available
        if (typeof Chart !== 'undefined') {
            // Impressions Chart
            var ctx1 = document.getElementById('impressionsChart');
            if (ctx1) {
                new Chart(ctx1, {
                    type: 'bar',
                    data: {
                        labels: countries,
                        datasets: [{
                            label: 'Total Impresi',
                            data: impressions,
                            backgroundColor: 'rgba(54, 162, 235, 0.6)',
                            borderColor: 'rgba(54, 162, 235, 1)',
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                });
            }
            
            // Revenue Chart
            var ctx2 = document.getElementById('revenueChart');
            if (ctx2) {
                new Chart(ctx2, {
                    type: 'doughnut',
                    data: {
                        labels: countries,
                        datasets: [{
                            label: 'Total Pendapatan',
                            data: revenue,
                            backgroundColor: [
                                '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
                                '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#FF6384',
                                '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40'
                            ]
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: true,
                        aspectRatio: 2
                    }
                });
            }
        }
    }

    // Fungsi untuk report error (jika ada)
    function report_eror(message) {
        console.error('Error:', message);
        alert(message);
    }
});