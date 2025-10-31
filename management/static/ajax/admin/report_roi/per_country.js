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
                    $('#charts_section').show();
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
    // Fungsi untuk generate charts
    function generateTrafficCountryCharts(data) {
        // Bersihkan chart jika data kosong dan sembunyikan section chart
        if (!data || data.length === 0) {
            if (window.roiChartInstance) {
                try { window.roiChartInstance.destroy(); } catch (e) { console.warn('Failed to destroy ROI chart:', e); }
                window.roiChartInstance = null;
            }
            $('#charts_section').hide();
            return;
        }
        // Sort data by ROI and take top 10
        var sortedData = data.sort(function (a, b) {
            return (b.roi || 0) - (a.roi || 0);
        }).slice(0, 10);
        // Prepare data for charts
        var countries = sortedData.map(function (item) {
            return item.country || 'Unknown';
        });
        var roi = sortedData.map(function (item) {
            return item.roi || 0;
        });
        // Create charts if Chart.js is available
        if (typeof Chart !== 'undefined') {
            // ROI Chart
            var ctx = document.getElementById('roiChart');
            if (ctx) {
                // Hancurkan chart sebelumnya jika ada untuk mencegah error canvas in use
                if (window.roiChartInstance) {
                    try { window.roiChartInstance.destroy(); } catch (e) { console.warn('Failed to destroy ROI chart:', e); }
                    window.roiChartInstance = null;
                }
                window.roiChartInstance = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: countries,
                        datasets: [{
                            label: 'ROI (%)',
                            data: roi,
                            backgroundColor: [
                                'rgba(255, 99, 132, 0.8)',
                                'rgba(54, 162, 235, 0.8)',
                                'rgba(255, 205, 86, 0.8)',
                                'rgba(75, 192, 192, 0.8)',
                                'rgba(153, 102, 255, 0.8)',
                                'rgba(255, 159, 64, 0.8)',
                                'rgba(199, 199, 199, 0.8)',
                                'rgba(83, 102, 255, 0.8)',
                                'rgba(255, 99, 255, 0.8)',
                                'rgba(99, 255, 132, 0.8)'
                            ],
                            borderColor: [
                                'rgba(255, 99, 132, 1)',
                                'rgba(54, 162, 235, 1)',
                                'rgba(255, 205, 86, 1)',
                                'rgba(75, 192, 192, 1)',
                                'rgba(153, 102, 255, 1)',
                                'rgba(255, 159, 64, 1)',
                                'rgba(199, 199, 199, 1)',
                                'rgba(83, 102, 255, 1)',
                                'rgba(255, 99, 255, 1)',
                                'rgba(99, 255, 132, 1)'
                            ],
                            borderWidth: 1
                        }]
                    },
                    options: {
                        indexAxis: 'y',
                        responsive: true,
                        scales: {
                            x: {
                                beginAtZero: true,
                                ticks: {
                                    callback: function (value) {
                                        return value + '%';
                                    }
                                }
                            }
                        },
                        plugins: {
                            tooltip: {
                                callbacks: {
                                    label: function (context) {
                                        return context.dataset.label + ': ' + context.parsed.x + '%';
                                    }
                                }
                            }
                        }
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