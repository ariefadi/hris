$(document).ready(function () {
    // Simpan data terakhir untuk re-render cepat saat toggle berubah
    window.lastRoiData = null;
    // Pulihkan preferensi toggle dari localStorage (default: off)
    var savedHideZero = localStorage.getItem('roi_hide_zero_spend');
    if (savedHideZero !== null) {
        $('#toggle_hide_zero_spend').prop('checked', savedHideZero === '1');
    }
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
                    // Simpan data mentah untuk kebutuhan toggle
                    window.lastRoiData = Array.isArray(response.data) ? response.data : [];
                    var displayData = applyZeroSpendFilter(window.lastRoiData);
                    // Sinkronkan opsi Filter Negara berdasarkan data ROI yang tampil di tabel
                    updateCountryOptionsFromRoi(displayData);
                    // Update summary boxes
                    updateSummaryBoxes(displayData);
                    $('#summary_boxes').show();
                    // Initialize DataTable
                    initializeDataTable(displayData);
                    // Generate charts if data available
                    generateTrafficCountryCharts(displayData);
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
    // Terapkan filter berdasar toggle hide zero spend
    function applyZeroSpendFilter(data) {
        var hideZero = $('#toggle_hide_zero_spend').is(':checked');
        if (!hideZero) return data || [];
        return (data || []).filter(function (item) {
            var spendVal = parseFloat(item.spend || 0);
            return spendVal > 0;
        });
    }
    // Perbarui opsi Select2 negara supaya hanya menampilkan negara yang ada di tabel
    function updateCountryOptionsFromRoi(data) {
        var select_country = $('#country_filter');
        var previouslySelected = select_country.val() || [];
        var countriesMap = {};
        // Kumpulkan daftar negara unik dari data ROI yang tampil
        if (Array.isArray(data)) {
            data.forEach(function (item) {
                var code = (item.country_code || '').toUpperCase();
                var name = item.country || '';
                if (code && name) {
                    countriesMap[code] = name;
                }
            });
        }
        // Render ulang opsi negara
        select_country.empty();
        var validSelections = [];
        Object.keys(countriesMap).sort().forEach(function (code) {
            var name = countriesMap[code];
            var isSelected = previouslySelected.includes(code);
            if (isSelected) {
                validSelections.push(code);
            }
            // Tampilkan label "Nama Negara (CODE)" agar jelas
            select_country.append(new Option(name + ' (' + code + ')', code, false, isSelected));
        });
        // Pertahankan pilihan sebelumnya yang masih valid
        if (validSelections.length > 0) {
            select_country.val(validSelections);
        }
        select_country.trigger('change');
    }
    // Fungsi untuk update summary boxes
    function updateSummaryBoxes(data) {
        if (!data || !Array.isArray(data)) return;
        // Hitung summary dari data
        var totalImpressions = 0;
        var totalSpend = 0;
        var totalClicksFb = 0;
        var totalClicksAdx = 0;
        var totalCPR = 0;
        var totalRevenue = 0;
        var totalROI = 0;
        var validROICount = 0;
        data.forEach(function (item) {
            totalImpressions += item.impressions || 0;
            totalSpend += item.spend || 0;
            totalClicksFb += item.clicks_fb || 0;
            totalClicksAdx += item.clicks_adx || 0;
            totalCPR += item.cpr || 0;
            totalRevenue += item.revenue || 0;
            if (item.roi && item.roi !== 0) {
                totalROI += item.roi;
                validROICount++;
            }
        });
        var averageCTRFb = totalImpressions > 0 ? (totalClicksFb / totalImpressions * 100) : 0;
        var averageCTRAdx = totalImpressions > 0 ? (totalClicksAdx / totalImpressions * 100) : 0;
        var totalROI = (((totalRevenue - totalSpend) / totalSpend) * 100);
        console.log(totalROI)
        $('#total_spend').text(formatCurrencyIDR(totalSpend));
        $('#total_clicks_fb').text(totalClicksFb.toLocaleString('id-ID'));
        $('#total_clicks_adx').text(totalClicksAdx.toLocaleString('id-ID'));
        $('#rata_cpr').text(formatCurrencyIDR(totalCPR / data.length));
        $('#total_ctr_fb').text(averageCTRFb.toFixed(2) + '%');
        $('#total_ctr_adx').text(averageCTRAdx.toFixed(2) + '%');
        $('#total_roi').text(totalROI.toFixed(2) + '%');
        $('#total_revenue').text(formatCurrencyIDR(totalRevenue));
    }
    // Fungsi untuk inisialisasi DataTable
    function initializeDataTable(data) {
        var tableData = [];
        if (data && Array.isArray(data)) {
            data.forEach(function (row) {
                var countryFlag = '';
                if (row.country_code) {
                    countryFlag = '<img src="https://flagcdn.com/16x12/' + row.country_code.toLowerCase() + '.png" alt="' + row.country_code + '" style="margin-right: 5px;"> ';
                }
                // Simpan ANGKA MURNI; format dilakukan di renderer display
                tableData.push([
                    countryFlag + (row.country || ''),   // 0: Negara
                    row.country_code || '',              // 1: Kode Negara
                    Number(row.spend || 0),              // 2: Spend (Rp)
                    Number(row.clicks_fb || 0),          // 3: Klik FB
                    Number(row.clicks_adx || 0),         // 4: Klik ADX
                    Number(row.cpr || 0),                // 5: CPR
                    Number(row.ctr_fb || 0),             // 6: CTR FB (%)
                    Number(row.ctr_adx || 0),            // 7: CTR ADX (%)
                    Number(row.cpc_fb || 0),             // 8: CPC FB (Rp)
                    Number(row.cpc_adx || 0),            // 9: CPC ADX (Rp)
                    Number(row.ecpm || 0),               // 10: eCPM (Rp)
                    Number(row.roi || 0),                // 11: ROI (%)
                    Number(row.revenue || 0)             // 12: Pendapatan (Rp)
                ]);
            });
        }
        // Destroy existing DataTable dan bersihkan state agar default order berlaku
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            var existing = $('#table_traffic_country').DataTable();
            if (existing.state) { existing.state.clear(); }
            existing.destroy();
        }
        
        var table = $('#table_traffic_country').DataTable({
            // Matikan stateSave supaya default order tidak di-override oleh state lama
            stateSave: false,
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
                },
                {
                    extend: 'copy',
                    text: 'Copy',
                    className: 'btn btn-info'
                },
                {
                    extend: 'csv',
                    text: 'Export CSV',
                    className: 'btn btn-primary'
                },
                {
                    extend: 'print',
                    text: 'Print',
                    className: 'btn btn-warning'
                },
                {
                    extend: 'colvis',
                    text: 'Column Visibility',
                    className: 'btn btn-default'
                }
            ],
            columnDefs: [
                // Spend (kolom 2): tampil Rupiah tanpa desimal, sort numerik
                {
                    targets: 2,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        if (type === 'sort' || type === 'type' || type === 'filter') return v;
                        return new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(v);
                        // Atau: return formatCurrencyIDR(v);
                    }
                },
                // Klik FB (kolom 3): tampil ribuan, sort numerik
                {
                    targets: 3,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        return (type === 'sort' || type === 'type' || type === 'filter') ? v : v.toLocaleString('id-ID');
                    }
                },
                // Klik ADX (kolom 4): tampil ribuan, sort numerik
                {
                    targets: 4,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        return (type === 'sort' || type === 'type' || type === 'filter') ? v : v.toLocaleString('id-ID');
                    }
                },
                // CPR (kolom 5): tampil ribuan, sort numerik
                {
                    targets: 5,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        if (type === 'sort' || type === 'type' || type === 'filter') return v;
                        return new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(v);
                    }
                },
                // CTR FB (kolom 6): tampil ribuan, sort numerik
                {
                    targets: 6,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        return (type === 'sort' || type === 'type' || type === 'filter') ? v : v.toFixed(2) + '%';
                    }
                },
                // CTR ADX (kolom 7): tampil ribuan, sort numerik
                {
                    targets: 7,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        return (type === 'sort' || type === 'type' || type === 'filter') ? v : v.toFixed(2) + '%';
                    }
                },
                // CPC FB (kolom 8): tampil Rupiah dengan desimal, sort numerik
                {
                    targets: 8,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        if (type === 'sort' || type === 'type' || type === 'filter') return v;
                        return new Intl.NumberFormat('id-ID', { 
                            style: 'currency', 
                            currency: 'IDR', 
                            minimumFractionDigits: 0, 
                            maximumFractionDigits: 0 
                        }).format(v);
                    }
                },
                // CPC ADX (kolom 9): tampil Rupiah dengan desimal, sort numerik
                {
                    targets: 9,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        if (type === 'sort' || type === 'type' || type === 'filter') return v;
                        return new Intl.NumberFormat('id-ID', { 
                            style: 'currency', 
                            currency: 'IDR', 
                            minimumFractionDigits: 0, 
                            maximumFractionDigits: 0 
                        }).format(v);
                    }
                },
                // eCPM (kolom 10): tampil Rupiah dengan desimal, sort numerik
                {
                    targets: 10,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        if (type === 'sort' || type === 'type' || type === 'filter') return v;
                        return new Intl.NumberFormat('id-ID', { 
                            style: 'currency', 
                            currency: 'IDR', 
                            minimumFractionDigits: 0, 
                            maximumFractionDigits: 0 
                        }).format(v);
                    }
                },
                // ROI (kolom 11): tampil persen, sort numerik
                {
                    targets: 11,
                    type: 'num-fmt',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        return (type === 'sort' || type === 'type' || type === 'filter') ? v : v.toFixed(2) + '%';
                    }
                },
                // Pendapatan (kolom 12): tampil Rupiah tanpa desimal, sort numerik
                {
                    targets: 12,
                    type: 'num',
                    render: function (data, type) {
                        var v = Number(data) || 0;
                        if (type === 'sort' || type === 'type' || type === 'filter') return v;
                        return new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(v);
                        // Atau: return formatCurrencyIDR(v);
                    }
                }
            ],
            order: [[11, 'desc']]
            // ... existing code ...
        });
        // ... existing code ...
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
        
        // Gunakan kode negara langsung dari data (ISO-2), fallback ke nama bila perlu
        
        // Process data and find max ROI for color scaling
        data.forEach(function(item) {
            var roiValue = parseFloat(item.roi) || 0;
            if (roiValue > maxROI) maxROI = roiValue;
            if (roiValue < minROI) minROI = roiValue;
            var countryCode = (item.country_code || '').toLowerCase();
            var countryName = item.country || 'Unknown';
            if (!countryCode) {
                // Fallback sederhana dari nama
                countryCode = (countryName || 'xx').toLowerCase().substring(0, 2);
            }
            mapData.push({
                'hc-key': countryCode,
                code: countryCode.toUpperCase(),
                name: countryName,
                value: roiValue,
                clicks_fb: item.clicks_fb || 0,
                clicks_adx: item.clicks_adx || 0,
                spend: item.spend || 0,
                revenue: item.revenue || 0
            });
        });

        // Buat kelas warna dinamis berdasarkan nilai maksimum ROI
        var ranges;
        if (maxROI <= 100) {
            ranges = [
                { from: 0, to: 20, color: '#FFF2CC', name: '0% - 20%' },
                { from: 20, to: 40, color: '#FFE066', name: '20% - 40%' },
                { from: 40, to: 60, color: '#FFCC02', name: '40% - 60%' },
                { from: 60, to: 80, color: '#FF9500', name: '60% - 80%' },
                { from: 80, to: 100, color: '#FF6B35', name: '80% - 100%' }
            ];
        } else if (maxROI <= 500) {
            ranges = [
                { from: 0, to: 50, color: '#FFF2CC', name: '0% - 50%' },
                { from: 50, to: 100, color: '#FFE066', name: '50% - 100%' },
                { from: 100, to: 200, color: '#FFCC02', name: '100% - 200%' },
                { from: 200, to: 350, color: '#FF9500', name: '200% - 350%' },
                { from: 350, to: 500, color: '#FF6B35', name: '350% - 500%' }
            ];
        } else if (maxROI <= 1000) {
            ranges = [
                { from: 0, to: 50, color: '#FFF2CC', name: '0% - 50%' },
                { from: 50, to: 100, color: '#FFE066', name: '50% - 100%' },
                { from: 100, to: 200, color: '#FFCC02', name: '100% - 200%' },
                { from: 200, to: 500, color: '#FF9500', name: '200% - 500%' },
                { from: 500, to: 1000, color: '#FF6B35', name: '500% - 1000%' }
            ];
        } else {
            ranges = [
                { from: 0, to: 50, color: '#FFF2CC', name: '0% - 50%' },
                { from: 50, to: 100, color: '#FFE066', name: '50% - 100%' },
                { from: 100, to: 200, color: '#FFCC02', name: '100% - 200%' },
                { from: 200, to: 500, color: '#FF9500', name: '200% - 500%' },
                { from: 500, to: 1000, color: '#FF6B35', name: '500% - 1000%' },
                { from: 1000, to: 2000, color: '#E63946', name: '1000% - 2000%' },
                { from: 2000, to: Infinity, color: '#A4161A', name: '> 2000%' }
            ];
        }

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
                                   'Clicks (FB): <b>' + this.clicks_fb.toLocaleString('id-ID') + '</b><br>' +
                                   'Clicks (ADX): <b>' + this.clicks_adx.toLocaleString('id-ID') + '</b><br>' +
                                   'Spend: <b>Rp ' + Math.round(this.spend).toLocaleString('id-ID') + '</b><br>' +
                                   'CPR: <b>' + this.cpr.toLocaleString('id-ID') + '</b><br>' +
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
    // Re-render saat toggle hide zero spend berubah
    $('#toggle_hide_zero_spend').on('change', function () {
        // Simpan preferensi pengguna
        var checked = $(this).is(':checked');
        localStorage.setItem('roi_hide_zero_spend', checked ? '1' : '0');
        var baseData = window.lastRoiData || [];
        var displayData = applyZeroSpendFilter(baseData);
        updateCountryOptionsFromRoi(displayData);
        updateSummaryBoxes(displayData);
        initializeDataTable(displayData);
        generateTrafficCountryCharts(displayData);
    });
    
    // Fungsi untuk report error (jika ada)
    function report_eror(message) {
        console.error('Error:', message);
        alert(message);
    }
});