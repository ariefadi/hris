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
    // Initialize Select2 for account
    $('#account_filter').select2({
        placeholder: '-- Pilih Account Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    // Initialize Select2 for domain
    $('#domain_filter').select2({
        placeholder: '-- Pilih Domain Terdaftar --',
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
    // Event handler untuk tombol Load
    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domain = $("#domain_filter").val();
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            $("#overlay").show();
            load_country_options(selected_account, selected_domain);
            load_adx_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    // Fungsi untuk memuat opsi negara ke select2
    function load_country_options(selected_account, selectedDomains) {
        if (selectedDomains) {
            selectedDomains = selectedDomains.join(',');
        }
        // Simpan pilihan country yang sudah dipilih sebelumnya
        var previouslySelected = $("#country_filter").val() || [];
        $.ajax({
            url: '/management/admin/get_countries_adx',
            type: 'GET',
            dataType: 'json',
            data: {
                'selected_account': selected_account,
                'selected_domains': selectedDomains
            },
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
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
                report_eror(xhr, status);
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
    function load_adx_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selectedDomains) {
        var startDate = tanggal_dari;
        var endDate = tanggal_sampai;
        var selected_account = selected_account;
        var selectedDomains = selectedDomains;
        var selectedCountries = $('#country_filter').val();
        if (!startDate || !endDate) {
            alert('Silakan pilih rentang tanggal');
            return;
        }
        // Convert array to comma-separated string for backend
        var domainFilter = '';
        if (selectedDomains && selectedDomains.length > 0) {
            domainFilter = selectedDomains.join(',');
        }
        // Convert array to comma-separated string for backend
        var countryFilter = '';
        if (selectedCountries && selectedCountries.length > 0) {
            countryFilter = selectedCountries.join(',');
        }
        // Tampilkan overlay loading
        $('#overlay').show();
        // Destroy existing DataTable if exists
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().destroy();
        }
        // Tampilkan overlay loading
        $('#overlay').show();
        // AJAX request
        $.ajax({
            url: '/management/admin/page_adx_traffic_country',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                selected_account: selected_account,
                selected_domains: domainFilter,
                selected_countries: countryFilter
            },
            headers: {
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function (response) {
                if (response && response.status) {
                    // Update summary boxes
                    updateSummaryBoxes(response.summary);
                    $('#summary_boxes').show();
                    // Initialize DataTable
                    initializeDataTable(response.data);
                    // Generate charts if data available
                    createCountryMap(response.data);
                    $('#charts_section').show();
                    $('#overlay').hide();
                } else {
                    var errorMsg = response.error || 'Terjadi kesalahan yang tidak diketahui';
                    console.error('[DEBUG] Response error:', errorMsg);
                    alert('Error: ' + errorMsg);
                }
            },
            error: function (xhr, status, error) {
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
    function updateSummaryBoxes(data) {
        var totalImpressions = Number(data.total_impressions || 0);
        var totalClicks = Number(data.total_clicks || 0);
        var totalRevenue = parseFloat(data.total_revenue || 0) || 0;
        var totalCtrRatio = parseFloat(data.total_ctr || 0) || 0;
        $('#total_impressions').text(totalImpressions.toLocaleString('id-ID'));
        $('#total_clicks').text(totalClicks.toLocaleString('id-ID'));
        $('#total_ctr').text(totalCtrRatio > 0 ? (totalCtrRatio * 100).toFixed(2) + '%' : '0%');
        $('#total_revenue').text(formatCurrencyIDR(totalRevenue));
    }

    // Fungsi untuk inisialisasi DataTable
    function initializeDataTable(data) {
        var tableData = [];
        if (data && Array.isArray(data)) {
            data.forEach(function (row) {
                var countryFlag = '';
                if (row.country_code) {
                    countryFlag = '<img src="https://flagcdn.com/16x12/' + String(row.country_code).toLowerCase() + '.png" alt="' + row.country_code + '" style="margin-right: 5px;"> ';
                }
                var impressionsNum = Number(row.impressions || 0);
                var clicksNum = Number(row.clicks || 0);
                var ctrNum = parseFloat(row.ctr);
                if (isNaN(ctrNum)) ctrNum = 0;
                var cpcNum = parseFloat(row.cpc || 0) || 0;
                var ecpmNum = parseFloat(row.ecpm || 0) || 0;
                var revenueNum = parseFloat(row.revenue || 0) || 0;

                tableData.push([
                    countryFlag + (row.country_name || ''),
                    row.country_code || '',
                    impressionsNum.toLocaleString('id-ID'),
                    clicksNum.toLocaleString('id-ID'),
                    ctrNum.toFixed(2) + '%',
                    formatCurrencyIDR(cpcNum),
                    formatCurrencyIDR(ecpmNum),
                    revenueNum
                ]);
            });
        }
        // Destroy existing DataTable if it exists
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().destroy();
        }
        var table = $('#table_traffic_country').DataTable({
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
                {
                    targets: 7,
                    type: 'num',
                    render: function (data, type, row) {
                        if (type === 'display') {
                            return formatCurrencyIDR(data || 0);
                        }
                        return data; // gunakan nilai numerik untuk sort/filter
                    }
                }
            ]
        });

        // Paksa urutan setelah inisialisasi untuk memastikan tidak tertimpa
        table.order([7, 'desc']).draw();
    }

    // Fungsi untuk membuat chart dengan Highcharts Maps
    function createCountryMap(data) {
        console.log('[DEBUG] createCountryMap called with data length:', data ? data.length : 0);

        // Jika tidak ada data, pastikan charts dibersihkan dan section disembunyikan
        if (!data || data.length === 0) {
            if (window.countryMapInstance) {
                try { window.countryMapInstance.destroy(); } catch (e) { console.warn('Failed to destroy world map:', e); }
                window.countryMapInstance = null;
            }
            $('#charts_section').hide();
            return;
        }

        // Prepare data for Highcharts Maps
        var mapData = [];
        var maxRevenue = 0;
        var minRevenue = Infinity;

        // Process data and find max revenue for color scaling
        data.forEach(function (item) {
            var revenue = parseFloat(item.revenue) || 0;
            if (revenue > 0) {
                if (revenue > maxRevenue) {
                    maxRevenue = revenue;
                }
                if (revenue < minRevenue) {
                    minRevenue = revenue;
                }

                // Map country codes to Highcharts format
                var countryCode = item.country_code;
                if (countryCode) {
                    mapData.push({
                        'hc-key': countryCode.toLowerCase(),
                        code: countryCode,
                        name: item.country_name || 'Unknown',
                        value: revenue,
                        impressions: item.impressions || 0,
                        clicks: item.clicks || 0,
                        ctr: item.ctr || 0,
                        cpc: item.cpc || 0,
                        ecpm: item.ecpm || 0
                    });
                }
            }
        });
        // Create fixed color ranges with more vibrant colors and informative labels
        var ranges = [
            { from: null, to: null, color: '#E6E7E8', name: 'Tidak ada data' }, // Entry untuk negara tanpa data
            { from: 0, to: 50000, color: '#FFF2CC', name: 'Rp.0 - Rp.50.000' },
            { from: 50000, to: 100000, color: '#FFE066', name: 'Lebih dari Rp.50.000 - Rp.100.000' },
            { from: 100000, to: 500000, color: '#FFCC02', name: 'Lebih dari Rp.100.000 - Rp.500.000' },
            { from: 500000, to: 1000000, color: '#FF9500', name: 'Lebih dari Rp.500.000 - Rp.1.000.000' },
            { from: 1000000, to: 5000000, color: '#FF6B35', name: 'Lebih dari Rp.1.000.000 - Rp.5.000.000' },
            { from: 5000000, to: 10000000, color: '#E63946', name: 'Lebih dari Rp.5.000.000 - Rp.10.000.000' },
            { from: 10000000, to: Infinity, color: '#A4161A', name: '> Rp.10.000.000' }
        ];
        // Destroy existing chart if any
        if (window.countryMapInstance) {
            try { window.countryMapInstance.destroy(); } catch (e) { console.warn('Failed to destroy world map:', e); }
            window.countryMapInstance = null;
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
            window.countryMapInstance = Highcharts.mapChart('worldMap', {
                chart: {
                    map: 'custom/world',
                    backgroundColor: 'transparent',
                    style: {
                        fontFamily: 'Arial, sans-serif'
                    }
                },
                title: {
                    text: 'Pendapatan AdX Per Negara',
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
                    enabled: false,
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
                    minColor: '#FFF2CC', // Warna kuning muda untuk pendapatan terendah
                    maxColor: '#A4161A', // Warna merah tua untuk pendapatan tertinggi
                    dataClasses: ranges.map(function (range) {
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
                        text: 'Tingkat Pendapatan',
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
                            'Pendapatan: <b>Rp {point.value:,.0f}</b><br>',
                        pointFormatter: function () {
                            var formattedValue = 'Rp ' + Math.round(this.value).toLocaleString('id-ID');
                            return '<b>' + this.name + '</b><br>' +
                                'Kode: ' + this.code + '<br>' +
                                'Pendapatan: <b>' + formattedValue + '</b><br>';
                        },
                        nullFormat: '<b>{point.name}</b><br>Tidak ada data traffic'
                    },
                    borderColor: '#606060',
                    borderWidth: 0.5,
                    states: {
                        hover: {
                            color: '#FFD700' // Warna emas untuk hover yang lebih sesuai dengan skema warna baru
                        }
                    },
                    allAreas: true // Tampilkan semua negara, termasuk yang tidak ada data
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
                                states: {
                                    hover: {
                                        fill: '#a4edba'
                                    }
                                }
                            },
                            menuItems: ['viewFullscreen', 'separator', 'downloadPNG', 'downloadJPEG', 'downloadPDF', 'downloadSVG']
                        }
                    }
                }
            });
            console.log('[DEBUG] Map created successfully');
        } catch (error) {
            console.error('[ERROR] Failed to create map:', error);
            alert('Error creating map: ' + error.message);
            // Fallback: show a message in the map container
            $('#worldMap').html('<div style="text-align: center; padding: 50px; color: #666;">Error loading map: ' + error.message + '</div>');
        }
    }
    // Fungsi untuk report error (jika ada)
    function report_eror(message) {
        console.error('Error:', message);
        alert(message);
    }
});