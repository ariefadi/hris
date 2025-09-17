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

    // Event handler untuk tombol Load
    $('#btn_load_data').click(function() {
        load_adx_traffic_country_data();
    });

    // Load data saat halaman pertama kali dibuka
    load_adx_traffic_country_data();

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
        var countryFilter = $('#country_filter').val();

        if (!startDate || !endDate) {
            alert('Silakan pilih rentang tanggal');
            return;
        }

        console.log('[DEBUG] Loading country data with params:', {
            start_date: startDate,
            end_date: endDate,
            country_filter: countryFilter
        });

        // Tampilkan overlay loading
        $('#overlay').show();
        $('#summary_boxes').hide();

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
                country_filter: countryFilter
            },
            headers: {
                'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
            },
            success: function(response) {
                console.log('[DEBUG] AJAX Success Response:', response);
                $('#overlay').hide();
                
                if (response && response.status) {
                    // Update summary boxes
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
                    
                    // Show success message
                    if (response.data && response.data.length > 0) {
                        console.log('Data berhasil dimuat: ' + response.data.length + ' negara');
                    } else {
                        console.log('Tidak ada data untuk periode yang dipilih');
                    }
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
    function updateSummaryBoxes(data) {
        if (!data || !Array.isArray(data)) return;
        
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
            data.forEach(function(row) {
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
        
        // Sort data by ROI and take top 10
        var sortedData = data.sort(function(a, b) {
            return (b.roi || 0) - (a.roi || 0);
        }).slice(0, 10);
        
        // Prepare data for charts
        var countries = sortedData.map(function(item) {
            return item.country || 'Unknown';
        });
        
        var roi = sortedData.map(function(item) {
            return item.roi || 0;
        });
        
        // Create charts if Chart.js is available
        if (typeof Chart !== 'undefined') {
            // ROI Chart
            var ctx = document.getElementById('roiChart');
            if (ctx) {
                new Chart(ctx, {
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
                                    callback: function(value) {
                                        return value + '%';
                                    }
                                }
                            }
                        },
                        plugins: {
                            tooltip: {
                                callbacks: {
                                    label: function(context) {
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