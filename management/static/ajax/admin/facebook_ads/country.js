/**
 * Reference Ajax Traffic Per Country Js
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

    // Set default tanggal hari ini
    var today = new Date();
    var todayString = today.getFullYear() + '-' +
        String(today.getMonth() + 1).padStart(2, '0') + '-' +
        String(today.getDate()).padStart(2, '0');
    $('#tanggal_dari').val(todayString);
    $('#tanggal_sampai').val(todayString);
    $('#tanggal_dari').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    });
    $('#tanggal_sampai').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    });
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    $('#select_sub_domain').select2({
        placeholder: '-- Pilih Domain --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    $('#select_country').select2({
        placeholder: '-- Pilih Negara --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4',
        multiple: true
    });
    // Load data negara untuk select2
    $('#btn_load_data').click(function (e) {
        e.preventDefault();
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#select_account").val() || '%';
        var data_account = selected_account ? selected_account : '%';
        var selected_sub_domain = $('#select_sub_domain').val() || '%';
        var data_sub_domain = selected_sub_domain ? selected_sub_domain : '%';
        if (tanggal_dari !== '' && tanggal_sampai !== '' && data_account != "") {
            loadSitesList();
            load_country_options(tanggal_dari, tanggal_sampai, data_account);
            destroy_table_data_per_country_facebook();
            table_data_per_country_facebook(tanggal_dari, tanggal_sampai, data_account, data_sub_domain);
        }
    });
});

function loadSitesList() {
    var selectedAccounts = $("#select_account").val() || "";
    // Simpan pilihan domain yang sudah dipilih sebelumnya
    var previouslySelected = $("#select_sub_domain").val() || [];
    $.ajax({
        url: '/management/admin/ads_sites_list',
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
                var select_site = $("#select_sub_domain");
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
            report_eror(xhr, status);
        }
    });
}

// Fungsi untuk memuat opsi negara ke select2
function load_country_options(tanggal_dari, tanggal_sampai, data_account) {
    // Simpan pilihan country yang sudah dipilih sebelumnya
    var previouslySelected = $("#select_country").val() || [];
    $.ajax({
        url: '/management/admin/get_countries_facebook_ads',
        type: 'GET',
        dataType: 'json',
        data: {
            'tanggal_dari': tanggal_dari,
            'tanggal_sampai': tanggal_sampai,
            'data_account': data_account
        },
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRFToken': $('[name=csrfmiddlewaretoken]').val()
        },
        success: function (response) {
            if (response.status) {
                var select_country = $('#select_country');
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
function table_data_per_country_facebook(tanggal_dari, tanggal_sampai, data_account, data_sub_domain) {
    var selected_countries = $('#select_country').val() || [];
    $.ajax({
        url: '/management/admin/page_per_country_facebook',
        type: 'POST',
        data: {
            'tanggal_dari': tanggal_dari,
            'tanggal_sampai': tanggal_sampai,
            'data_account': data_account,
            'data_sub_domain': data_sub_domain,
            'selected_countries': JSON.stringify(selected_countries),
            'csrfmiddlewaretoken': $('[name=csrfmiddlewaretoken]').val()
        },
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').show();
        },
        success: function (data_country) {
            $('#overlay').hide();
            const tanggal = new Date();
            judul = "Rekapitulasi Traffic Per Country Facebook";
            $.each(data_country.data_country, function (index, value) {
                const frequency = Number(value?.frequency) || 0;
                const formattedFrequency = frequency.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                var event_data = '<tr>';
                event_data += '<td class="text-left" style="font-size: 12px;"><b>' + value.country + '</b></td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.spend).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.impressions).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.reach).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.clicks).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + formattedFrequency + ' %</td>';
                event_data += '</tr>';
                $("#table_data_per_country_facebook tbody").append(event_data);
            })
            // Menggunakan data total yang sudah difilter dari backend
            const totalData = data_country.total_country;
            // Spend
            const spend = Number(totalData?.spend) || 0;
            const totalSpend = spend.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
            // Impressions
            const impressions = Number(totalData?.impressions) || 0;
            const totalImpressions = impressions.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
            // Reach
            const reach = Number(totalData?.reach) || 0;
            const totalReach = reach.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
            // Clicks
            const clicks = Number(totalData?.clicks) || 0;
            const totalClicks = clicks.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
            // Frequency
            const frequency = Number(totalData?.frequency) || 0;
            const totalFrequency = frequency.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + ' %';
            $('#total_spend').text(totalSpend);
            $('#total_impressions').text(totalImpressions);
            $('#total_reach').text(totalReach);
            $('#total_clicks').text(totalClicks);
            $('#total_frequency').text(totalFrequency);
            // Periksa apakah DataTable sudah diinisialisasi sebelumnya
            if ($.fn.dataTable.isDataTable('#table_data_per_country_facebook')) {
                $('#table_data_per_country_facebook').DataTable().destroy();
            }
            $('#table_data_per_country_facebook').DataTable({
                "paging": true,
                "pageLength": 50,
                "lengthChange": true,
                "searching": true,
                "ordering": true,
                responsive: true,
                dom: 'Blfrtip',
                searching: true,
                buttons: [
                    {
                        extend: 'excel',
                        filename: judul,
                        text: 'Download Excel',
                        title: judul,
                        messageTop: "laporan traffic per country facebook didownload pada "
                            + tanggal.getHours() + ":"
                            + tanggal.getMinutes() + " "
                            + tanggal.getDate() + "-"
                            + (tanggal.getMonth() + 1) + "-"
                            + tanggal.getFullYear(),
                        exportOptions: {
                            columns: ':visible',
                            columns: [0, 1, 2, 3, 4, 5],      // hanya kolom yang terlihat
                            modifier: {
                                search: 'applied',      // sesuai filter pencarian
                                order: 'applied'        // sesuai urutan saat itu
                            }
                        },
                        customize: function (xlsx) {
                            const sheet = xlsx.xl.worksheets['sheet1.xml'];
                            // =========================
                            // Set column width secara manual (unit: character width)
                            // =========================
                            const colWidths = [20, 10, 10, 10, 10, 10]; // 💡 Sesuaikan berdasarkan % di HTML
                            const cols = $('cols', sheet);
                            cols.empty(); // Kosongkan default <col> dari DataTables
                            for (let i = 0; i < colWidths.length; i++) {
                                cols.append(
                                    `<col min="${i + 1}" max="${i + 1}" width="${colWidths[i]}" customWidth="1"/>`
                                );
                            }

                        }
                    },
                    {
                        extend: 'pdf',
                        orientation: 'landscape',
                        pageSize: 'A4',
                        filename: judul,
                        text: 'Download Pdf',
                        className: 'btn btn-warning',
                        title: judul,
                        messageBottom: "laporan traffic per country facebook didownload pada "
                            + tanggal.getHours() + ":"
                            + tanggal.getMinutes()
                            + " " + tanggal.getDate()
                            + "-" + (tanggal.getMonth() + 1)
                            + "-" + tanggal.getFullYear(),
                        customize: function (doc) {
                            // Header style (bold + center)
                            doc.styles.tableHeader = {
                                bold: true,
                                fontSize: 11,
                                color: 'black',
                                alignment: 'center'
                            };

                            // Ambil body tabel (data + header)
                            const body = doc.content[1].table.body;
                            // Loop dari baris kedua (index 1, karena index 0 adalah header)
                            for (let i = 1; i < body.length; i++) {
                                if (body[i]) {
                                    if (body[i][0]) body[i][0].alignment = 'center';
                                    if (body[i][1]) body[i][1].alignment = 'left';
                                    if (body[i][2]) body[i][2].alignment = 'right';
                                    if (body[i][3]) body[i][3].alignment = 'right';
                                    if (body[i][4]) body[i][4].alignment = 'right';
                                    if (body[i][5]) body[i][5].alignment = 'right';
                                }
                            }
                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]
                            // Manual width sesuai presentase kolom HTML (tanpa kolom terakhir)
                            doc.content[1].table.widths = ['20%', '10%', '10%', '10%', '10%', '10%', '10%'];
                        }
                    }
                ]
            });

            // Render world spend map (white-to-black)
            try {
                createSpendMap(data_country.data_country || []);
            } catch (err) {
                console.error('Failed to create spend map:', err);
            }
        }
    });
}

function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}
const csrftoken = getCookie('csrftoken');

function destroy_table_data_per_country_facebook() {
    // Periksa apakah tabel sudah diinisialisasi sebagai DataTable
    if ($.fn.dataTable.isDataTable('#table_data_per_country_facebook')) {
        $('#table_data_per_country_facebook').DataTable().clear().destroy();
    }
    // Bersihkan konten tbody secara manual
    $('#table_data_per_country_facebook tbody').empty();
}

// Highcharts Map: Spend per Country (White → Black)
function createSpendMap(items) {
    // Siapkan data peta
    var mapData = [];
    if (!items || !items.length) {
        // Bersihkan peta bila tidak ada data
        if (window.fbSpendMapInstance) {
            try { window.fbSpendMapInstance.destroy(); } catch (e) { }
            window.fbSpendMapInstance = null;
        }
        $('#charts_section').hide();
        return;
    }

    items.forEach(function (item) {
        var spend = parseFloat(item.spend) || 0;
        var countryField = String(item.country || '');
        // Ekstrak kode negara dari format "Nama Negara (CODE)"
        var codeMatch = countryField.match(/\(([A-Za-z]{2})\)/);
        var countryCode = codeMatch ? codeMatch[1].toLowerCase() : null;
        if (!countryCode) return;

        mapData.push({
            'hc-key': countryCode,
            code: countryCode.toUpperCase(),
            name: countryField.split('(')[0].trim(),
            value: spend,
            impressions: item.impressions || 0,
            clicks: item.clicks || 0,
            reach: item.reach || 0,
            frequency: item.frequency || 0
        });
    });

    if (!mapData.length) {
        $('#charts_section').hide();
        return;
    }

    // Tampilkan section peta
    $('#charts_section').show();
    $('#worldMap').css({ height: '500px', width: '100%', display: 'block', visibility: 'visible' });

    // Hancurkan instance lama
    if (window.fbSpendMapInstance) {
        try { window.fbSpendMapInstance.destroy(); } catch (e) { }
        window.fbSpendMapInstance = null;
    }

    // Kelas warna: putih (rendah) ke hitam (tinggi)
    var ranges = [
        { from: 0, to: 0, color: '#ffffff', name: '0' },
        { from: 1, to: 50, color: '#f2f2f2', name: '1 - 50' },
        { from: 50, to: 250, color: '#d9d9d9', name: '51 - 250' },
        { from: 250, to: 1000, color: '#bfbfbf', name: '251 - 1.000' },
        { from: 1000, to: 5000, color: '#999999', name: '1.001 - 5.000' },
        { from: 5000, to: 10000, color: '#666666', name: '5.001 - 10.000' },
        { from: 10000, to: Infinity, color: '#000000', name: '> 10.000' }
    ];

    // Render peta
    window.fbSpendMapInstance = Highcharts.mapChart('worldMap', {
        chart: {
            map: 'custom/world',
            backgroundColor: 'transparent',
            style: { fontFamily: 'Arial, sans-serif' }
        },
        title: {
            text: 'Spend Facebook Ads Per Negara',
            style: { fontSize: '16px', fontWeight: '600', color: '#333' }
        },
        subtitle: {
            text: 'Skema warna: putih → hitam berdasarkan spend',
            style: { fontSize: '12px', color: '#666' }
        },
        mapNavigation: {
            enabled: true,
            buttonOptions: { verticalAlign: 'bottom' }
        },
        colorAxis: {
            min: 0,
            minColor: '#ffffff',
            maxColor: '#000000',
            dataClasses: ranges.map(function (range) {
                return { from: range.from, to: range.to, color: range.color, name: range.name };
            })
        },
        legend: {
            title: { text: 'Tingkat Spend', style: { color: '#333', fontSize: '12px' } },
            align: 'left',
            verticalAlign: 'bottom',
            floating: true,
            layout: 'vertical',
            backgroundColor: 'rgba(255,255,255,0.9)',
            symbolRadius: 0,
            symbolHeight: 14
        },
        series: [{
            name: 'Negara',
            data: mapData,
            joinBy: ['hc-key', 'hc-key'],
            nullColor: '#e6e7e8',
            borderColor: '#606060',
            borderWidth: 0.5,
            states: { hover: { color: '#444444' } },
            tooltip: {
                backgroundColor: 'rgba(0,0,0,0.85)',
                style: { color: 'white' },
                pointFormatter: function () {
                    var spendStr = 'Rp ' + Math.round(this.value).toLocaleString('id-ID');
                    return '<b>' + this.name + '</b><br>' +
                        'Kode: ' + this.code + '<br>' +
                        'Spend: <b>' + spendStr + '</b><br>' +
                        'Impressions: ' + Number(this.impressions).toLocaleString('id-ID') + '<br>' +
                        'Reach: ' + Number(this.reach).toLocaleString('id-ID') + '<br>' +
                        'Clicks: ' + Number(this.clicks).toLocaleString('id-ID') + '<br>' +
                        'Frequency: ' + (Number(this.frequency) || 0).toFixed(2);
                },
                nullFormat: '<b>{point.name}</b><br>Tidak ada data'
            },
            allAreas: true
        }],
        exporting: {
            enabled: true,
            buttons: { contextButton: { menuItems: ['viewFullscreen', 'separator', 'downloadPNG', 'downloadJPEG', 'downloadPDF', 'downloadSVG'] } }
        }
    });
}