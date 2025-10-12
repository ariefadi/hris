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
    $('#select_sub_domain').select2({
        placeholder: '-- Pilih Sub Domain --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    $('#select_country').select2({
        placeholder: '-- Pilih Negara --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4',
        multiple: true
    })
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
    $('#tanggal_dari').change(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var data_sub_domain = '%';
        var data_account = '%';
        if(tanggal_dari!="" && tanggal_sampai!="" && data_sub_domain!="" && data_account!="")
        {
            destroy_table_data_per_country_facebook()
            table_data_per_country_facebook(tanggal_dari, tanggal_sampai, data_sub_domain, data_account)
        }    
    });
    $('#tanggal_sampai').change(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var data_sub_domain = '%';
        var data_account = '%';
        if(tanggal_dari!="" && tanggal_sampai!="" && data_sub_domain!="" && data_account!="")
        {
            destroy_table_data_per_country_facebook()
            table_data_per_country_facebook(tanggal_dari, tanggal_sampai, data_sub_domain, data_account)
        }    
    });
    $('#select_sub_domain').change(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected = $(this).val();
        var data_sub_domain = selected ? selected : '%'; 
        var data_account = '%';
        if(tanggal_dari!="" && tanggal_sampai!="" && data_sub_domain!="" && data_account!="")
        {
            destroy_table_data_per_country_facebook()
            table_data_per_country_facebook(tanggal_dari, tanggal_sampai, data_sub_domain, data_account)
        }    
    });
    $('#select_account').change(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var data_sub_domain = $("#select_sub_domain option:selected").val() || '%';
        var selected = $(this).val();
        var data_account = selected ? selected : '%';
        if(tanggal_dari!="" && tanggal_sampai!="" && data_sub_domain!="" && data_account!="")
        {
            destroy_table_data_per_country_facebook()
            table_data_per_country_facebook(tanggal_dari, tanggal_sampai, data_sub_domain, data_account)
        }    
    });
    
    // Auto-load data akan dipanggil setelah country options dimuat
    
    // Load data negara untuk select2
    load_country_options();
    
    // Event handler untuk filter negara
    $('#select_country').change(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var data_sub_domain = $("#select_sub_domain option:selected").val() || '%';
        var data_account = $("#select_account option:selected").val() || '%';
        if(tanggal_dari!="" && tanggal_sampai!="" && data_sub_domain!="" && data_account!="")
        {
            destroy_table_data_per_country_facebook()
            table_data_per_country_facebook(tanggal_dari, tanggal_sampai, data_sub_domain, data_account)
        }    
    });
});
function table_data_per_country_facebook(tanggal_dari, tanggal_sampai, data_sub_domain, data_account) {
    var selected_countries = $('#select_country').val() || [];
    
    $.ajax({
        url: '/management/admin/page_per_country_facebook',
        type: 'POST',
        data: {
            'tanggal_dari': tanggal_dari,
            'tanggal_sampai': tanggal_sampai,
            'data_sub_domain': data_sub_domain,
            'data_account': data_account,
            'selected_countries': JSON.stringify(selected_countries),
            'csrfmiddlewaretoken': $('[name=csrfmiddlewaretoken]').val()
        },
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_country) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Traffic Per Country Facebook";
            $.each(data_country.data_country, function (index, value) {
                let data_cpr = value.cpr;
                let cpr_number = parseFloat(data_cpr)
                let cpr = cpr_number.toFixed(0).replace(',', '.');
                const frequency = Number(value?.frequency) || 0;
                const formattedFrequency = frequency.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                var event_data = '<tr>';
                event_data += '<td class="text-left" style="font-size: 12px;"><b>' + value.country + '</b></td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.spend).replace(/\B(?=(\d{3})+(?!\d))/g, ".") +  '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.impressions).replace(/\B(?=(\d{3})+(?!\d))/g, ".") +  '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.reach).replace(/\B(?=(\d{3})+(?!\d))/g, ".") +  '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.clicks).replace(/\B(?=(\d{3})+(?!\d))/g, ".") +  '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + formattedFrequency + ' %</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + cpr + '</td>';
                event_data += '</tr>';  
                $("#table_data_per_country_facebook tbody").append(event_data);    
            })
            // Debug: Log response structure
            console.log("DEBUG - Full response:", data_country);
            console.log("DEBUG - total_country:", data_country.total_country);
            
            // Menggunakan data total yang sudah difilter dari backend
            const totalData = data_country.total_country;
            
            console.log("DEBUG - totalData:", totalData);
            
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
            
            // CPR (Cost Per Result)
            const cpr = Number(totalData?.cost_per_result) || 0;
            const totalCpr = cpr.toFixed(0).replace(',', '.');
            
            console.log("DEBUG - Calculated values:", {
                spend, impressions, reach, clicks, frequency, cpr
            });
            
            $('#total_spend').text(totalSpend);
            $('#total_impressions').text(totalImpressions);
            $('#total_reach').text(totalReach);
            $('#total_clicks').text(totalClicks);
            $('#total_frequency').text(totalFrequency);
            $('#total_cpr').text(totalCpr);
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
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()+" "
                                    +tanggal.getDate()+"-"
                                    +(tanggal.getMonth()+1)+"-"
                                    +tanggal.getFullYear(),
                        exportOptions: {
                            columns: ':visible', 
                            columns: [0, 1, 2, 3, 4, 5, 6],      // hanya kolom yang terlihat
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
                            const colWidths = [20, 10, 10, 10, 10, 10, 10]; // 💡 Sesuaikan berdasarkan % di HTML
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
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()
                                    +" "+tanggal.getDate()
                                    +"-"+(tanggal.getMonth()+1)
                                    +"-"+tanggal.getFullYear(),
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

function destroy_table_data_per_country_facebook(){
    // Periksa apakah tabel sudah diinisialisasi sebagai DataTable
    if ($.fn.dataTable.isDataTable('#table_data_per_country_facebook')) {
        $('#table_data_per_country_facebook').DataTable().clear().destroy();
    }
    // Bersihkan konten tbody secara manual
    $('#table_data_per_country_facebook tbody').empty();
}

// Fungsi untuk memuat opsi negara ke select2
function load_country_options() {
    $.ajax({
        url: '/management/admin/get_countries_facebook_ads',
        type: 'GET',
        dataType: 'json',
        success: function(response) {
            console.log('Data Negara didapat : ', response);
            if(response.status) {
                var select_country = $('#select_country');
                select_country.empty();
                
                $.each(response.countries, function(index, country) {
                    select_country.append(new Option(country.name, country.code, false, false));
                });
                
                select_country.trigger('change');
                
                // Load data awal setelah country options dimuat
                var today = new Date();
                var todayString = today.getFullYear() + '-' + 
                                 String(today.getMonth() + 1).padStart(2, '0') + '-' + 
                                 String(today.getDate()).padStart(2, '0');
                
                table_data_per_country_facebook(todayString, todayString, '%', '%');
            }
        },
        error: function(xhr, status, error) {
            console.log('Error loading countries:', error);
        }
    });
}