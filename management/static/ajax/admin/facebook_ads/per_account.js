/**
 * Reference Ajax Traffic Per Account Js
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
    $('.select2').select2()
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    $('#tanggal').datepicker({
      format: 'yyyy-mm-dd',
      autoclose: true,
      todayHighlight: true
    });
    
    // Auto-load data saat halaman dimuat
    $(document).ready(function() {
        var data_account = '%'; // Default semua account
        var tanggal = $('#tanggal').val();
        var data_sub_domain = $('#select_sub_domain').val() || '%';
        
        if(tanggal && tanggal !== '' && data_sub_domain) {
            table_data_per_account_facebook(data_account, tanggal, data_sub_domain);
        }
    });
    $('#select_sub_domain').select2({
        placeholder: '-- Pilih Sub Domain --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    $('#select_account').change(function (e) {
        var selected = $(this).val();
        var data_account = selected ? selected : '%';
        var tanggal = $('#tanggal').val();
        var data_sub_domain = $('#select_sub_domain').val() || '%';
        if(tanggal && tanggal !== '' && data_sub_domain)
        {
            destroy_table_data_per_account_facebook()
            table_data_per_account_facebook(data_account, tanggal, data_sub_domain)
        }    
    });
    $(document).on('change', '#tanggal', function (e) {
        var data_account = $("#select_account option:selected").val() || '%';
        var tanggal = $("#tanggal").val();
        var data_sub_domain = $('#select_sub_domain').val() || '%';
        if(tanggal && tanggal !== '' && data_sub_domain)
        {
            destroy_table_data_per_account_facebook()
            table_data_per_account_facebook(data_account, tanggal, data_sub_domain)
        }   
    });
    $('#select_sub_domain').change(function (e) {
        var data_account = $("#select_account option:selected").val() || '%';
        var tanggal = $("#tanggal").val();
        var selected = $(this).val();
        var data_sub_domain = selected ? selected : '%'; 
        if(tanggal && tanggal !== '' && data_sub_domain)
        {
            destroy_table_data_per_account_facebook()
            table_data_per_account_facebook(data_account, tanggal, data_sub_domain)
        }  
    });
});
function table_data_per_account_facebook(data_account, tanggal, data_sub_domain) {
    $.ajax({
        url: '/management/admin/page_per_account_facebook?data_account='+data_account+'&tanggal='+tanggal+'&data_sub_domain='+data_sub_domain,
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_per_account) {
            $('#overlay').fadeOut(500);
            destroy_table_data_per_account_facebook()
            const tanggal = new Date();
            judul = "Rekapitulasi Traffic Per Account Facebook";
            $.each(data_per_account.data_per_account, function (index, value) {
                const date_active = value.start_time;
                // Ubah ke format yang dikenali JavaScript (tambahkan titik dua di zona waktu)
                const isoFormatted = date_active.replace(/(\+|\-)(\d{2})(\d{2})$/, '$1$2:$3');
                const date = new Date(isoFormatted);
                // Ambil komponen tanggal dan waktu
                const year = date.getFullYear();
                const month = String(date.getMonth() + 1).padStart(2, '0');
                const day = String(date.getDate()).padStart(2, '0');
                const hours = String(date.getHours()).padStart(2, '0');
                const minutes = String(date.getMinutes()).padStart(2, '0');
                const seconds = String(date.getSeconds()).padStart(2, '0');
                // Gabungkan dalam format yang diinginkan
                const formatted = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                // Budget
                const budget = Number(value?.daily_budget) || 0;
                const formattedBudget = budget.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Spend
                const spend = Number(value?.spend) || 0;
                const formattedSpend = spend.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Impressions
                const impressions = Number(value?.impressions) || 0;
                const formattedImpressions = impressions.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Reach
                const reach = Number(value?.reach) || 0;
                const formattedReach = reach.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Frequency
                const frequency = Number(value?.frequency) || 0;
                const formattedFrequency = frequency.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                // Clicks
                const clicks = Number(value?.clicks) || 0;
                const formattedClicks = clicks.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // CPR
                let data_cpr = value.cpr;
                let cpr_number = parseFloat(data_cpr)
                let cpr = cpr_number.toFixed(0).replace(',', '.');
                // Tambahkan logika: jika spend > budget, beri class `table-danger`
                const isOverBudget = spend > budget;
                const rowClass = isOverBudget ? 'table-danger' : '';
                var event_data = `<tr class="${rowClass}">`;
                event_data += '<td class="text-center" style="font-size: 12px;">' + (index + 1) + '</td>';
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-secondary" style="color: white;">' + (value.account_name || 'N/A') + '</span></td>';
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-info" style="color: white;">' + value.campaign_name + '</span></td>';
                if(value.status == 'ACTIVE'){
                    event_data += '<td class="text-center" style="font-size: 12px;"><span class="badge badge-primary" style="color: white;">Aktif</span></td>';
                }else if(value.status == 'PAUSED'){
                    event_data += '<td class="text-center" style="font-size: 12px;"><span class="badge badge-warning" style="color: white;">Pause</span></td>';
                }else{
                    event_data += '<td class="text-center" style="font-size: 12px;"><span class="badge badge-danger" style="color: white;">Tidak <br> Aktif</span></td>';
                }
                event_data += ' <td class="text-center" style="font-size: 12px;">' + formatted + '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' +
                                    '<input type="text" class="form-control text-right" style="font-size: 12px;" id="daily_budget_'+value.campaign_id+'" value="' + formattedBudget + '">' +
                                    '<span id="autosave-status_'+value.campaign_id+'" class="badge badge-danger" style="color: white; font-size:10px;"></span>'
                              ' </td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedSpend +  '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedImpressions +  '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedReach +  '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedClicks + '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedFrequency + ' %</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + cpr + '</td>';
                event_data += '<td class="text-center">' +
                                    '<div class="form-check form-switch">' +
                                        '<input class="form-check-input" type="checkbox" id="switch_campaign_'+value.campaign_id+'" ' + 
                                            (value.status === 'ACTIVE' ? 'checked' : '') + '>' +
                                    '</div>' +
                                    '<span id="autosave-button_'+value.campaign_id+'" class="badge badge-danger" style="color: white; font-size:10px;"></span>'
                                '</td>';
                event_data += '</tr>';  
                $("#table_data_per_account_facebook tbody").append(event_data)
                let timeout = null;
                $(`#daily_budget_${value.campaign_id}`).on('input', function () {
                    clearTimeout(timeout); // Reset timer
                    timeout = setTimeout(function () {
                        let formData = new FormData();
                        let content = $(`#daily_budget_${value.campaign_id}`).val();
                        var data_account = $("#select_account option:selected").val();
                        formData.append('account_id', data_account);
                        formData.append('campaign_id', value.campaign_id);
                        formData.append('daily_budget', content);
                        $.ajax({
                            url: '/management/admin/update_daily_budget_per_campaign',
                            method: 'POST',
                            data: formData,
                            headers: { 
                                "X-CSRFToken": csrftoken 
                            },
                            processData: false,
                            contentType: false,
                            dataType: "json",
                            success: function (data) {
                                $(`#autosave-status_${value.campaign_id}`).text('Daily Budget Diubah');
                                setTimeout(function () {
                                    table_data_per_account_facebook(data_account);
                                }, 1000);;
                            }
                        });    
                    });
                });
                $(`#switch_campaign_${value.campaign_id}`).on('change', function () {
                    clearTimeout(timeout); // Reset timer
                    timeout = setTimeout(function () {
                        let formData = new FormData();
                        let isChecked = $(`#switch_campaign_${value.campaign_id}`).prop('checked');
                        let status = isChecked ? 'ACTIVE' : 'PAUSED';
                        let data_account = $("#select_account option:selected").val() || '%';
                        formData.append('account_id', data_account);
                        formData.append('campaign_id', value.campaign_id);
                        formData.append('switch_campaign', status);
                        
                        // Tampilkan SweetAlert loading untuk individual switch
                        const loadingAlert = Swal.fire({
                            title: 'Mengupdate Status',
                            text: 'Mohon tunggu...',
                            icon: 'info',
                            allowOutsideClick: false,
                            allowEscapeKey: false,
                            showConfirmButton: false,
                            didOpen: () => {
                                Swal.showLoading();
                            }
                        });
                        
                        $.ajax({
                            url: '/management/admin/update_switch_campaign',
                            method: 'POST',
                            data: formData,
                            headers: { 
                                "X-CSRFToken": csrftoken 
                            },
                            processData: false,
                            contentType: false,
                            dataType: "json",
                            success: function (data) {
                                Swal.close();
                                
                                if (data.success) {
                                    // Tampilkan success alert
                                    Swal.fire({
                                        title: 'Berhasil!',
                                        text: data.message || 'Status campaign berhasil diubah',
                                        icon: 'success',
                                        timer: 1500,
                                        showConfirmButton: false
                                    });
                                    
                                    setTimeout(function () {
                                        var tanggal = $("#tanggal").val();
                                        var data_sub_domain = $("#select_sub_domain option:selected").val();
                                        table_data_per_account_facebook(data_account, tanggal, data_sub_domain);
                                        // Update header switch after individual switch update
                                        updateHeaderSwitch();
                                    }, 1000);
                                } else {
                                    // Tampilkan error alert dengan pesan dari server
                                    Swal.fire({
                                        title: 'Error!',
                                        text: data.message || 'Terjadi kesalahan saat mengubah status campaign',
                                        icon: 'error',
                                        confirmButtonText: 'OK'
                                    });
                                    
                                    // Kembalikan switch ke posisi sebelumnya
                                    $(`#switch_campaign_${value.campaign_id}`).prop('checked', !$(`#switch_campaign_${value.campaign_id}`).prop('checked'));
                                }
                            },
                            error: function(xhr, status, error) {
                                Swal.close();
                                
                                let errorMessage = 'Terjadi kesalahan saat mengubah status campaign';
                                
                                // Coba parse response JSON untuk mendapatkan pesan error yang lebih spesifik
                                try {
                                    const response = JSON.parse(xhr.responseText);
                                    if (response.message) {
                                        errorMessage = response.message;
                                    }
                                } catch (e) {
                                    // Jika tidak bisa parse JSON, gunakan pesan default
                                    if (xhr.status === 500) {
                                        errorMessage = 'Terjadi kesalahan server internal';
                                    } else if (xhr.status === 404) {
                                        errorMessage = 'Endpoint tidak ditemukan';
                                    } else if (xhr.status === 403) {
                                        errorMessage = 'Akses ditolak';
                                    }
                                }
                                
                                // Tampilkan error alert
                                Swal.fire({
                                    title: 'Error!',
                                    text: errorMessage,
                                    icon: 'error',
                                    confirmButtonText: 'OK'
                                });
                                
                                // Kembalikan switch ke posisi sebelumnya
                                $(`#switch_campaign_${value.campaign_id}`).prop('checked', !$(`#switch_campaign_${value.campaign_id}`).prop('checked'));
                            }
                        });    
                    }); // Delay 1 detik sebelum mengirim permintaan
                });
            })
            $.each(data_per_account.total_per_account, function (index, value) {
                // Budget
                const budget = Number(value?.total_budget) || 0;
                const totalBudget = budget.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Spend
                const spend = Number(value?.total_spend) || 0;
                const totalSpend = spend.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Impressions
                const impressions = Number(value?.total_impressions) || 0;
                const totalImpressions = impressions.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Reach
                const reach = Number(value?.total_reach) || 0;
                const totalReach = reach.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Clicks
                const clicks = Number(value?.total_click) || 0;
                const totalClicks = clicks.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Frequency
                const frequency = Number(value?.total_frequency) || 0;
                const totalFrequency = frequency.toFixed(2).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + ' %';
                // CPR
                let data_cpr = value.total_cpr;
                let cpr_number = parseFloat(data_cpr)
                let totalCpr = cpr_number.toFixed(0).replace(',', '.');
                $('#total_budget').text(totalBudget);
                $('#total_spend').text(totalSpend);
                $('#total_impressions').text(totalImpressions);
                $('#total_reach').text(totalReach);
                $('#total_clicks').text(totalClicks);
                $('#total_frequency').text(totalFrequency);
                $('#total_cpr').text(totalCpr);
            })
            var table = $('#table_data_per_account_facebook').DataTable({  
                "paging": true,
                "pageLength": 50,
                "lengthChange": true,
                "searching": true,
                "ordering": true,
                responsive: false,
                dom: 'Blfrtip',
                searching: true,
                buttons: [
                    {
                        extend: 'excel',
                        filename: judul,
                        text: 'Download Excel',
                        title: judul,
                        messageTop: "laporan traffic per account facebook didownload pada "
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()+" "
                                    +tanggal.getDate()+"-"
                                    +(tanggal.getMonth()+1)+"-"
                                    +tanggal.getFullYear(),
                        exportOptions: {
                            columns: ':visible', 
                            columns: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],      // hanya kolom yang terlihat
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
                            const colWidths = [5, 12, 13, 8, 12, 10, 10, 10, 10, 10, 10, 10]; // 💡 Sesuaikan berdasarkan % di HTML
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
                        messageBottom: "laporan traffic per account facebook didownload pada "
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
                                    if (body[i][0]) body[i][0].alignment = 'center';  // No
                                    if (body[i][1]) body[i][1].alignment = 'left';    // Account Name
                                    if (body[i][2]) body[i][2].alignment = 'left';    // Campaign Name
                                    if (body[i][3]) body[i][3].alignment = 'center';  // Status
                                    if (body[i][4]) body[i][4].alignment = 'center';  // Tanggal Mulai
                                    if (body[i][5]) body[i][5].alignment = 'right';   // Daily Budget
                                    if (body[i][6]) body[i][6].alignment = 'right';   // Spend
                                    if (body[i][7]) body[i][7].alignment = 'right';   // Impressions
                                    if (body[i][8]) body[i][8].alignment = 'right';   // Reach
                                    if (body[i][9]) body[i][9].alignment = 'right';   // Clicks
                                    if (body[i][10]) body[i][10].alignment = 'right'; // Frequency
                                    if (body[i][11]) body[i][11].alignment = 'right'; // CPR
                                }
                            }
                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]
                            // Manual width sesuai presentase kolom HTML (tanpa kolom terakhir)
                            doc.content[1].table.widths = ['3%', '12%', '13%', '8%', '9%', '12%', '9%', '9%', '9%', '9%', '9%', '8%'];
                        }
                    }
                ]
            });
            
            // FixedColumns removed as requested
            
            // Update header switch based on campaign statuses
             window.updateHeaderSwitch = function() {
                  const campaignSwitches = $('input[id^="switch_campaign_"]');
                  const totalCampaigns = campaignSwitches.length;
                  const activeCampaigns = campaignSwitches.filter(':checked').length;
                  const headerSwitch = $('#headerSwitch');
                 
                 if (totalCampaigns === 0) {
                     headerSwitch.prop('checked', false);
                     return;
                 }
                 
                 // Logic: active if all active, or if inactive campaigns are less than half
                 const inactiveCampaigns = totalCampaigns - activeCampaigns;
                 const shouldBeActive = activeCampaigns === totalCampaigns || inactiveCampaigns < (totalCampaigns / 2);
                 
                 headerSwitch.prop('checked', shouldBeActive);
             };
            
            // Toggle all campaigns based on header switch
            window.toggleAllCampaigns = function() {
                const headerSwitch = $('#headerSwitch');
                const isChecked = headerSwitch.is(':checked');
                const action = isChecked ? 'mengaktifkan' : 'menonaktifkan';
                
                if (confirm(`Apakah Anda yakin ingin ${action} semua campaign yang ditampilkan?`)) {
                    const campaignIds = [];
                    const targetStatus = isChecked ? 'ACTIVE' : 'PAUSED';
                    
                    // Get all campaign IDs from the form-switch elements
                    $('input[id^="switch_campaign_"]').each(function() {
                        const switchId = $(this).attr('id');
                        const campaignId = switchId.replace('switch_campaign_', '');
                        campaignIds.push(campaignId);
                    });
                    
                    if (campaignIds.length > 0) {
                        bulkUpdateCampaignStatus(campaignIds, targetStatus);
                    } else {
                        alert('Tidak ada campaign yang ditemukan.');
                        // Revert header switch if no campaigns found
                        headerSwitch.prop('checked', !isChecked);
                    }
                } else {
                    // Revert header switch if user cancels
                    headerSwitch.prop('checked', !isChecked);
                }
            };
            
            // Fungsi untuk menampilkan/menyembunyikan loading indicator dengan SweetAlert2
            function showBulkUpdateLoader(show) {
                if (show) {
                    // Tampilkan SweetAlert loading
                    Swal.fire({
                        title: 'Mengupdate Status Campaign',
                        html: 'Mohon tunggu, proses sedang berjalan...',
                        icon: 'info',
                        allowOutsideClick: false,
                        allowEscapeKey: false,
                        showConfirmButton: false,
                        didOpen: () => {
                            Swal.showLoading();
                        }
                    });
                } else {
                    // Tutup SweetAlert loading
                    Swal.close();
                }
            }
            
            // Call updateHeaderSwitch multiple times to ensure it works
            setTimeout(function() {
                updateHeaderSwitch();
            }, 300);
            
            setTimeout(function() {
                updateHeaderSwitch();
            }, 800);
            
            setTimeout(function() {
                updateHeaderSwitch();
            }, 1500);
            
            // Update header switch after table is loaded and on any switch change
             table.on('draw', function() {
                 // Use setTimeout to ensure DOM is fully updated
                 setTimeout(function() {
                     updateHeaderSwitch();
                     
                     // Add event listeners to individual campaign switches
                     $('input[id^="switch_campaign_"]').off('change.headerUpdate').on('change.headerUpdate', function() {
                         updateHeaderSwitch();
                     });
                 }, 100);
             });
             
             // Initial update after table initialization
             setTimeout(function() {
                 updateHeaderSwitch();
             }, 500);
            
            function bulkUpdateCampaignStatus(campaignIds, status) {
                const data_account = $("#select_account option:selected").val();
                
                // Validasi parameter sebelum mengirim request
                if (!campaignIds || campaignIds.length === 0) {
                    alert('Tidak ada campaign yang ditemukan untuk diupdate.');
                    return;
                }
                
                if (!status || (status !== 'ACTIVE' && status !== 'PAUSED')) {
                    alert('Status tidak valid.');
                    return;
                }
                
                // Jika tidak ada account yang dipilih, gunakan '%' sebagai default
                const accountId = data_account || '%';
                
                // Tampilkan loading indicator
                showBulkUpdateLoader(true);
                
                $.ajax({
                    url: '/management/admin/facebook_ads/bulk_update_campaign_status/',
                    type: 'POST',
                    data: {
                        'account_id': accountId,
                        'campaign_ids': JSON.stringify(campaignIds),
                        'status': status,
                        'csrfmiddlewaretoken': csrftoken
                    },
                    success: function(response) {
                        // Sembunyikan loading indicator
                        showBulkUpdateLoader(false);
                        
                        if (response.success) {
                            // Show success message with SweetAlert2
                            Swal.fire({
                                title: 'Berhasil!',
                                text: response.message || 'Status campaign berhasil diupdate!',
                                icon: 'success',
                                timer: 1500,
                                showConfirmButton: false
                            });
                            
                            // Refresh the table
                            setTimeout(function() {
                                var tanggal = $("#tanggal").val();
                                var data_sub_domain = $("#select_sub_domain option:selected").val();
                                table_data_per_account_facebook(data_account, tanggal, data_sub_domain);
                                // Update header switch after bulk update
                                updateHeaderSwitch();
                            }, 1000);
                        } else {
                            Swal.fire({
                                title: 'Error!',
                                text: response.message || 'Gagal mengupdate status campaign',
                                icon: 'error',
                                confirmButtonText: 'OK'
                            });
                        }
                    },
                    error: function(xhr, status, error) {
                        // Sembunyikan loading indicator
                        showBulkUpdateLoader(false);
                        
                        let errorMessage = 'Terjadi kesalahan saat mengupdate status campaign';
                        
                        // Coba parse response JSON untuk mendapatkan pesan error yang lebih spesifik
                        try {
                            const response = JSON.parse(xhr.responseText);
                            if (response.message) {
                                errorMessage = response.message;
                            }
                        } catch (e) {
                            // Jika tidak bisa parse JSON, gunakan pesan default
                            if (xhr.status === 500) {
                                errorMessage = 'Terjadi kesalahan server internal';
                            } else if (xhr.status === 404) {
                                errorMessage = 'Endpoint tidak ditemukan';
                            } else if (xhr.status === 403) {
                                errorMessage = 'Akses ditolak';
                            }
                        }
                        
                        Swal.fire({
                            title: 'Error!',
                            text: errorMessage,
                            icon: 'error',
                            confirmButtonText: 'OK'
                        });
                    }
                });
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

function destroy_table_data_per_account_facebook(){
    if ($.fn.DataTable.isDataTable('#table_data_per_account_facebook')) {
        $('#table_data_per_account_facebook').DataTable().clear().destroy();
    }
    $('#table_data_per_account_facebook tbody').empty();
}

