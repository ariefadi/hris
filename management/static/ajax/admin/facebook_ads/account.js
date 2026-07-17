/**
 * Reference Ajax Admin Data User Js
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
    $('#select_user_st_add').select2({
        dropdownParent: $('#add_user'),
        placeholder: '-- Pilih Status --',
        allowClear: true,
        width: '100%',
        minimumResultsForSearch: 0
    })
    // default reload data
    table_data_account_ads();

    $(document).on('click', '.btn-delete-account', function (e) {
        e.preventDefault();
        var accountAdsId = $(this).data('accountAdsId');
        var accountName = $(this).data('accountName') || '';

        Swal.fire({
            icon: 'warning',
            title: 'Hapus Account?',
            text: 'Hapus account "' + accountName + '"?',
            showCancelButton: true,
            confirmButtonText: 'Ya, hapus',
            cancelButtonText: 'Batal'
        }).then(function (result) {
            if (!result.isConfirmed) return;

            var formData = new FormData();
            formData.append('account_ads_id', accountAdsId);

            $.ajax({
                type: 'POST',
                url: '/management/admin/delete_account_facebook',
                data: formData,
                headers: {
                    "X-CSRFToken": csrftoken
                },
                processData: false,
                contentType: false,
                dataType: 'json',
                beforeSend: function () {
                    $('#overlay').fadeIn(200);
                },
                success: function (resp) {
                    $('#overlay').fadeOut(200);
                    if (resp && resp.status === true) {
                        Swal.fire({
                            icon: 'success',
                            title: 'Berhasil',
                            text: resp.message || 'Account berhasil dihapus'
                        }).then(function () {
                            location.reload();
                        });
                    } else {
                        Swal.fire({
                            icon: 'error',
                            title: 'Gagal',
                            text: (resp && resp.message) ? resp.message : 'Gagal menghapus account'
                        });
                    }
                },
                error: function (xhr, status, error) {
                    $('#overlay').fadeOut(200);
                    report_eror(xhr, status);
                }
            });
        });
    });

    $(document).on('click', '.btn-fb-token-check', function(e) {
        e.preventDefault();
        var accountAdsId = $(this).data('accountAdsId');
        if (!accountAdsId) return;
        var btn = $(this);
        btn.prop('disabled', true);
        checkFacebookToken(accountAdsId, false).always(function() {
            btn.prop('disabled', false);
        });
    });

    $(document).on('click', '.btn-fb-token-extend', function(e) {
        e.preventDefault();
        extendFacebookToken($(this).data('accountAdsId'));
    });

    $(document).on('click', '.btn-fb-token-oauth', function(e) {
        e.preventDefault();
        var accountAdsId = $(this).data('accountAdsId');
        var accountName = $(this).closest('tr').find('td').eq(1).text().trim();
        reauthorizeFacebookToken(accountAdsId, accountName);
    });

    $(document).on('click', '.btn-fb-token-partner', function(e) {
        e.preventDefault();
        requestFacebookPartnerToken($(this).data('accountAdsId'), $(this).closest('tr').find('td').eq(1).text().trim());
    });

    $('#btnCheckAllFacebookTokens').on('click', function(e) {
        e.preventDefault();
        checkAllFacebookTokens();
    });
});
function table_data_account_ads() {
    $.ajax({
        url: '/management/admin/page_account_facebook',
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_account_ads) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Account Facebook Ads"
            $.each(data_account_ads.data_account_ads, function (index, value) {
                var url_detail = '/management/admin/edit_account_facebook/'+value.account_ads_id;
                var isWarning = !(Number(value.total_data || 0) > 0);
                var event_data = `<tr${isWarning ? ' class="table-warning"' : ''}>`;
                event_data += '<td class="text-center">' + (index + 1) + '</td>';
                event_data += '<td class="text-left">' + value.account_name + '</td>';
                event_data += '<td class="text-left">' + value.account_email + '</td>';
                event_data += '<td class="text-center">' + value.account_id + '</td>';
                event_data += '<td class="text-center">' + value.app_id + '</td>';
                event_data += '<td class="text-left">' + value.pemilik_account + '</td>';
                event_data += `
                                    <td class="text-center">
                                        ${!isWarning
                                            ? '<i class="bi bi-check-circle-fill"></i>'
                                            : '<i class="bi bi-exclamation-octagon"></i>'}
                                    </td>
                                `;
                event_data += '<td class="text-center">' + renderTokenCell(value.account_ads_id, {
                    status: 'unknown',
                    label: 'Belum dicek',
                    can_reauthorize: true,
                    owner_name: value.pemilik_account || ''
                }) + '</td>';

                event_data += '<td class="text-center">'
                    + '<div class="btn-group" role="group">'
                    + '<a href="' + url_detail + '" type="button" class="btn btn-success btn-xs"><i class="bi bi-pencil"></i></a>'
                    + '<button type="button" class="btn btn-warning btn-xs btn-delete-account" data-account-ads-id="' + value.account_ads_id + '" data-account-name="' + (value.account_name || '') + '"><i class="bi bi-trash"></i></button>'
                    + '</div>'
                    + '</td>';
                event_data += '</tr>';  
                $("#table_data_account tbody").append(event_data);    
            })
            $('#table_data_account').DataTable({
                responsive: false,
                autoWidth: false,  
                "paging": true,
                "lengthChange": true,
                "searching": true,
                "ordering": true,
                dom: 'Bfrtip',
                searching: true,
                buttons: [
                    {
                        extend: 'excel',
                        filename: judul,
                        text: 'Download Excel',
                        title: judul,
                        messageTop: "laporan user didownload pada "
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()+" "
                                    +tanggal.getDate()+"-"
                                    +(tanggal.getMonth()+1)+"-"
                                    +tanggal.getFullYear(),
                        exportOptions: {
                            columns: [0, 1, 2, 3, 4, 5, 6],
                            modifier: {
                                search: 'applied',
                                order: 'applied'
                            }
                        },
                        customize: function (xlsx) {
                            const sheet = xlsx.xl.worksheets['sheet1.xml'];
                            // =========================
                            // Set column width secara manual (unit: character width)
                            // =========================
                            const colWidths = [5, 25, 15, 15, 15, 20, 8];
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
                        messageBottom: "laporan user didownload pada "
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()
                                    +" "+tanggal.getDate()
                                    +"-"+(tanggal.getMonth()+1)
                                    +"-"+tanggal.getFullYear(),
                        exportOptions: {
                            columns: ':not(:last-child)' // ❌ Kecualikan kolom terakhir
                        },
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
                                // Center kolom ke-1 (index 0) dan kolom ke-3 (index 2)
                                body[i][0].alignment = 'center';
                                body[i][1].alignment = 'left';
                                body[i][2].alignment = 'center';
                                body[i][3].alignment = 'center';
                                body[i][4].alignment = 'center';
                                body[i][5].alignment = 'left';
                            }

                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]

                            // Manual width sesuai presentase kolom HTML (tanpa kolom terakhir)
                            doc.content[1].table.widths = ['5%', '22%', '14%', '14%', '14%', '18%', '8%'];
                        }
                    }
                ]
            });
            try { $('#table_data_account').DataTable().columns.adjust(); } catch (e) {}
        }
    });
}

function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

function getCsrfToken() {
    var input = document.querySelector('input[name="csrfmiddlewaretoken"]');
    if (input && input.value) {
        return input.value;
    }
    return getCookie('csrftoken');
}

const csrftoken = getCsrfToken();
window.fbTokenCache = window.fbTokenCache || {};

function escHtml(value) {
    return String(value == null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function renderTokenOwnerHint(info) {
    info = info || {};
    var needsOwner = info.expiry_reason === 'data_access'
        || info.status === 'expired'
        || (info.status === 'expiring_soon' && info.expiry_reason !== 'token');
    if (!needsOwner) return '';
    var owner = String(info.owner_name || '').trim();
    var ownerText = owner ? ('Hubungi <strong>' + escHtml(owner) + '</strong>') : 'Hubungi <strong>Pemilik Account</strong>';
    return '<small class="fb-token-owner-hint"><i class="fas fa-user-shield mr-1"></i>'
        + ownerText
        + ' untuk Authorize Ulang atau kirim token baru via Edit Account.</small>';
}

function renderTokenBadge(info) {
    info = info || {};
    var st = info.status || 'unknown';
    var html = '<span class="fb-token-badge is-' + escHtml(st) + '">' + escHtml(info.label || 'Belum dicek') + '</span>';
    if (info.expires_label) {
        html += '<small class="fb-token-meta">Exp: ' + escHtml(info.expires_label) + '</small>';
    }
    if (info.data_access_expires_label && info.data_access_expires_label !== info.expires_label) {
        html += '<small class="fb-token-meta">Data: ' + escHtml(info.data_access_expires_label) + '</small>';
    }
    if (info.message) {
        html += '<small class="fb-token-meta">' + escHtml(info.message) + '</small>';
    }
    html += renderTokenOwnerHint(info);
    return html;
}

function renderTokenActions(accountAdsId, info) {
    info = info || {};
    var html = '';
    html += '<button type="button" class="btn btn-outline-primary btn-xs btn-fb-token-check" data-account-ads-id="' + escHtml(accountAdsId) + '" title="Cek token"><i class="fas fa-sync-alt"></i></button>';
    if (info.can_extend) {
        html += '<button type="button" class="btn btn-outline-success btn-xs btn-fb-token-extend" data-account-ads-id="' + escHtml(accountAdsId) + '" title="Perpanjang token"><i class="fas fa-clock"></i></button>';
    }
    if (info.can_reauthorize !== false) {
        html += '<button type="button" class="btn btn-outline-warning btn-xs btn-fb-token-oauth" data-account-ads-id="' + escHtml(accountAdsId) + '" title="Authorize ulang"><i class="fas fa-key"></i></button>';
    }
    html += '<button type="button" class="btn btn-outline-info btn-xs btn-fb-token-partner" data-account-ads-id="' + escHtml(accountAdsId) + '" title="Minta token via Partner BM"><i class="fas fa-paper-plane"></i></button>';
    return html;
}

function renderTokenCell(accountAdsId, info) {
    info = info || { status: 'unknown', label: 'Belum dicek', can_reauthorize: true };
    window.fbTokenCache[accountAdsId] = info;
    return '<div class="fb-token-cell" data-account-ads-id="' + escHtml(accountAdsId) + '">'
        + '<div class="fb-token-cell__status">' + renderTokenBadge(info) + '</div>'
        + '<div class="fb-token-cell__actions">' + renderTokenActions(accountAdsId, info) + '</div>'
        + '</div>';
}

function mergeTokenInfoWithRowOwner(accountAdsId, info) {
    var merged = Object.assign({}, info || {});
    if (merged.owner_name) return merged;
    var cell = document.querySelector('.fb-token-cell[data-account-ads-id="' + accountAdsId + '"]');
    if (!cell) return merged;
    var row = cell.closest('tr');
    if (!row) return merged;
    var ownerCell = row.querySelector('td:nth-child(6)');
    if (ownerCell) {
        merged.owner_name = String(ownerCell.textContent || '').trim();
    }
    return merged;
}

function updateTokenCell(accountAdsId, info) {
    info = mergeTokenInfoWithRowOwner(accountAdsId, info || {});
    window.fbTokenCache[accountAdsId] = info;
    var cell = document.querySelector('.fb-token-cell[data-account-ads-id="' + accountAdsId + '"]');
    if (!cell) return;
    var statusEl = cell.querySelector('.fb-token-cell__status');
    var actionsEl = cell.querySelector('.fb-token-cell__actions');
    if (statusEl) statusEl.innerHTML = renderTokenBadge(info);
    if (actionsEl) actionsEl.innerHTML = renderTokenActions(accountAdsId, info);
}

function checkFacebookToken(accountAdsId, silent) {
    var formData = new FormData();
    formData.append('account_ads_id', accountAdsId);
    return $.ajax({
        type: 'POST',
        url: '/management/admin/facebook_account_token_check',
        data: formData,
        headers: { 'X-CSRFToken': csrftoken },
        processData: false,
        contentType: false,
        dataType: 'json'
    }).then(function(resp) {
        if (!resp || !resp.status) {
            throw new Error((resp && resp.message) ? resp.message : 'Gagal memeriksa token');
        }
        updateTokenCell(accountAdsId, resp.data || {});
        if (!silent) {
            Swal.fire({
                icon: (resp.data && resp.data.is_valid) ? 'success' : 'warning',
                title: 'Status Token',
                text: (resp.data && resp.data.label) ? resp.data.label : 'Selesai'
            });
        }
        return resp;
    });
}

function extendFacebookToken(accountAdsId) {
    var info = (window.fbTokenCache && window.fbTokenCache[accountAdsId]) ? window.fbTokenCache[accountAdsId] : {};
    if (info && info.expiry_reason === 'data_access') {
        var ownerLine = info.owner_name
            ? ('Minta <strong>' + escHtml(info.owner_name) + '</strong> (Pemilik Account) yang login Facebook untuk Authorize Ulang.')
            : 'Minta <strong>Pemilik Account</strong> yang login Facebook untuk Authorize Ulang.';
        Swal.fire({
            icon: 'info',
            title: 'Perlu Authorize Ulang',
            html: 'Status <strong>Akses data segera habis</strong> artinya izin akses data Facebook akan habis'
                + (info.data_access_expires_label ? ' pada <strong>' + escHtml(info.data_access_expires_label) + '</strong>' : '')
                + '.<br><br>Tombol <strong>Perpanjang Token</strong> tidak memperbarui izin data ini.<br>'
                + ownerLine
                + '<br><br>Alternatif: pemilik akun kirim <em>Access Token</em> baru, lalu paste di halaman <strong>Edit Account</strong>.'
        });
        return;
    }
    if (info && info.can_extend === false) {
        Swal.fire({
            icon: 'warning',
            title: 'Tidak Bisa Diperpanjang',
            text: (info.message || 'Token tidak bisa diperpanjang.')
                + (info.status === 'expired'
                    ? ' Gunakan Authorize Ulang (ikon kunci) untuk login ulang ke Facebook.'
                    : ' Coba Authorize Ulang jika perpanjangan otomatis gagal.')
        });
        return;
    }
    Swal.fire({
        icon: 'question',
        title: 'Perpanjang Token?',
        text: 'Token yang masih valid akan ditukar ke long-lived token.',
        showCancelButton: true,
        confirmButtonText: 'Ya, perpanjang',
        cancelButtonText: 'Batal'
    }).then(function(result) {
        if (!result.isConfirmed) return;
        var formData = new FormData();
        formData.append('account_ads_id', accountAdsId);
        $.ajax({
            type: 'POST',
            url: '/management/admin/facebook_account_token_extend',
            data: formData,
            headers: { 'X-CSRFToken': csrftoken },
            processData: false,
            contentType: false,
            dataType: 'json',
            beforeSend: function() { $('#overlay').fadeIn(200); },
            success: function(resp) {
                $('#overlay').fadeOut(200);
                if (resp && resp.status) {
                    updateTokenCell(accountAdsId, resp.data || {});
                    var data = resp.data || {};
                    var icon = data.status === 'valid' ? 'success' : 'warning';
                    var title = data.status === 'valid' ? 'Berhasil' : 'Token Diperpanjang';
                    Swal.fire({ icon: icon, title: title, text: resp.message || 'Token diperpanjang' });
                } else {
                    var errMsg = (resp && resp.message) ? resp.message : 'Gagal memperpanjang token';
                    if (/expired|authorize|login/i.test(errMsg)) {
                        errMsg += ' Gunakan Authorize Ulang (ikon kunci) jika token sudah expired.';
                    }
                    Swal.fire({ icon: 'error', title: 'Gagal', text: errMsg });
                }
            },
            error: function(xhr) {
                $('#overlay').fadeOut(200);
                var msg = 'Gagal memperpanjang token';
                try {
                    var json = xhr.responseJSON;
                    if (json && json.message) msg = json.message;
                } catch (e) {}
                Swal.fire({ icon: 'error', title: 'Gagal', text: msg });
            }
        });
    });
}

function reauthorizeFacebookToken(accountAdsId, accountName) {
    var redirectUri = '';
    var meta = document.getElementById('fbOAuthMeta');
    if (meta) {
        redirectUri = meta.getAttribute('data-redirect-uri') || '';
    }
    var accountLabel = String(accountName || accountAdsId || '').trim() || accountAdsId;
    var ownerName = '';
    var row = document.querySelector('.btn-fb-token-oauth[data-account-ads-id="' + accountAdsId + '"]');
    if (row) {
        var tr = row.closest('tr');
        if (tr) {
            var ownerCell = tr.querySelector('td:nth-child(6)');
            if (ownerCell) ownerName = String(ownerCell.textContent || '').trim();
        }
    }
    var ownerHint = ownerName
        ? ('Jika Anda tidak punya akses Facebook, minta <strong>' + escHtml(ownerName) + '</strong> (Pemilik Account) yang melakukan langkah ini, atau mengirim Access Token baru untuk di-paste di <strong>Edit Account</strong>.<br><br>')
        : ('Jika Anda tidak punya akses Facebook, minta <strong>Pemilik Account</strong> yang melakukan langkah ini, atau mengirim Access Token baru untuk di-paste di <strong>Edit Account</strong>.<br><br>');

    Swal.fire({
        icon: 'info',
        title: 'Authorize Ulang Token?',
        html: '<p style="font-size:14px;margin:0 0 10px;">Account: <strong>' + escHtml(accountLabel) + '</strong></p>'
            + '<p style="font-size:13px;text-align:left;margin:0 0 8px;">' + ownerHint
            + 'Jika Anda punya akses Facebook, Anda akan dialihkan ke halaman login Facebook untuk mendapatkan access token baru.</p>'
            + '<p style="font-size:13px;text-align:left;margin:0 0 6px;"><strong>Supaya tidak muncul error &quot;Aplikasi tidak aktif&quot;:</strong></p>'
            + '<ol style="text-align:left;font-size:12px;margin:0;padding-left:18px;">'
            + '<li>Buka <a href="https://developers.facebook.com/apps" target="_blank" rel="noopener">Meta Developer Console</a> &rarr; pilih App ID account ini</li>'
            + '<li>Pastikan app <b>aktif</b> (Live) atau akun Facebook Anda ditambahkan sebagai <b>Developer/Tester</b> di menu App Roles</li>'
            + '<li>Daftarkan Redirect URI ini di <b>Facebook Login &gt; Settings</b>:<br><code style="font-size:11px;word-break:break-all;display:block;margin-top:4px;">' + escHtml(redirectUri) + '</code></li>'
            + '</ol>',
        showCancelButton: true,
        confirmButtonText: 'Lanjut ke Facebook',
        cancelButtonText: 'Batal',
        width: 620,
        customClass: { htmlContainer: 'text-left' }
    }).then(function(result) {
        if (!result.isConfirmed) return;
        window.location.href = '/management/admin/facebook_account_oauth_start?account_ads_id=' + encodeURIComponent(accountAdsId);
    });
}

function requestFacebookPartnerToken(accountAdsId, accountName) {
    if (!accountAdsId) return;
    var accountLabel = String(accountName || accountAdsId || '').trim() || accountAdsId;
    Swal.fire({
        icon: 'question',
        title: 'Minta Token ke Partner BM?',
        html: '<p style="font-size:13px;text-align:left;">HRIS akan mengirim <strong>webhook</strong> ke partner BM (jika URL sudah diset di <code>.env</code>) '
            + 'dan menampilkan <code>request_token</code> untuk partner submit token via API.</p>'
            + '<p style="font-size:13px;text-align:left;margin:0;">Account: <strong>' + escHtml(accountLabel) + '</strong></p>',
        showCancelButton: true,
        confirmButtonText: 'Kirim Permintaan',
        cancelButtonText: 'Batal',
        width: 620,
        customClass: { htmlContainer: 'text-left' }
    }).then(function(result) {
        if (!result.isConfirmed) return;
        var formData = new FormData();
        formData.append('account_ads_id', accountAdsId);
        $.ajax({
            type: 'POST',
            url: '/management/admin/facebook_partner_token_request',
            data: formData,
            headers: { 'X-CSRFToken': csrftoken },
            processData: false,
            contentType: false,
            dataType: 'json',
            beforeSend: function() { $('#overlay').fadeIn(200); },
            success: function(resp) {
                $('#overlay').fadeOut(200);
                if (!resp || !resp.status || !resp.data) {
                    Swal.fire({ icon: 'error', title: 'Gagal', text: (resp && resp.message) ? resp.message : 'Gagal membuat permintaan partner.' });
                    return;
                }
                var d = resp.data;
                var webhookMsg = (d.webhook && d.webhook.message) ? d.webhook.message : 'Webhook tidak dikirim.';
                var curlExample = 'curl -X POST "' + escHtml(d.submit_url) + '" \\\n'
                    + '  -H "Content-Type: application/json" \\\n'
                    + '  -H "X-API-Key: YOUR_PARTNER_API_KEY" \\\n'
                    + '  -d \'{"request_token":"' + escHtml(d.request_token) + '","access_token":"EAAG...","submitted_by":"partner-bm"}\'';
                Swal.fire({
                    icon: 'success',
                    title: 'Permintaan Partner Dibuat',
                    html: '<p style="font-size:12px;text-align:left;"><strong>Webhook:</strong> ' + escHtml(webhookMsg) + '</p>'
                        + '<p style="font-size:12px;text-align:left;"><strong>Submit URL:</strong><br><code style="word-break:break-all;">' + escHtml(d.submit_url) + '</code></p>'
                        + '<p style="font-size:12px;text-align:left;"><strong>Request Token</strong> (berlaku sampai ' + escHtml(d.expires_at_label || '-') + '):</p>'
                        + '<textarea readonly class="form-control" rows="3" style="font-size:11px;">' + escHtml(d.request_token) + '</textarea>'
                        + '<p style="font-size:12px;text-align:left;margin-top:10px;"><strong>Contoh curl untuk Partner BM:</strong></p>'
                        + '<pre style="font-size:10px;text-align:left;white-space:pre-wrap;max-height:160px;overflow:auto;">' + curlExample + '</pre>',
                    width: 760,
                    customClass: { htmlContainer: 'text-left' }
                });
            },
            error: function(xhr, status, error) {
                $('#overlay').fadeOut(200);
                report_eror(xhr, status);
            }
        });
    });
}

function checkAllFacebookTokens(silent) {
    var formData = new FormData();
    var btn = document.getElementById('btnCheckAllFacebookTokens');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Memuat...';
    }
    $.ajax({
        type: 'POST',
        url: '/management/admin/facebook_account_token_check_all',
        data: formData,
        headers: { 'X-CSRFToken': csrftoken },
        processData: false,
        contentType: false,
        dataType: 'json',
        beforeSend: function() { if (!silent) $('#overlay').fadeIn(200); },
        success: function(resp) {
            if (!silent) $('#overlay').fadeOut(200);
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i class="fas fa-sync-alt"></i> Cek Semua';
            }
            if (!resp || !resp.status) {
                if (!silent) {
                    Swal.fire({ icon: 'error', title: 'Gagal', text: (resp && resp.message) ? resp.message : 'Gagal memeriksa token' });
                }
                return;
            }
            var items = (resp.data && resp.data.items) ? resp.data.items : [];
            items.forEach(function(item) {
                updateTokenCell(item.account_ads_id, item.token || {});
            });
            if (silent) return;
            var summary = (resp.data && resp.data.summary) ? resp.data.summary : {};
            Swal.fire({
                icon: 'info',
                title: 'Cek Token Selesai',
                html: 'Valid: <b>' + (summary.valid || 0) + '</b><br>'
                    + 'Segera expired: <b>' + (summary.expiring_soon || 0) + '</b><br>'
                    + 'Expired: <b>' + (summary.expired || 0) + '</b><br>'
                    + 'Kosong/Error: <b>' + ((summary.missing || 0) + (summary.error || 0)) + '</b>'
            });
        },
        error: function(xhr, status, error) {
            if (!silent) $('#overlay').fadeOut(200);
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i class="fas fa-sync-alt"></i> Cek Semua';
            }
            if (!silent) report_eror(xhr, status);
        }
    });
}

$('#simpan_data_account').on('click',function(e){
    var account_name = $("#account_name").val();
    var account_email =  $("#account_email").val();
    var account_id =  $("#account_id").val();
    var app_id =  $("#app_id").val();
    var app_secret =  $("#app_secret").val();
    var access_token =  $("#access_token").val();
    // alert(select_unit)
    let formData = new FormData();
    if( account_name!="" && account_email!="" && account_id!="" && app_id!="") {
        formData.append('account_name',account_name);
        formData.append('account_email',account_email);
        formData.append('account_id',account_id);
        formData.append('app_id',app_id);
        formData.append('app_secret',app_secret);
        formData.append('access_token',access_token);
        $.ajax({
            type: 'POST',
            url: '/management/admin/post_account_ads',
            data: formData,
            headers: { 
                "X-CSRFToken": csrftoken 
            },
            processData: false,
            contentType: false,
            dataType: "json",
            success: function (data) {
                if (data.status === true) {
                    Swal.fire({
                        icon: 'success',
                        title: 'Berhasil',
                        text: data.message,
                    }).then(() => {
                        location.reload();
                    });
                } else {
                    Swal.fire({
                        icon: 'error',
                        title: 'Gagal',
                        text: data.message,
                    }).then(() => {
                        location.reload();
                    });
                }
            }
        })
    }
})