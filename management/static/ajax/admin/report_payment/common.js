/**
 * Shared Payment AdSense / AdX page.
 */
(function () {
    'use strict';

    var cfg = window.HRIS_PAYMENT_PAGE || {};
    var payTable = null;
    var txnTable = null;
    var invalidTable = null;
    var invalidDomainTable = null;
    var accountAdsTable = null;

    function escapeHtml(text) {
        return String(text == null ? '' : text)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function formatMoney(value, currency) {
        var num = parseFloat(value);
        if (isNaN(num)) num = 0;
        var code = String(currency || 'IDR').toUpperCase();
        if (!code || code === 'IDR') {
            return 'Rp ' + Math.round(num).toLocaleString('id-ID');
        }
        return code + ' ' + num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function originalMoneyHint(row) {
        if (!row || !row.fx_converted) return '';
        return '<br><small class="pay-muted">' + formatMoney(row.amount_original, row.currency_original) + ' × ' + escapeHtml(String(row.fx_rate || '')) + '</small>';
    }

    function selectedAccounts() {
        var values = [];
        var el = document.getElementById('account_filter');
        if (!el) return '';
        if (window.$ && $(el).val) {
            values = $(el).val() || [];
        } else {
            values = Array.prototype.slice.call(el.selectedOptions || []).map(function (opt) { return opt.value; });
        }
        return (values || []).filter(Boolean).join(',');
    }

    function queryParams() {
        return $.param({
            start_date: $('#tanggal_dari').val() || '',
            end_date: $('#tanggal_sampai').val() || '',
            selected_account: selectedAccounts(),
            status: $('#status_filter').val() || '',
            policy_only: $('#policy_filter').val() || '0'
        });
    }

    function categoryPill(category) {
        var key = String(category || 'other');
        var label = key.replace(/_/g, ' ');
        var cls = 'is-other';
        if (key === 'invalid_activity' || key === 'policy') cls = 'is-invalid';
        else if (key === 'adjustment') cls = 'is-adjust';
        else if (key === 'paid' || key === 'payment' || key === 'earning') cls = 'is-paid';
        else if (key === 'unpaid') cls = 'is-unpaid';
        return '<span class="pay-pill ' + cls + '">' + escapeHtml(label) + '</span>';
    }

    function gapHtml(value) {
        if (value === null || value === undefined || value === '') return '-';
        var num = parseFloat(value);
        if (isNaN(num)) return '-';
        var cls = num > 0.009 ? 'pay-num-down' : (num < -0.009 ? 'pay-num-up' : '');
        return '<span class="' + cls + '">' + formatMoney(num) + '</span>';
    }

    function destroyTable($table) {
        if ($table && $table.length && $.fn.DataTable && $.fn.DataTable.isDataTable($table)) {
            $table.DataTable().clear().destroy();
        }
    }

    function renderPayments(rows) {
        var $table = $('#table_payments');
        var $tb = $table.find('tbody');
        if (!$tb.length || $table.data('skipRender')) return;
        destroyTable($table);
        $tb.empty();
        (rows || []).forEach(function (row) {
            var period = '';
            if (row.earning_period_start && row.earning_period_end) {
                period = escapeHtml(row.earning_period_start) + ' s/d ' + escapeHtml(row.earning_period_end);
                if (row.estimated_revenue != null) {
                    period += '<br><small>' + formatMoney(row.estimated_revenue, row.currency) + '</small>';
                }
            } else {
                period = '-';
            }
            $tb.append(
                '<tr>' +
                    '<td>' + escapeHtml(row.account_name || row.account_id || '-') + '</td>' +
                    '<td>' + escapeHtml(row.publisher_account || '-') + '</td>' +
                    '<td>' + categoryPill(row.status) + '</td>' +
                    '<td>' + escapeHtml(row.channel || '-') + '</td>' +
                    '<td>' + escapeHtml(row.payment_date || '-') + '</td>' +
                    '<td>' + formatMoney(row.amount_value, 'IDR') + originalMoneyHint(row) + '</td>' +
                    '<td>' + period + '</td>' +
                    '<td>' + gapHtml(row.estimated_gap) + '</td>' +
                    '<td>' + escapeHtml(row.source_kind || '-') + '</td>' +
                '</tr>'
            );
        });
        if ($.fn.DataTable) {
            payTable = $table.DataTable({
                order: [[4, 'desc']],
                pageLength: 25,
                autoWidth: false
            });
        }
    }

    function renderTransactions(rows) {
        var $table = $('#table_transactions');
        var $tb = $table.find('tbody');
        if (!$tb.length) return;
        destroyTable($table);
        $tb.empty();
        (rows || []).forEach(function (row) {
            var amount = parseFloat(row.amount_value || 0);
            $tb.append(
                '<tr>' +
                    '<td>' + escapeHtml(row.txn_date || '-') + '</td>' +
                    '<td>' + escapeHtml(row.account_name || row.account_id || '-') + '</td>' +
                    '<td>' + categoryPill(row.category) + '</td>' +
                    '<td>' + escapeHtml(row.txn_type || '-') + '</td>' +
                    '<td>' + escapeHtml(row.description || '-') + '</td>' +
                    '<td class="' + (amount < 0 ? 'pay-num-down' : '') + '">' + formatMoney(amount, 'IDR') + originalMoneyHint(row) + '</td>' +
                    '<td>' + escapeHtml(row.invoice_no || '-') + '</td>' +
                    '<td>' + escapeHtml(row.source_file || '-') + '</td>' +
                '</tr>'
            );
        });
        if ($.fn.DataTable) {
            txnTable = $table.DataTable({
                order: [[0, 'desc']],
                pageLength: 25,
                autoWidth: false
            });
        }
    }

    function renderSummary(summary) {
        summary = summary || {};
        $('#sumUnpaid').text(formatMoney(summary.unpaid_total, 'IDR'));
        $('#sumPaid').text(formatMoney(summary.paid_total, 'IDR'));
        $('#sumDeduct').text(formatMoney(summary.deduction_total, 'IDR'));
        $('#sumCount').text((summary.payment_count || 0) + ' / ' + (summary.transaction_count || 0));
        var syncText = summary.last_sync || '-';
        if (summary.fx_note) {
            syncText += ' · IDR';
        }
        $('#payLastSync').text(syncText);
        if ($('#payFxNote').length) {
            $('#payFxNote').text(summary.fx_note || '');
        }
    }

    function showOverlay(show) {
        if (window.$) {
            if (show) $('#overlay').show();
            else $('#overlay').hide();
        }
    }

    function toast(ok, message) {
        if (window.Swal) {
            Swal.fire({
                icon: ok ? 'success' : 'error',
                title: ok ? 'Berhasil' : 'Gagal',
                text: message || '',
                timer: ok ? 2200 : undefined
            });
            return;
        }
        window.alert(message || (ok ? 'Berhasil' : 'Gagal'));
    }

    function currentTabHref() {
        if (!window.$) return '';
        return String($('.pay-page .nav-tabs .nav-link.active').attr('href') || '');
    }

    function showTransactionsTab() {
        var tab = currentTabHref();
        if (tab === '#tabInvalid' || tab === '#tabAccountAds' || tab === '#tabCompare' || tab === '#tabImportCsv' || tab === '#tabPayments') return;
        if (window.$) {
            $('a[href="#tabTransactions"]').tab('show');
        }
    }

    function matchPill(match) {
        var key = String(match || 'missing');
        if (key === 'ok') return '<span class="pay-pill is-paid">Cocok</span>';
        if (key === 'gap') return '<span class="pay-pill is-adjust">Ada selisih</span>';
        if (key === 'api_only') return '<span class="pay-pill is-other">API saja</span>';
        if (key === 'csv_only') return '<span class="pay-pill is-invalid">Hanya dari CSV</span>';
        return '<span class="pay-pill is-other">Belum ada data</span>';
    }

    function moneyOrDash(value, currency) {
        if (value === null || value === undefined || value === '') {
            return '<span class="pay-muted">-</span>';
        }
        return formatMoney(value, currency);
    }

    function renderCompare(summary) {
        var $table = $('#table_compare');
        if (!$table.length) return;
        summary = summary || {};
        if ($('#compareNote').length) {
            if (summary.has_api && !summary.has_csv) {
                $('#compareNote').html('Angka API sudah ada. Kolom CSV masih kosong karena file Transactions belum diimport — itu opsional.');
            } else if (summary.has_csv && !summary.has_api) {
                $('#compareNote').html('CSV sudah masuk. Klik <strong>Tarik API</strong> agar unpaid/paid resmi bisa dibandingkan.');
            } else if (summary.has_api && summary.has_csv) {
                $('#compareNote').html('API untuk total bayar, CSV untuk rincian earning dan Invalid Traffic. Status <strong>Cocok</strong> berarti angkanya selaras.');
            }
        }
        destroyTable($table);
        var $tb = $table.find('tbody').empty();
        var rows = summary.compare || [];
        if (!rows.length) {
            $tb.append('<tr><td colspan="6" class="pay-empty">Belum ada data untuk dibandingkan. Tarik API dulu, CSV hanya jika perlu rincian IVT.</td></tr>');
            return;
        }
        rows.forEach(function (row) {
            var gap = row.gap;
            var gapHtml = (gap === null || gap === undefined)
                ? '<span class="pay-muted">-</span>'
                : '<span class="' + (Math.abs(gap) >= 1 ? 'pay-num-down' : 'pay-num-up') + '">' + formatMoney(gap, summary.currency) + '</span>';
            $tb.append(
                '<tr>' +
                    '<td><strong>' + escapeHtml(row.label || '-') + '</strong></td>' +
                    '<td>' + moneyOrDash(row.api_value, summary.currency) + '</td>' +
                    '<td>' + moneyOrDash(row.csv_value, summary.currency) + '</td>' +
                    '<td>' + gapHtml + '</td>' +
                    '<td>' + matchPill(row.match) + '</td>' +
                    '<td class="pay-reason">' + escapeHtml(row.meaning || '') + '</td>' +
                '</tr>'
            );
        });
    }

    function ratePill(label) {
        var key = String(label || 'rendah');
        var cls = key === 'tinggi' ? 'is-invalid' : (key === 'sedang' ? 'is-adjust' : 'is-paid');
        return '<span class="pay-pill ' + cls + '">' + escapeHtml(key) + '</span>';
    }

    function renderInvalid(data) {
        data = data || {};
        if ($('#invalidNote').length) {
            $('#invalidNote').text(data.note || $('#invalidNote').text());
        }
        var $groups = $('#invalidGroups');
        if ($groups.length) {
            $groups.empty();
            var groups = data.groups || [];
            if (!groups.length) {
                $groups.append('<div class="pay-empty">Belum ada baris Invalid Activity pada filter ini. Import CSV Payments dulu.</div>');
            } else {
                groups.forEach(function (group) {
                    $groups.append(
                        '<div class="pay-insight-card is-risk">' +
                            '<div class="k">' + escapeHtml(group.product_label || 'Produk') + ' · ' + escapeHtml(group.reason_label || 'IVT') + '</div>' +
                            '<div class="v">' + formatMoney(group.invalid_amount, data.currency) + '</div>' +
                            '<div class="d">' +
                                escapeHtml(group.meaning || '') +
                                '<br>Earning: ' + formatMoney(group.earning_amount, data.currency) +
                                ' · Rasio IVT: ' + escapeHtml(String(group.ivt_rate || 0)) + '% ' + ratePill(group.ivt_rate_label) +
                            '</div>' +
                        '</div>'
                    );
                });
            }
        }
        var $table = $('#table_invalid');
        if ($table.length) {
            destroyTable($table);
            var $tb = $table.find('tbody').empty();
            (data.lines || []).forEach(function (row) {
                var amount = parseFloat(row.amount_value || 0);
                $tb.append(
                    '<tr>' +
                        '<td>' + escapeHtml(row.txn_date || '-') + '</td>' +
                        '<td>' + escapeHtml(row.product_label || '-') + '</td>' +
                        '<td>' + categoryPill(row.reason_label || 'invalid') + '</td>' +
                        '<td><div>' + escapeHtml(row.description || '-') + '</div><div class="pay-reason">' + escapeHtml(row.reason_detail || '') + '</div></td>' +
                        '<td class="pay-num-down">' + formatMoney(amount, row.currency || data.currency) + '</td>' +
                    '</tr>'
                );
            });
            if ($.fn.DataTable) {
                invalidTable = $table.DataTable({ order: [[0, 'desc']], pageLength: 25, autoWidth: false });
            }
        }
        var $dom = $('#table_invalid_domains');
        if ($dom.length) {
            destroyTable($dom);
            var $db = $dom.find('tbody').empty();
            (data.suspect_domains || []).forEach(function (row) {
                $db.append(
                    '<tr>' +
                        '<td>' + escapeHtml(row.domain || '-') + '</td>' +
                        '<td>' + escapeHtml(row.account_ads || '-') + '</td>' +
                        '<td>' + formatMoney(row.revenue, data.currency) + '</td>' +
                        '<td>' + formatMoney(row.rekap_revenue, data.currency) + '</td>' +
                        '<td class="' + (parseFloat(row.gap || 0) > 0 ? 'pay-num-down' : '') + '">' + formatMoney(row.gap, data.currency) + '</td>' +
                        '<td class="pay-num-down">' + formatMoney(row.estimated_ivt, data.currency) + '</td>' +
                        '<td class="pay-reason">' + escapeHtml(row.likely_reason || '-') + '</td>' +
                    '</tr>'
                );
            });
            if ($.fn.DataTable) {
                invalidDomainTable = $dom.DataTable({ order: [[5, 'desc']], pageLength: 25, autoWidth: false });
            }
        }
    }

    function renderAccountAds(data) {
        data = data || {};
        if ($('#accountAdsNote').length) {
            $('#accountAdsNote').text(data.note || $('#accountAdsNote').text());
        }
        var $groups = $('#accountAdsGroups');
        if ($groups.length) {
            $groups.empty();
            var accounts = data.accounts || [];
            if (!accounts.length) {
                $groups.append('<div class="pay-empty">Tidak ada subdomain pada Filter Account untuk rentang tanggal ini.</div>');
            } else {
                accounts.forEach(function (acc) {
                    $groups.append(
                        '<div class="pay-insight-card ' + (acc.running ? 'is-run' : '') + '">' +
                            '<div class="k">' + escapeHtml(acc.account_ads || 'Tanpa Account Ads') + '</div>' +
                            '<div class="v">' + formatMoney(acc.spend, data.currency) + '</div>' +
                            '<div class="d">' +
                                escapeHtml(String(acc.domain_count || 0)) + ' subdomain · Pendapatan ' + formatMoney(acc.revenue, data.currency) +
                                ' · Net ' + formatMoney(acc.net, data.currency) +
                                ' · ROI ' + escapeHtml(String(acc.roi || 0)) + '%' +
                                (acc.running ? '' : '<br>Tidak ada iklan Meta yang jalan di subdomain ini.') +
                            '</div>' +
                        '</div>'
                    );
                });
            }
        }
        var $table = $('#table_account_ads');
        if (!$table.length) return;
        destroyTable($table);
        var $tb = $table.find('tbody').empty();
        (data.domains || []).forEach(function (row) {
            var roi = parseFloat(row.roi || 0);
            $tb.append(
                '<tr>' +
                    '<td>' + escapeHtml(row.domain || '-') + '</td>' +
                    '<td>' + escapeHtml(row.account_ads || '-') + '</td>' +
                    '<td>' + categoryPill(row.campaign_status || '-') + '</td>' +
                    '<td>' + formatMoney(row.spend, data.currency) + '</td>' +
                    '<td>' + formatMoney(row.revenue, data.currency) + '</td>' +
                    '<td>' + formatMoney(row.net, data.currency) + '</td>' +
                    '<td class="' + (roi < 0 ? 'pay-num-down' : (roi > 0 ? 'pay-num-up' : '')) + '">' + escapeHtml(String(roi.toFixed ? roi.toFixed(2) : roi)) + '%</td>' +
                    '<td class="pay-num-down">' + formatMoney(row.estimated_ivt, data.currency) + '</td>' +
                '</tr>'
            );
        });
        if ($.fn.DataTable) {
            accountAdsTable = $table.DataTable({ order: [[3, 'desc']], pageLength: 25, autoWidth: false });
        }
    }

    function lastMonthRange() {
        var now = new Date();
        var end = new Date(now.getFullYear(), now.getMonth(), 0);
        var start = new Date(end.getFullYear(), end.getMonth(), 1);
        function ymd(d) {
            var m = String(d.getMonth() + 1).padStart(2, '0');
            var day = String(d.getDate()).padStart(2, '0');
            return d.getFullYear() + '-' + m + '-' + day;
        }
        return { start: ymd(start), end: ymd(end) };
    }

    function loadData() {
        showOverlay(true);
        if (cfg.source === 'adx') {
            showTransactionsTab();
        }
        $.getJSON(cfg.dataUrl + '?' + queryParams())
            .done(function (json) {
                if (!json || !json.status) {
                    toast(false, (json && json.error) || 'Gagal memuat data payment');
                    return;
                }
                var data = json.data || {};
                renderSummary(data.summary || {});
                renderCompare(data.summary || {});
                renderPayments(data.payments || []);
                renderTransactions(data.transactions || []);
                renderInvalid(data.invalid || {});
                renderAccountAds(data.account_ads || {});
            })
            .fail(function () {
                toast(false, 'Gagal memuat data payment');
            })
            .always(function () {
                showOverlay(false);
            });
    }

    function syncApi() {
        if (!cfg.canSync || !cfg.syncUrl) return;
        showOverlay(true);
        $.ajax({
            url: cfg.syncUrl,
            method: 'POST',
            data: {
                selected_account: selectedAccounts()
            }
        }).done(function (json) {
            if (!json || !json.status) {
                toast(false, (json && json.error) || 'Gagal menarik payment dari API');
                return;
            }
            var extra = (json.errors && json.errors.length) ? '\n' + json.errors.slice(0, 5).join('\n') : '';
            toast(true, (json.message || 'Sinkron selesai') + extra);
            loadData();
        }).fail(function () {
            toast(false, 'Gagal menarik payment dari API');
        }).always(function () {
            showOverlay(false);
        });
    }

    function importCsv() {
        var accountId = $('#import_account').val();
        var fileInput = document.getElementById('import_file');
        if (!accountId) {
            toast(false, 'Pilih akun tujuan import');
            return;
        }
        if (!fileInput || !fileInput.files || !fileInput.files[0]) {
            toast(false, 'Pilih file CSV/Excel terlebih dahulu');
            return;
        }
        var form = new FormData();
        form.append('account_id', accountId);
        form.append('file', fileInput.files[0]);
        showOverlay(true);
        $.ajax({
            url: cfg.importUrl,
            method: 'POST',
            data: form,
            processData: false,
            contentType: false
        }).done(function (json) {
            if (!json || !json.status) {
                toast(false, (json && json.error) || 'Import gagal');
                return;
            }
            toast(true, json.message || 'Import selesai');
            fileInput.value = '';
            if (window.$) {
                var nextTab = cfg.source === 'adsense' ? '#tabCompare' : '#tabTransactions';
                $('a[href="' + nextTab + '"]').tab('show');
            }
            loadData();
        }).fail(function () {
            toast(false, 'Import gagal');
        }).always(function () {
            showOverlay(false);
        });
    }

    function boot($) {
        if ($.fn.select2) {
            $('#account_filter').select2({
                placeholder: 'Semua akun',
                allowClear: true,
                width: '100%',
                theme: 'bootstrap4'
            });
            if ($('#import_account').length) {
                $('#import_account').select2({
                    placeholder: 'Cari dan pilih akun tujuan import',
                    allowClear: true,
                    width: '100%',
                    theme: 'bootstrap4'
                });
            }
        }
        var range = lastMonthRange();
        if (!$('#tanggal_dari').val()) $('#tanggal_dari').val(range.start);
        if (!$('#tanggal_sampai').val()) $('#tanggal_sampai').val(range.end);
        if (window.HrisDatepicker && typeof window.HrisDatepicker.initRange === 'function') {
            window.HrisDatepicker.initRange('#tanggal_dari', '#tanggal_sampai', {
                defaultFrom: range.start,
                defaultTo: range.end,
                skipDefaults: true
            });
        } else if (window.HrisDatepicker && typeof window.HrisDatepicker.init === 'function') {
            window.HrisDatepicker.init(document);
        }
        $('#btn_load_data').on('click', loadData);
        $('#btn_sync_api').on('click', syncApi);
        $('#btn_import_csv').on('click', importCsv);
        $('#import_account').on('change', function () {
            var $opt = $(this).find('option:selected');
            var network = $opt.data('network') || '';
            var publisher = String($opt.data('publisher') || '').replace(/^pub-/i, '');
            if ($('#btn_open_admanager').length) {
                var adxHref = network
                    ? ('https://admanager.google.com/' + String(network).replace(/\D/g, '') + '#payments')
                    : 'https://admanager.google.com/';
                $('#btn_open_admanager').attr('href', adxHref);
            }
            if ($('#btn_open_adsense').length) {
                var adsenseHref = publisher
                    ? ('https://www.google.com/adsense/new/u/0/pub-' + publisher + '/main/payments')
                    : 'https://www.google.com/adsense/';
                $('#btn_open_adsense').attr('href', adsenseHref);
            }
        });
        loadData();
    }

    function waitForJQuery(tries) {
        var $ = window.jQuery || window.$;
        if ($) {
            $(boot);
            return;
        }
        if ((tries || 0) >= 40) return;
        window.setTimeout(function () {
            waitForJQuery((tries || 0) + 1);
        }, 50);
    }

    waitForJQuery(0);
})();
