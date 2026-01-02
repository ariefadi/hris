/**
 * Reference Ajax AdX Traffic Per Account
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

    window.showOnlySelected = false;

    function fallbackCopyText(text) {
        var ta = document.createElement('textarea');
        ta.value = String(text || '');
        ta.style.position = 'fixed';
        ta.style.top = '-1000px';
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        try { document.execCommand('copy'); } catch (e) {}
        document.body.removeChild(ta);
    }

    window.showOnlySelected = false;

    function fallbackCopyText(text) {
        var ta = document.createElement('textarea');
        ta.value = String(text || '');
        ta.style.position = 'fixed';
        ta.style.top = '-1000px';
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        try { document.execCommand('copy'); } catch (e) {}
        document.body.removeChild(ta);
    }

    window.showOnlySelected = false;

    function fallbackCopyText(text) {
        var ta = document.createElement('textarea');
        ta.value = String(text || '');
        ta.style.position = 'fixed';
        ta.style.top = '-1000px';
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        try { document.execCommand('copy'); } catch (e) {}
        document.body.removeChild(ta);
    }
    // Pulihkan preferensi toggle dari localStorage (default: off)
    var savedHideZero = localStorage.getItem('roi_hide_zero_spend');
    if (savedHideZero !== null) {
        $('#toggle_hide_zero_spend').prop('checked', savedHideZero === '1');
    }
    window.showOnlySelected = false;
    // Initialize date pickers
    var today = new Date();
    var lastWeek = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
    $('#tanggal_dari').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    }).datepicker('setDate', lastWeek);
    $('#tanggal_sampai').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    }).datepicker('setDate', today);
    $('#account_filter').select2({
        placeholder: '-- Pilih Akun Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    let allAccountOptions = $('#account_filter').html();  
    // Initialize Select2 for site filter
    $('#domain_filter').select2({   
        placeholder: '-- Pilih Domain --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    let allDomainOptions = $('#domain_filter').html();  
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    $('#btn_load_data').click(function (e) {
        $('#overlay').show();
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domain = $("#domain_filter").val();
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            $("#overlay").show();
            load_adx_traffic_account_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    // Flag untuk mencegah infinite loop saat update filter
    var isUpdating = false;
    $('#account_filter').on('change', function () {
        if (isUpdating) return;
        let account = $(this).val();
        if (account && account.length > 0) {
            adx_site_list(); // filter domain by account
        } else {
            // restore semua domain dari template
            isUpdating = true;
            $('#domain_filter')
                .html(allDomainOptions)
                .val(null)
                .trigger('change.select2');
            isUpdating = false;
        }
    });
    function adx_site_list() {
        var selected_account = $("#account_filter").val();
        if (selected_account) {
            selected_account = selected_account.join(',');
        }
        return $.ajax({
            url: '/management/admin/adx_sites_list',
            type: 'GET',
            data: {
                selected_accounts: selected_account
            },
            headers: {
                'X-CSRFToken': csrftoken
            },
            success: function (response) {
                if (response && response.status) {
                    let $domain = $('#domain_filter');
                    let currentSelected = $domain.val(); // Simpan pilihan saat ini

                    isUpdating = true;
                    // 1. Kosongkan option lama
                    $domain.empty();

                    // 2. Tambahkan option baru
                    response.data.forEach(function (domain) {
                        let isSelected = currentSelected && currentSelected.includes(domain);
                        let option = new Option(domain, domain, isSelected, isSelected);
                        $domain.append(option);
                    });

                    // 3. Refresh select2
                    $domain.trigger('change.select2');
                    isUpdating = false;
                }
            },
            error: function (xhr, status, error) {
                report_eror(xhr, error);
            }
        });
    }
    $('#domain_filter').on('change', function () {
        if (isUpdating) return;
        let domain = $(this).val();
        if (domain && domain.length > 0) {
            adx_account_list();
        } else {
            isUpdating = true;
            $('#account_filter')
                .html(allAccountOptions)
                .val(null)
                .trigger('change.select2');
            isUpdating = false;
        }
    });
    function adx_account_list() {
        var selected_domain = $("#domain_filter").val();
        if (selected_domain) {
            selected_domain = selected_domain.join(',');
        }
        return $.ajax({
            url: '/management/admin/adx_accounts_list',
            type: 'GET',
            data: {
                selected_domains: selected_domain
            },
            headers: {
                'X-CSRFToken': csrftoken
            },
            success: function (response) {
                if (response && response.status) {
                    let $account = $('#account_filter');
                    let suggested = (response.data || []).map(function (a) { return String(a.account_id); });
                    isUpdating = true;
                    $account.html(allAccountOptions);
                    $account.val(suggested).trigger('change.select2');
                    isUpdating = false;
                }
            },
            error: function (xhr, status, error) {
                report_eror(xhr, error);
            }
        });
    }
    function getSelectedTextList(selector) { 
        var $el = $(selector);
        var items = [];
        try {
            var s2 = $el.select2('data');
            if (Array.isArray(s2) && s2.length) {
                items = s2.map(function (d) {
                    return d && d.text ? String(d.text) : '';
                });
            }
        } catch (e) {
            items = [];
        }
        if (!items || items.length === 0) {
            try {
                items = $el.find('option:selected').map(function () {
                    return $(this).text();
                }).get();
            } catch (e) {
                items = [];
            }
        }
        return (items || []).map(function (t) {
            return String(t || '').trim();
        }).filter(function (t) {
            return t;
        });
    }

    function escapeHtml(text) {
        return String(text || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/\"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function escapeXmlText(text) {
        return String(text || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function formatDateID(d) {
        if (!d) return '-';
        var months = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
        try {
            var date = new Date(d + 'T00:00:00');
            return date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear();
        } catch (e) {
            return d;
        }
    }

    function getExportMetaRoiDomain() {
        var start = $('#tanggal_dari').val();
        var end = $('#tanggal_sampai').val();
        var titleText = 'ROI Traffic Per Domain';
        var periodText = 'Periode ' + formatDateID(start) + ' s/d ' + formatDateID(end);

        var accounts = getSelectedTextList('#account_filter');
        var domains = getSelectedTextList('#domain_filter');

        return {
            titleText: titleText,
            periodText: periodText,
            accountText: accounts.length ? ('Account: ' + accounts.join(', ')) : '',
            domainText: domains.length ? ('Domain: ' + domains.join(', ')) : ''
        };
    }

    // Initialize DataTable
    var table = $('#table_traffic_account').DataTable({
        responsive: true,
        paging: true,
        pageLength: 25,
        lengthChange: true,
        lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "Semua"]],
        searching: true,
        ordering: true,
        fontSize: '10px',
        fontStyle: 'normal',
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
        dom: 'Blfrtip',
        buttons: [
            {
                text: 'Tampilkan Terpilih',
                className: 'btn btn-secondary',
                action: function (e, dt) {
                    window.showOnlySelected = !window.showOnlySelected;
                    $(e.currentTarget).toggleClass('active', window.showOnlySelected);
                    dt.draw();
                }
            },
            {
                text: 'Copy Terpilih',
                className: 'btn btn-info',
                action: function (e, dt) {
                    var lines = [];
                    lines.push(['Domain', 'Tanggal', 'ROI', 'Pendapatan'].join('\t'));

                    $('#table_traffic_account tbody input.row-select:checked').each(function () {
                        var tr = $(this).closest('tr');
                        var r = dt.row(tr).data();
                        if (!r) return;

                        var domainPlain = String(r[1] || '').replace(/<[^>]*>/g, '').trim();
                        var tanggalPlain = String(r[2] || '').replace(/<[^>]*>/g, '').trim();
                        var roiVal = Number(r[12] || 0);
                        var pendapatanVal = Number(r[13] || 0);

                        var roiText = formatNumber(roiVal, 2) + ' %';
                        var pendapatanText = formatCurrencyIDR(pendapatanVal);

                        if (domainPlain || tanggalPlain) {
                            lines.push([domainPlain, tanggalPlain, roiText, pendapatanText].join('\t'));
                        }
                    });

                    if (lines.length <= 1) {
                        alert('Pilih minimal satu baris terlebih dahulu.');
                        return;
                    }

                    var textToCopy = lines.join('\n');
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                        navigator.clipboard.writeText(textToCopy)
                            .then(function () { alert('Data terpilih berhasil dicopy.'); })
                            .catch(function () { fallbackCopyText(textToCopy); });
                    } else {
                        fallbackCopyText(textToCopy);
                    }
                }
            },
            {
                extend: 'excel',
                text: 'Export Excel',
                className: 'btn btn-success',
                exportOptions: { columns: ':visible' },
                title: function () { 
                    var meta = getExportMetaRoiDomain();
                    let title = 'ROI Traffic Per Domain';
                    if (meta.periodText) title += ' ' + meta.periodText;
                    if (meta.accountText) title += ' ' + meta.accountText;
                    if (meta.domainText) title += ' ' + meta.domainText;
                    return title;
                },
                customize: function (xlsx) {
                    var meta = getExportMetaRoiDomain();
                    var headerRows = [meta.titleText, meta.periodText];
                    if (meta.accountText) headerRows.push(meta.accountText);
                    if (meta.domainText) headerRows.push(meta.domainText);

                    var sheet = xlsx.xl.worksheets['sheet1.xml'];
                    var numrows = headerRows.length;

                    $('row', sheet).each(function () {
                        var r = parseInt($(this).attr('r'));
                        $(this).attr('r', r + numrows);
                    });
                    $('row c', sheet).each(function () {
                        var attr = $(this).attr('r');
                        var col = attr.replace(/[0-9]/g, '');
                        var row = parseInt(attr.replace(/[A-Z]/g, ''));
                        $(this).attr('r', col + (row + numrows));
                    });

                    var dim = $('dimension', sheet).attr('ref');
                    var lastCol = 'A';
                    if (dim) {
                        var parts = dim.split(':');
                        if (parts[1]) {
                            lastCol = parts[1].replace(/[0-9]/g, '') || 'A';
                            var lastRowNum = parseInt(parts[1].replace(/[A-Z]/g, '')) || 0;
                            $('dimension', sheet).attr('ref', parts[0] + ':' + lastCol + (lastRowNum + numrows));
                        }
                    }

                    for (var i = headerRows.length; i >= 1; i--) {
                        var txt = escapeXmlText(headerRows[i - 1]);
                        var rowXml = '<row r="' + i + '"><c t="inlineStr" r="A' + i + '"><is><t>' + txt + '</t></is></c></row>';
                        $('sheetData', sheet).prepend(rowXml);
                    }

                    var merges = $('mergeCells', sheet);
                    var mergeXml = '';
                    for (var m = 1; m <= headerRows.length; m++) {
                        mergeXml += '<mergeCell ref="A' + m + ':' + lastCol + m + '"/>';
                    }
                    if (merges.length === 0) {
                        $('worksheet', sheet).append('<mergeCells count="' + headerRows.length + '">' + mergeXml + '</mergeCells>');
                    } else {
                        var c = parseInt(merges.attr('count') || '0');
                        merges.attr('count', c + headerRows.length);
                        merges.append(mergeXml);
                    }
                }
            },
            {
                extend: 'pdf',
                text: 'Export PDF',
                className: 'btn btn-danger',
                orientation: 'landscape',
                exportOptions: { columns: ':visible' },
                title: function () { 
                    var meta = getExportMetaRoiDomain();
                    let title = 'ROI Traffic Per Domain';
                    if (meta.periodText) title += ' ' + meta.periodText;
                    if (meta.accountText) title += ' ' + meta.accountText;
                    if (meta.domainText) title += ' ' + meta.domainText;
                    return title;
                },
                customize: function (doc) {
                    var meta = getExportMetaRoiDomain();
                    var inserts = [];
                    inserts.push({ text: meta.periodText, alignment: 'center', margin: [0, 0, 0, 6] });
                    if (meta.accountText) inserts.push({ text: meta.accountText, alignment: 'center', margin: [0, 0, 0, 4] });
                    if (meta.domainText) inserts.push({ text: meta.domainText, alignment: 'center', margin: [0, 0, 0, 8] });
                    doc.content.splice(1, 0, ...inserts);
                }
            },
            {
                text: 'Copy',
                className: 'btn btn-info',
                action: function (e, dt) {
                    var meta = getExportMetaRoiDomain();
                    var data = dt.buttons.exportData({ columns: ':visible' });
                    var lines = [];
                    lines.push(meta.titleText);
                    lines.push(meta.periodText);
                    if (meta.accountText) lines.push(meta.accountText);
                    if (meta.domainText) lines.push(meta.domainText);
                    lines.push('');
                    lines.push((data.header || []).join('\t'));
                    (data.body || []).forEach(function (row) { lines.push(row.join('\t')); });
                    var text = lines.join('\n');
                    function fallbackCopy(str) {
                        var ta = document.createElement('textarea');
                        ta.value = str;
                        ta.style.position = 'fixed';
                        ta.style.top = '-1000px';
                        document.body.appendChild(ta);
                        ta.focus();
                        ta.select();
                        try { document.execCommand('copy'); } catch(e) {}
                        document.body.removeChild(ta);
                    }
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                        navigator.clipboard.writeText(text).catch(function(){ fallbackCopy(text); });
                    } else {
                        fallbackCopy(text);
                    }
                },
                exportOptions: { columns: ':visible' } 
            },
            {
                extend: 'csv',
                text: 'Export CSV',
                className: 'btn btn-primary',
                exportOptions: { columns: ':visible' },
                title: function () { 
                    var meta = getExportMetaRoiDomain();
                    let title = 'ROI Traffic Per Domain';
                    if (meta.periodText) title += ' ' + meta.periodText;
                    if (meta.accountText) title += ' ' + meta.accountText;
                    if (meta.domainText) title += ' ' + meta.domainText;
                    return title;
                },
                customize: function (csv) {
                    var meta = getExportMetaRoiDomain();
                    var out = meta.titleText + '\n' + meta.periodText;
                    if (meta.accountText) out += '\n' + meta.accountText;
                    if (meta.domainText) out += '\n' + meta.domainText;
                    return out + '\n\n' + csv;
                }
            },
            {
                extend: 'print',
                text: 'Print',
                className: 'btn btn-warning',
                exportOptions: { columns: ':visible' },
                title: function () { 
                    var meta = getExportMetaRoiDomain();
                    let title = 'ROI Traffic Per Domain';
                    if (meta.periodText) title += ' ' + meta.periodText;
                    if (meta.accountText) title += ' ' + meta.accountText;
                    if (meta.domainText) title += ' ' + meta.domainText;
                    return '<h3 style="text-align:center;margin:0">' + title + '</h3>';
                },
                messageTop: function () {
                    var meta = getExportMetaRoiDomain();
                    var html = '<div style="text-align:center;margin-bottom:8px">' + escapeHtml(meta.periodText) + '</div>';
                    if (meta.accountText) html += '<div style="text-align:center;margin-bottom:4px">' + escapeHtml(meta.accountText) + '</div>';
                    if (meta.domainText) html += '<div style="text-align:center;margin-bottom:8px">' + escapeHtml(meta.domainText) + '</div>';
                    return html;
                }
            },
            {
                extend: 'colvis',
                text: 'Column Visibility',
                className: 'btn btn-default',
                exportOptions: { columns: ':visible' } 
            }
        ],
        columnDefs: [
            {
                targets: 0,
                orderable: false,
                searchable: false,
                className: 'dt-body-center checkbox-cell',
                render: function (data, type, row, meta) {
                    var id = 'row_select_' + meta.row;
                    return '<div class="form-check checkbox-center m-0">' +
                           '<input type="checkbox" class="form-check-input row-select" id="' + id + '" />' +
                           '<label class="form-check-label" for="' + id + '" title="Pilih baris"></label>' +
                           '</div>';
                }
            },
            {
                targets: 2, // INDEX kolom Tanggal (mulai dari 0)
                render: function (data, type) {
                    if (type === 'sort' || type === 'type') {
                        return data.sort;     // YYYY-MM-DD
                    }
                    return data.display;      // "30 November 2025"
                }
            },
            {
                targets: [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                className: "text-right"
            },
            {
                targets: 3,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatCurrencyIDR(val);
                }
            },
            {
                targets: 4,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatNumber(val);
                }
            },
            {
                targets: 5,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatNumber(val);
                }
            },
            {
                targets: 6,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatCurrencyIDR(val);
                }
            },
            {
                targets: 7,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatNumber(val, 2) + ' %';
                }
            },
            {
                targets: 8,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatNumber(val, 2) + ' %';
                }
            },
            {
                targets: 9,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatCurrencyIDR(val);
                }
            },
            {
                targets: 10,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatCurrencyIDR(val);
                }
            },
            {
                targets: 11,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatCurrencyIDR(val);
                }
            },
            {
                targets: 12,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatNumber(val, 2) + ' %';
                }
            },
            {
                targets: 13,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatCurrencyIDR(val);
                }
            }
        ],
        order: [[2, 'asc']]
    });

    var table = $('#table_traffic_account').DataTable();

    $.fn.dataTable.ext.search.push(function (settings, data, dataIndex) {
        try {
            if (!settings || !settings.nTable || settings.nTable.id !== 'table_traffic_account') return true;
        } catch (e) {
            return true;
        }
        if (!window.showOnlySelected) return true;
        var rowNode = table.row(dataIndex).node();
        var checked = $(rowNode).find('input.row-select').prop('checked');
        return !!checked;
    });

    $('#select_all_rows').off('change').on('change', function () {
        var checked = $(this).is(':checked');
        var $inputs = $('#table_traffic_account tbody input.row-select').prop('checked', checked);
        $inputs.each(function () {
            $(this).closest('tr').toggleClass('selected-row', checked);
        });
        table.draw(false);
    });

    $('#table_traffic_account tbody').off('change', 'input.row-select').on('change', 'input.row-select', function () {
        var $tr = $(this).closest('tr');
        $tr.toggleClass('selected-row', $(this).is(':checked'));

        var all = $('#table_traffic_account tbody input.row-select').length;
        var selected = $('#table_traffic_account tbody input.row-select:checked').length;
        $('#select_all_rows').prop('checked', all > 0 && selected === all);

        if (window.showOnlySelected) {
            table.draw(false);
        }
    });

    table.on('draw', function () {
        $('#table_traffic_account tbody input.row-select').each(function () {
            $(this).closest('tr').toggleClass('selected-row', $(this).is(':checked'));
        });
        var all = $('#table_traffic_account tbody input.row-select').length;
        var selected = $('#table_traffic_account tbody input.row-select:checked').length;
        $('#select_all_rows').prop('checked', all > 0 && selected === all);
    });

    function fallbackCopyText(text) {
        var ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.top = '-1000px';
        ta.style.left = '-1000px';
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        try {
            document.execCommand('copy');
            alert('Data terpilih berhasil dicopy ke clipboard.');
        } catch (e) {
            alert('Gagal menyalin ke clipboard.');
        } finally {
            document.body.removeChild(ta);
        }
    }

    // Terapkan filter berdasar toggle hide zero spend
    // Global helper: filter data spend > 0
    window.applyZeroSpendFilter = function (data) {
        var hideZero = $('#toggle_hide_zero_spend').is(':checked');
        var arr = data || [];
        if (!hideZero) return arr;
        return arr.filter(function (item) {
            var spendVal = parseFloat(item.spend || 0);
            return spendVal > 0;
        });
    };
    // Tambahkan helper: pilih dataset sesuai toggle, prioritaskan data_filtered dari backend
    window.applyZeroSpendFilterDataset = function () {
        var hideZero = $('#toggle_hide_zero_spend').is(':checked');
        if (hideZero) {
            if (Array.isArray(window.lastRoiDataFiltered) && window.lastRoiDataFiltered.length > 0) {
                return window.lastRoiDataFiltered;
            }
            var base = Array.isArray(window.lastRoiDataAll) ? window.lastRoiDataAll : (window.lastRoiData || []);
            return (base || []).filter(function (item) { return Number(item.spend || 0) > 0; });
        }
        return Array.isArray(window.lastRoiDataAll) ? window.lastRoiDataAll : (window.lastRoiData || []);
    };

    // Re-render saat toggle hide zero spend berubah
    $('#toggle_hide_zero_spend').on('change', function () {
        var checked = $(this).is(':checked');
        localStorage.setItem('roi_hide_zero_spend', checked ? '1' : '0');

        // Gunakan dataset sesuai toggle (prioritas backend data_filtered)
        var displayData = window.applyZeroSpendFilterDataset();

        // Update summary box sesuai data yang ditampilkan
        window.updateSummaryBoxes(displayData);

        // Re-render chart dari data hasil filter
        if (displayData.length > 0) {
            $('#charts_section').show();
            createROIDailyChart(displayData);
        } else {
            if (roiChart) { roiChart.destroy(); roiChart = null; }
            $('#charts_section').hide();
        }

        // Re-render DataTable dari data hasil filter
        var table = $('#table_traffic_account').DataTable();
        table.clear();
        displayData.forEach(function (item) {
            var formattedDate = item.date || '-';
            if (item.date && item.date.match(/\d{4}-\d{2}-\d{2}/)) {
                var months = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
                var date = new Date(item.date + 'T00:00:00');
                formattedDate = date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear();
            }
            table.row.add([
                '',
                item.site_name || '-',
                formattedDate,
                Number(item.spend || 0),
                Number(item.clicks_fb || 0),
                Number(item.clicks_adx || 0),
                Number(item.cpr || 0),
                Number(item.ctr_fb || 0),
                Number(item.ctr_adx || 0),
                Number(item.cpc_fb || 0),
                Number(item.cpc_adx || 0),
                Number(item.cpm || 0),
                Number(item.roi || 0),
                Number(item.revenue || 0)
            ]);
        });
        table.draw();
    });
});

function load_adx_traffic_account_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain) {
    var selectedAccount = $('#select_account').val();
    if (!tanggal_dari || !tanggal_sampai) {
        alert('Silakan pilih tanggal dari dan sampai.');
        return;
    }
    var accountFilter = '';
    if (selected_account && selected_account.length > 0) {
        accountFilter = selected_account.join(',');
    }
    // Convert array to comma-separated string for backend
    var domainFilter = '';
    if (selected_domain && selected_domain.length > 0) {
        domainFilter = selected_domain.join(',');
    }
    $.ajax({
        url: '/management/admin/page_roi_traffic_domain',
        type: 'GET',
        data: {
            start_date: tanggal_dari,
            end_date: tanggal_sampai,
            selected_account_adx: accountFilter,
            selected_domains: domainFilter,
            selected_account_ads: selectedAccount,
        },
        headers: {
            'X-CSRFToken': csrftoken
        },
        success: function (response) {
            if (response && response.status) {
                // Hapus pengisian summary lama berbasis response.summary
                // Global helper: hitung ulang summary dari dataset yang sedang ditampilkan
                window.updateSummaryBoxes = function (data) {
                    var totalClicksFb = 0;
                    var totalClicksAdx = 0;
                    var totalSpend = 0;
                    var totalRevenue = 0;
                
                    (data || []).forEach(function (item) {
                        totalClicksFb += Number(item.clicks_fb || 0);
                        totalClicksAdx += Number(item.clicks_adx || 0);
                        totalSpend += Number(item.spend || 0);
                        totalRevenue += Number(item.revenue || 0);
                    });
                
                    var roiNett = totalSpend > 0 ? ((totalRevenue - totalSpend) / totalSpend) * 100 : 0;
                
                    $('#total_clicks_fb').text(formatNumber(totalClicksFb));
                    $('#total_clicks_adx').text(formatNumber(totalClicksAdx));
                    $('#total_spend').text(formatCurrencyIDR(totalSpend));
                    $('#roi_nett').text(formatNumber(roiNett, 2) + '%');
                    $('#total_revenue').text(formatCurrencyIDR(totalRevenue));
                
                    // pastikan summary terlihat
                    $('#summary_boxes').show();
                };
                // Create ROI Daily Chart
                // Simpan dataset agregasi (all vs filtered)
                window.lastRoiDataAll = Array.isArray(response.data) ? response.data : [];
                window.lastRoiDataFiltered = Array.isArray(response.data_filtered)
                    ? response.data_filtered
                    : (window.lastRoiDataAll || []).filter(function (i) {
                        return Number(i.spend || 0) > 0;
                    });
                // Pertahankan kompatibilitas lama
                window.lastRoiData = window.lastRoiDataAll;

                // Gunakan dataset sesuai toggle (prioritas backend data_filtered)
                var displayData = window.applyZeroSpendFilterDataset();

                // UPDATE SUMMARY BOX dari data hasil filter
                window.updateSummaryBoxes(displayData);

                // Chart: gunakan data hasil filter
                if (displayData && displayData.length > 0) {
                    $('#charts_section').show();
                    createROIDailyChart(displayData);
                    if (roiChart && typeof roiChart.resize === 'function') { roiChart.resize(); }
                } else {
                    if (roiChart) { roiChart.destroy(); roiChart = null; }
                    $('#charts_section').hide();
                }
                
                // Update DataTable menggunakan data hasil filter
                var table = $('#table_traffic_account').DataTable();
                table.clear();
                
                displayData.forEach(function (item) {
                    var formattedDate = item.date || '-';
                    if (item.date && item.date.match(/\d{4}-\d{2}-\d{2}/)) {
                        var months = [
                            'Januari','Februari','Maret','April','Mei','Juni',
                            'Juli','Agustus','September','Oktober','November','Desember'
                        ];
                        var date = new Date(item.date + 'T00:00:00');
                        var day = date.getDate();
                        var month = months[date.getMonth()];
                        var year = date.getFullYear();
                        formattedDate = day + ' ' + month + ' ' + year;
                    }
                    table.row.add([
                        '',
                        item.site_name || '-',
                        {
                            display: formattedDate,
                            sort: item.date || '' // YYYY-MM-DD
                        },
                        Number(item.spend || 0),
                        Number(item.clicks_fb || 0),
                        Number(item.clicks_adx || 0),
                        Number(item.cpr || 0),
                        Number(item.ctr_fb || 0),
                        Number(item.ctr_adx || 0),
                        Number(item.cpc_fb || 0),
                        Number(item.cpc_adx || 0),
                        Number(item.cpm || 0),
                        Number(item.roi || 0),
                        Number(item.revenue || 0)
                    ]);
                });
                
                table.draw();
                showSuccessMessage('Traffic data loaded successfully!');
                $("#overlay").hide();
            } else {
                alert('Error: ' + (response && response.error ? response.error : 'Unknown error occurred'));
                $("#overlay").hide();
            }
        },
        error: function (jqXHR, textStatus, errorThrown) {
            report_eror(jqXHR, textStatus);
            $("#overlay").hide();
        }
    });
}
function formatNumber(num, decimals = 0) {
    if (num === null || num === undefined) return '0';
    return parseFloat(num).toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}
// Fungsi untuk format mata uang IDR
function formatCurrencyIDR(value) {
    // Handle null, undefined, or non-numeric values
    if (value === null || value === undefined || value === '') {
        return 'Rp. 0';
    }
    
    // Handle set objects or other complex objects
    if (typeof value === 'object' && value !== null) {
        // If it's a set-like object, try to get the first value or return 0
        if (value.constructor && value.constructor.name === 'Set') {
            return 'Rp. 0';
        }
        // For other objects, try to convert to string first
        value = String(value);
    }
    
    // Convert to string and remove currency symbols and commas
    let stringValue = String(value);
    let numValue = parseFloat(stringValue.replace(/[$,]/g, ''));
    
    if (isNaN(numValue)) return 'Rp. 0';

    // Round to remove decimals and format with Indonesian number format
    return 'Rp. ' + Math.round(numValue).toLocaleString('id-ID');
}
function showSuccessMessage(message) {
    var alertHtml = '<div class="alert alert-success alert-dismissible fade show" role="alert">';
    alertHtml += '<i class="bi bi-check-circle"></i> ' + message;
    alertHtml += '<button type="button" class="close" data-dismiss="alert" aria-label="Close">';
    alertHtml += '<span aria-hidden="true">&times;</span>';
    alertHtml += '</button>';
    alertHtml += '</div>';
    $('.card-body').first().prepend(alertHtml);
    setTimeout(function () {
        $('.alert-success').fadeOut('slow', function () {
            $(this).remove();
        });
    }, 3000);
}
function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}
const csrftoken = getCookie('csrftoken');
// Global variable to store chart instance
let roiChart = null;
// Function to create ROI Daily Chart
function createROIDailyChart(data) {
    // Check if Chart.js is loaded
    if (typeof Chart === 'undefined') {
        console.error('Chart.js is not loaded!');
        return;
    }
    // Destroy existing chart if it exists
    if (roiChart) {
        roiChart.destroy();
    }
    // Group data by date and calculate average ROI per date
    const dailyROI = {};
    const dailyRevenue = {};
    const dailySpend = {};
    data.forEach(item => {
        const date = item.date;
        const roi = parseFloat(item.roi || 0);
        const revenue = parseFloat(item.revenue || 0);
        const spend = parseFloat(item.spend || 0);
        if (!dailyROI[date]) {
            dailyROI[date] = [];
            dailyRevenue[date] = 0;
            dailySpend[date] = 0;
        }
        dailyROI[date].push(roi);
        dailyRevenue[date] += revenue;
        dailySpend[date] += spend;
    });
    // Calculate average ROI per date and sort by date
    const sortedDates = Object.keys(dailyROI).sort();
    const avgROIData = [];
    const revenueData = [];
    const spendData = [];
    const labels = [];
    sortedDates.forEach(date => {
        const roiValues = dailyROI[date];
        const avgROI = roiValues.reduce((sum, roi) => sum + roi, 0) / roiValues.length;
        // Format date for display
        const dateObj = new Date(date + 'T00:00:00');
        const formattedDate = dateObj.toLocaleDateString('id-ID', {
            day: '2-digit',
            month: 'short'
        });
        labels.push(formattedDate);
        // Pastikan nilai ROI berupa number, bukan string
        avgROIData.push(parseFloat(avgROI.toFixed(2)));
        revenueData.push(dailyRevenue[date]);
        spendData.push(dailySpend[date]);
    });
    // Buat chart baru
    const canvasElement = document.getElementById('chart_roi_daily');
    if (!canvasElement) {
        console.error('Canvas element with id "chart_roi_daily" not found!');
        return;
    }
    if (canvasElement.tagName !== 'CANVAS') {
        console.error('Element with id "chart_roi_daily" is not a canvas element! It is:', canvasElement.tagName);
        return;
    }
    // Pastikan ukuran canvas memadai
    canvasElement.style.height = '300px';
    canvasElement.style.width = '100%';
    // Hapus atribut width/height bawaan agar tidak bentrok dengan style
    canvasElement.removeAttribute('width');
    canvasElement.removeAttribute('height');
    const ctx = canvasElement.getContext('2d');
    if (!ctx) {
        console.error('Failed to get 2D context from canvas element!');
        return;
    }
    // Create the chart
    roiChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'ROI Harian (%)',
                data: avgROIData,
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.1)',
                borderWidth: 3,
                fill: true,
                tension: 0.4,
                pointBackgroundColor: 'rgb(75, 192, 192)',
                pointBorderColor: '#fff',
                pointBorderWidth: 2,
                pointRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Tren ROI Harian',
                    font: {
                        size: 16,
                        weight: 'bold'
                    }
                },
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function (context) {
                            const dataIndex = context.dataIndex;
                            const roi = context.parsed.y;
                            const revenue = revenueData[dataIndex];
                            const spend = spendData[dataIndex];

                            return [
                                `ROI: ${Number(roi).toFixed(2)}%`,
                                `Revenue: ${formatCurrencyIDR(revenue)}`,
                                `Spend: ${formatCurrencyIDR(spend)}`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Tanggal',
                        font: {
                            weight: 'bold'
                        }
                    },
                    grid: {
                        display: true,
                        color: 'rgba(0, 0, 0, 0.1)'
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'ROI (%)',
                        font: {
                            weight: 'bold'
                        }
                    },
                    grid: {
                        display: true,
                        color: 'rgba(0, 0, 0, 0.1)'
                    },
                    ticks: {
                        callback: function (value) {
                            return value + '%';
                        }
                    }
                }
            },
            interaction: {
                mode: 'nearest',
                axis: 'x',
                intersect: false
            }
        }
    });
}