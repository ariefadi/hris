/**
 * Reference Ajax AdX Traffic Per Account
 */
function escapeHtml(text) {
    return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function isRoiDarkTheme() {
    return document.documentElement.getAttribute('data-theme') === 'dark';
}

function getRoiChartTheme() {
    var dark = isRoiDarkTheme();
    return {
        text: dark ? '#e2e8f0' : '#334155',
        muted: dark ? '#94a3b8' : '#64748b',
        grid: dark ? 'rgba(148, 163, 184, 0.12)' : 'rgba(15, 23, 42, 0.08)',
        tooltipBg: dark ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.98)',
        tooltipBorder: dark ? 'rgba(255,255,255,0.1)' : 'rgba(15, 23, 42, 0.1)'
    };
}

var ROI_TRAFFIC_DOMAIN_CHART_VISIBLE_KEY = 'roiTrafficDomainChartVisible';

function isRoiTrafficDomainChartVisible() {
    try {
        return localStorage.getItem(ROI_TRAFFIC_DOMAIN_CHART_VISIBLE_KEY) !== '0';
    } catch (e) {
        return true;
    }
}

function reflowRoiTrafficDomainChart() {
    if (roiChart && typeof roiChart.resize === 'function') {
        try { roiChart.resize(); } catch (e) { }
    }
}

function setRoiTrafficDomainChartVisible(visible, animate) {
    window.__roiTrafficDomainChartVisible = !!visible;
    try {
        localStorage.setItem(ROI_TRAFFIC_DOMAIN_CHART_VISIBLE_KEY, visible ? '1' : '0');
    } catch (e) { }

    var $section = $('#charts_section');
    var $body = $('#charts_section_body');
    var $btn = $('#btnToggleChart');
    if (!$section.length || !$body.length || !$btn.length) return;

    $btn.attr('aria-expanded', visible ? 'true' : 'false');
    if (visible) {
        $btn.html('<i class="fas fa-eye-slash" aria-hidden="true"></i> Sembunyikan Grafik');
        $section.removeClass('chart-collapsed');
    } else {
        $btn.html('<i class="fas fa-eye" aria-hidden="true"></i> Tampilkan Grafik');
        $section.addClass('chart-collapsed');
    }

    if (animate) {
        if (visible) {
            $body.stop(true, true).slideDown(200, reflowRoiTrafficDomainChart);
        } else {
            $body.stop(true, true).slideUp(200);
        }
        return;
    }

    $body.toggle(visible);
    if (visible) reflowRoiTrafficDomainChart();
}

function showRoiTrafficDomainChartPanel() {
    $('#charts_section').show();
    $('#btnToggleChart').show();
    setRoiTrafficDomainChartVisible(isRoiTrafficDomainChartVisible(), false);
}

function hideRoiTrafficDomainChartPanel() {
    $('#charts_section').hide();
    $('#btnToggleChart').hide();
}

function showRoiDomainContent() {
    $('#roiDomainEmpty').hide();
}

function resetRoiDomainSections() {
    $('#summary_boxes, #charts_section').hide();
    $('#btnToggleChart').hide();
}

function formatSiteCell(name) {
    var n = String(name || '-').trim();
    if (!n || n === '-') return '-';
    return '<span class="site-badge" title="' + escapeHtml(n) + '">' + escapeHtml(n) + '</span>';
}

function formatDateCell(formattedDate) {
    if (!formattedDate || formattedDate === '-') return '-';
    return '<span class="date-badge">' + escapeHtml(String(formattedDate)) + '</span>';
}

function buildRoiDomainRow(item) {
    var formattedDate = item.date || '-';
    if (item.date && item.date.match(/\d{4}-\d{2}-\d{2}/)) {
        var months = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember'];
        var date = new Date(item.date + 'T00:00:00');
        formattedDate = date.getDate() + ' ' + months[date.getMonth()] + ' ' + date.getFullYear();
    }
    return [
        '',
        formatSiteCell(item.site_name),
        { display: formatDateCell(formattedDate), sort: item.date || '' },
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
        Number(item.revenue || 0),
        (Number(item.revenue || 0) - Number(item.spend || 0))
    ];
}

function normalizeDomainFilter(selected_domain) {
    if (Array.isArray(selected_domain)) {
        return selected_domain.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
    }
    return String(selected_domain || '').trim();
}

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

    $('#btnToggleChart').on('click', function (e) {
        e.preventDefault();
        setRoiTrafficDomainChartVisible(!window.__roiTrafficDomainChartVisible, true);
    });

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
    if (window.HrisDatepicker) {
        HrisDatepicker.initRange('#tanggal_dari', '#tanggal_sampai');
    }
    $('#account_filter').select2({
        placeholder: '-- Pilih Akun Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    $('#domain_filter').select2({
        placeholder: 'ketik subdomain AdX...',
        allowClear: true,
        width: '100%',
        theme: 'bootstrap4',
        tags: true,
        tokenSeparators: [','],
        ajax: {
            url: '/management/admin/adx_domain_suggest',
            dataType: 'json',
            delay: 250,
            data: function (params) {
                var selectedAccount = $('#account_filter').val();
                if (Array.isArray(selectedAccount)) selectedAccount = selectedAccount.join(',');
                return {
                    q: params.term || '',
                    start_date: $('#tanggal_dari').val() || '',
                    end_date: $('#tanggal_sampai').val() || '',
                    selected_account: selectedAccount || ''
                };
            },
            processResults: function (data) {
                return { results: (data && data.results) ? data.results : [] };
            }
        }
    });
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    $('#btn_load_data').click(function (e) {
        $('#overlay').show();
        $('#roiDomainEmpty').hide();
        resetRoiDomainSections();
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domain = normalizeDomainFilter($("#domain_filter").val());
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            $("#overlay").show();
            window.resetRoiDomainTableToHeaderOnly();
            $('#summary_boxes').hide();
            $('#charts_section').hide();
            load_adx_traffic_account_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    // Filter silang account-domain dinonaktifkan karena domain menggunakan freetext.
    function getSelectedTextList(selector) { 
        var $el = $(selector);
        var items = [];
        if (selector === '#domain_filter') {
            var raw = String($el.val() || '').trim();
            return raw ? raw.split(',').map(function (d) { return String(d || '').trim(); }).filter(function (d) { return d; }) : [];
        }
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

        var startSlug = String(start || '').replace(/-/g, '');
        var endSlug = String(end || '').replace(/-/g, '');

        return {
            titleText: titleText,
            periodText: periodText,
            accountText: accounts.length ? ('Account: ' + accounts.join(', ')) : '',
            domainText: domains.length ? ('Domain: ' + domains.join(', ')) : '',
            filenameBase: 'roi_traffic_domain_' + (startSlug || 'start') + '_' + (endSlug || 'end')
        };
    }

    function roiDomainExportPdfWidths(headers) {
        if (!headers || headers.length !== 14) return null;
        return [70, 46, 38, 22, 24, 30, 26, 26, 32, 32, 32, 28, 36, 38];
    }

    function roiDomainPdfExpandWeight(header, index) {
        var h = String(header || '').toLowerCase();
        if (index === 0 || h.indexOf('subdomain') >= 0 || (h.indexOf('domain') >= 0 && h.indexOf('subdomain') < 0)) return 1.05;
        if (h.indexOf('tanggal') >= 0 || h.indexOf('date') >= 0) return 0.58;
        if (h.indexOf('spend') >= 0) return 0.62;
        if (h.indexOf('klik') >= 0 && h.indexOf('adx') >= 0) return 0.48;
        if (h.indexOf('klik') >= 0) return 0.45;
        if (h.indexOf('ecpm') >= 0 || (h.indexOf('cpm') >= 0 && h.indexOf('ecpm') < 0)) return 0.95;
        if (h.indexOf('roi') >= 0 && h.indexOf('ctr') < 0) return 0.92;
        if (h.indexOf('pendapatan') >= 0 && h.indexOf('bersih') < 0) return 0.75;
        if (h.indexOf('bersih') >= 0) return 0.82;
        return 1;
    }

    window.roiDomainTable = null;

    window.resetRoiDomainTableToHeaderOnly = function () {
        if ($.fn.DataTable && $.fn.DataTable.isDataTable && $.fn.DataTable.isDataTable('#table_traffic_account')) {
            var existing = $('#table_traffic_account').DataTable();
            if (existing && existing.state) { existing.state.clear(); }
            existing.destroy();
        }
        $('#table_traffic_account tbody').empty();
        $('#select_all_rows').prop('checked', false);
        window.roiDomainTable = null;
    };

    function bindRoiDomainRowSelectionHandlers(table) {
        if (!table) return;

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

        table.off('draw.roiDomain').on('draw.roiDomain', function () {
            $('#table_traffic_account tbody input.row-select').each(function () {
                $(this).closest('tr').toggleClass('selected-row', $(this).is(':checked'));
            });
            var all = $('#table_traffic_account tbody input.row-select').length;
            var selected = $('#table_traffic_account tbody input.row-select:checked').length;
            $('#select_all_rows').prop('checked', all > 0 && selected === all);
        });
    }

    window.ensureRoiDomainTable = function () {
        if ($.fn.DataTable && $.fn.DataTable.isDataTable && $.fn.DataTable.isDataTable('#table_traffic_account')) {
            var existing = $('#table_traffic_account').DataTable();
            window.roiDomainTable = existing;
            bindRoiDomainRowSelectionHandlers(existing);
            return existing;
        }

        var table = $('#table_traffic_account').DataTable({
        responsive: false,
        scrollX: true,
        scrollXInner: '100%',
        scrollCollapse: true,
        autoWidth: false,
        paging: true,
        pageLength: 25,
        lengthChange: true,
        lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "Semua"]],
        searching: true,
        ordering: true,
        fontsize: '10px',
        order: [[0, 'asc']],
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
        buttons: (window.HrisTableExport && HrisTableExport.buildTrafficExportButtons)
            ? HrisTableExport.buildTrafficExportButtons({
                getDt: function () { return window.roiDomainTable; },
                getMeta: getExportMetaRoiDomain,
                columnSelector: ':visible:not(.no-export)',
                extraButtons: [
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
                                if (r[2] && typeof r[2] === 'object' && r[2].display) {
                                    tanggalPlain = String(r[2].display || '').replace(/<[^>]*>/g, '').trim();
                                }
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
                    }
                ],
                sheetName: 'ROI Traffic Domain',
                pdfWidths: roiDomainExportPdfWidths,
                pdfExpandWeight: roiDomainPdfExpandWeight,
                pdfFontSize: 7,
                pageMargins: [10, 22, 10, 22]
            })
            : [],
        columnDefs: [
            {
                targets: 0,
                orderable: false,
                searchable: false,
                className: 'dt-body-center checkbox-cell no-export',
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
                targets: [0, 2, 7, 8, 12],
                className: "text-center"
            },
            {
                targets: [3, 4, 5, 6, 9, 10, 11, 13, 14],
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
                    if (type === 'sort' || type === 'type' || type === 'filter') return val;
                    var cls = val >= 0 ? 'roi-val-positive' : 'roi-val-negative';
                    return '<span class="' + cls + '">' + formatNumber(val, 2) + ' %</span>';
                }
            },
            {
                targets: 13,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatCurrencyIDR(val);
                }
            },
            {
                targets: 14,
                type: 'num',
                render: function (data, type) {
                    var val = Number(data) || 0;
                    return (type === 'sort' || type === 'type' || type === 'filter') ? val : formatCurrencyIDR(val);
                }
            }
        ],
        order: [[2, 'asc']]
    });

        window.roiDomainTable = table;
        bindRoiDomainRowSelectionHandlers(table);
        return table;
    };

    $.fn.dataTable.ext.search.push(function (settings, data, dataIndex) {
        try {
            if (!settings || !settings.nTable || settings.nTable.id !== 'table_traffic_account') return true;
        } catch (e) {
            return true;
        }
        if (!window.showOnlySelected) return true;
        var dt = window.roiDomainTable;
        if (!dt) return true;
        var rowNode = dt.row(dataIndex).node();
        var checked = $(rowNode).find('input.row-select').prop('checked');
        return !!checked;
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

        if (!Array.isArray(window.lastRoiDataAll) && !Array.isArray(window.lastRoiData)) {
            return;
        }

        // Gunakan dataset sesuai toggle (prioritas backend data_filtered)
        var displayData = window.applyZeroSpendFilterDataset();

        // Update summary box sesuai data yang ditampilkan
        window.updateSummaryBoxes(displayData);

        // Re-render chart dari data hasil filter
        if (displayData.length > 0) {
            showRoiTrafficDomainChartPanel();
            createROIDailyChart(displayData);
        } else {
            if (roiChart) { roiChart.destroy(); roiChart = null; }
            hideRoiTrafficDomainChartPanel();
        }

        // Re-render DataTable dari data hasil filter
        var table = window.ensureRoiDomainTable();
        table.clear();
        displayData.forEach(function (item) {
            table.row.add(buildRoiDomainRow(item));
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
    var domainFilter = normalizeDomainFilter(selected_domain);
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
                    var totalNetRevenue = totalRevenue - totalSpend;

                    $('#total_clicks_fb').text(formatNumber(totalClicksFb));
                    $('#total_clicks_adx').text(formatNumber(totalClicksAdx));
                    $('#total_spend').text(formatCurrencyIDR(totalSpend));
                    $('#roi_nett').text(formatNumber(roiNett, 2) + '%');
                    $('#total_revenue').text(formatCurrencyIDR(totalRevenue));
                    $('#total_net_revenue').text(formatCurrencyIDR(totalNetRevenue));

                    $('#summary_boxes').show();
                    showRoiDomainContent();
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
                    showRoiTrafficDomainChartPanel();
                    $('#chartRoiDailyEmpty').hide();
                    createROIDailyChart(displayData);
                    if (roiChart && typeof roiChart.resize === 'function') { roiChart.resize(); }
                } else {
                    if (roiChart) { roiChart.destroy(); roiChart = null; }
                    hideRoiTrafficDomainChartPanel();
                    $('#chart_roi_daily').hide();
                    $('#chartRoiDailyEmpty').show();
                }
                
                var table = window.ensureRoiDomainTable();
                table.clear();
                displayData.forEach(function (item) {
                    table.row.add(buildRoiDomainRow(item));
                });
                
                table.draw();
                showRoiDomainContent();
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
    $('.roi-filter-card .card-body').first().prepend(alertHtml);
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
    if (typeof Chart === 'undefined') {
        console.error('Chart.js is not loaded!');
        return;
    }
    if (!data || data.length === 0) {
        if (roiChart) { roiChart.destroy(); roiChart = null; }
        $('#chart_roi_daily').hide();
        $('#chartRoiDailyEmpty').show();
        return;
    }
    $('#chart_roi_daily').show();
    $('#chartRoiDailyEmpty').hide();

    var theme = getRoiChartTheme();

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
    const profitData = [];
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
        profitData.push(dailyRevenue[date] - dailySpend[date]);
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
            datasets: [
                {
                    label: 'Profit (Rp)',
                    data: profitData,
                    type: 'bar',
                    yAxisID: 'y1',
                    backgroundColor: 'rgba(34, 197, 94, 0.35)',
                    borderColor: 'rgba(34, 197, 94, 1)',
                    borderWidth: 1,
                    order: 0
                },
                {
                    label: 'ROI Harian (%)',
                    data: avgROIData,
                    borderColor: 'rgba(13, 148, 136, 1)',
                    backgroundColor: 'rgba(13, 148, 136, 0.12)',
                    borderWidth: 2.5,
                    fill: true,
                    tension: 0.35,
                    pointBackgroundColor: 'rgba(13, 148, 136, 1)',
                    pointBorderColor: '#fff',
                    pointBorderWidth: 2,
                    pointRadius: 5,
                    yAxisID: 'y',
                    order: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: { display: false },
                legend: {
                    display: true,
                    position: 'top',
                    labels: { color: theme.text, boxWidth: 12, padding: 14, font: { size: 11 } }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: theme.tooltipBg,
                    borderColor: theme.tooltipBorder,
                    borderWidth: 1,
                    titleColor: theme.text,
                    bodyColor: theme.text,
                    callbacks: {
                        label: function (context) {
                            const dataIndex = context.dataIndex;
                            const dsLabel = context.dataset && context.dataset.label ? context.dataset.label : '';
                            const revenue = revenueData[dataIndex];
                            const spend = spendData[dataIndex];

                            if (dsLabel === 'Profit (Rp)') {
                                const v = (context.parsed && context.parsed.y !== undefined) ? context.parsed.y : context.raw;
                                return [`Profit: ${formatCurrencyIDR(v)}`];
                            }

                            const roi = context.parsed.y;
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
                    title: { display: true, text: 'Tanggal', font: { weight: 'bold' }, color: theme.text },
                    ticks: { color: theme.muted },
                    grid: { display: true, color: theme.grid }
                },
                y: {
                    display: true,
                    title: { display: true, text: 'ROI (%)', font: { weight: 'bold' }, color: theme.text },
                    ticks: {
                        color: theme.muted,
                        callback: function (value) { return value + '%'; }
                    },
                    grid: { display: true, color: theme.grid }
                },
                y1: {
                    display: true,
                    position: 'right',
                    title: { display: true, text: 'Profit (Rp)', font: { weight: 'bold' }, color: theme.text },
                    grid: { drawOnChartArea: false },
                    ticks: {
                        color: theme.muted,
                        callback: function (value) { return formatCurrencyIDR(value); }
                    }
                }
            },
            interaction: { mode: 'nearest', axis: 'x', intersect: false }
        }
    });

    if ($('#btnToggleChart').is(':visible')) {
        setRoiTrafficDomainChartVisible(typeof window.__roiTrafficDomainChartVisible === 'boolean'
            ? window.__roiTrafficDomainChartVisible
            : isRoiTrafficDomainChartVisible(), false);
    }
}