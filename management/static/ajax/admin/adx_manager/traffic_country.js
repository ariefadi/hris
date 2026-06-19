function normalizeDomainFilter(selected_domain) {
    if (Array.isArray(selected_domain)) {
        return selected_domain.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
    }
    return String(selected_domain || '').trim();
}

function isAdxDarkTheme() {
    return document.documentElement.getAttribute('data-theme') === 'dark';
}

function getAdxChartTheme() {
    var dark = isAdxDarkTheme();
    return {
        text: dark ? '#e2e8f0' : '#334155',
        muted: dark ? '#94a3b8' : '#64748b',
        grid: dark ? 'rgba(148, 163, 184, 0.12)' : 'rgba(15, 23, 42, 0.08)',
        tooltipBg: dark ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.98)',
        tooltipBorder: dark ? 'rgba(255,255,255,0.1)' : 'rgba(15, 23, 42, 0.1)'
    };
}

function showAdxTrafficLoader(message) {
    var msg = String(message || 'Memuat data traffic per negara...').trim();
    if (window.HrisLoader && typeof window.HrisLoader.show === 'function') {
        window.HrisLoader.show(msg);
        return;
    }
    var $overlay = $('#overlay');
    if ($overlay.length) {
        $overlay.attr('data-loader-message', msg);
        $overlay.show();
    }
}

function hideAdxTrafficLoader() {
    if (window.HrisLoader && typeof window.HrisLoader.forceHide === 'function') {
        window.HrisLoader.forceHide();
        return;
    }
    $('#overlay').hide();
}

function showAdxTrafficResults() {
    $('#adxTrafficEmptyState').hide();
    $('#adxTrafficResults').show();
}

function hideAdxTrafficResults() {
    $('#adxTrafficResults').hide();
    $('#adxTrafficEmptyState').show();
}

function formatNumber(num, decimals) {
    decimals = decimals === undefined ? 0 : decimals;
    if (num === null || num === undefined) return '0';
    return parseFloat(num).toLocaleString('id-ID', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

function formatCurrencyIDR(value) {
    var numValue = parseFloat(String(value || '').replace(/[$,]/g, ''));
    if (isNaN(numValue)) return value;
    return 'Rp ' + Math.round(numValue).toLocaleString('id-ID');
}

var HRIS_WORLD_MAP_TOPO_URL = 'https://cdn.jsdelivr.net/npm/@highcharts/map-collection@2.1.0/custom/world.topo.json';

function loadWorldMapTopology() {
    if (window.__hrisWorldMapTopology) {
        return Promise.resolve(window.__hrisWorldMapTopology);
    }
    if (!window.__hrisWorldMapTopologyPromise) {
        window.__hrisWorldMapTopologyPromise = fetch(HRIS_WORLD_MAP_TOPO_URL, { credentials: 'omit' })
            .then(function (res) {
                if (!res.ok) throw new Error('Gagal memuat data peta dunia (HTTP ' + res.status + ')');
                return res.json();
            })
            .then(function (topology) {
                window.__hrisWorldMapTopology = topology;
                return topology;
            })
            .catch(function (err) {
                window.__hrisWorldMapTopologyPromise = null;
                throw err;
            });
    }
    return window.__hrisWorldMapTopologyPromise;
}

var ADX_COUNTRY_MAP_VISIBLE_KEY = 'adxTrafficCountryMapVisible';

function isCountryMapVisible() {
    try {
        return localStorage.getItem(ADX_COUNTRY_MAP_VISIBLE_KEY) !== '0';
    } catch (e) {
        return true;
    }
}

function reflowCountryMap() {
    if (window.countryMapInstance && typeof window.countryMapInstance.reflow === 'function') {
        try { window.countryMapInstance.reflow(); } catch (e) { }
    }
}

function setCountryMapVisible(visible, animate) {
    window.__adxCountryMapVisible = !!visible;
    try {
        localStorage.setItem(ADX_COUNTRY_MAP_VISIBLE_KEY, visible ? '1' : '0');
    } catch (e) { }

    var $section = $('#charts_section');
    var $wrap = $section.find('.adx-country-map-wrap');
    var $btn = $('#btnToggleCountryMap');
    if (!$section.length || !$wrap.length || !$btn.length) return;

    $btn.attr('aria-expanded', visible ? 'true' : 'false');
    if (visible) {
        $btn.html('<i class="fas fa-eye-slash" aria-hidden="true"></i> Sembunyikan Peta');
        $section.removeClass('map-collapsed');
    } else {
        $btn.html('<i class="fas fa-eye" aria-hidden="true"></i> Tampilkan Peta');
        $section.addClass('map-collapsed');
    }

    if (animate) {
        if (visible) {
            $wrap.stop(true, true).slideDown(200, reflowCountryMap);
        } else {
            $wrap.stop(true, true).slideUp(200);
        }
        return;
    }

    $wrap.toggle(visible);
    if (visible) reflowCountryMap();
}

$(document).ready(function () {
    if (window.HrisDatepicker) {
        HrisDatepicker.initRange('#tanggal_dari', '#tanggal_sampai');
    }
    // Initialize Select2 for account
    $('#account_filter').select2({
        placeholder: '-- Pilih Account Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });
    let allAccountOptions = $('#account_filter').html();  
    // Select2 untuk Filter Subdomain: searchable + freetext (tagging) + AJAX suggest
    $('#domain_filter').select2({
        placeholder: 'ketik subdomain…',
        allowClear: true,
        width: '100%',
        theme: 'bootstrap4',
        tags: true,
        tokenSeparators: [','],
        minimumInputLength: 1,
        ajax: {
            url: '/management/admin/adx_domain_suggest',
            dataType: 'json',
            delay: 250,
            data: function (params) {
                var selected_account = $('#account_filter').val() || [];
                return {
                    q: params.term || '',
                    start_date: $('#tanggal_dari').val() || '',
                    end_date: $('#tanggal_sampai').val() || '',
                    selected_account: (selected_account && selected_account.length) ? selected_account.join(',') : ''
                };
            },
            processResults: function (data) {
                return { results: (data && data.results) ? data.results : [] };
            },
            cache: true
        },
        createTag: function (params) {
            var term = $.trim(params.term || '');
            if (!term) return null;
            return { id: term, text: term, newTag: true };
        }
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
    hideAdxTrafficResults();

    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domain = normalizeDomainFilter($("#domain_filter").val());
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            showAdxTrafficLoader();
            load_country_options(selected_account, selected_domain);
            load_adx_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selected_domain);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    $('#btnToggleCountryMap').on('click', function (e) {
        e.preventDefault();
        setCountryMapVisible(!window.__adxCountryMapVisible, true);
    });
    // Filter silang account-domain dinonaktifkan karena domain menggunakan freetext.
    // Fungsi untuk memuat opsi negara ke select2
    function load_country_options(selected_account, selectedDomains) {
        selectedDomains = normalizeDomainFilter(selectedDomains);
        var accountFilter = '';
        if (Array.isArray(selected_account)) {
            accountFilter = selected_account.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
        } else {
            accountFilter = String(selected_account || '').trim();
        }
        // Simpan pilihan country yang sudah dipilih sebelumnya
        var previouslySelected = $("#country_filter").val() || [];
        $.ajax({
            url: '/management/admin/get_countries_adx',
            type: 'GET',
            dataType: 'json',
            data: {
                'selected_accounts': accountFilter,
                'selected_domains': selectedDomains
            },
            headers: {
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': csrftoken
            },
            success: function (response) {
                if (response && (response.status === true || response.status === 'success')) {
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
        var accountFilter = '';
        if (selected_account && selected_account.length > 0) {
            accountFilter = selected_account.join(',');
        }
        // Convert array to comma-separated string for backend
        var domainFilter = normalizeDomainFilter(selectedDomains);
        // Convert array to comma-separated string for backend
        var countryFilter = '';
        if (selectedCountries && selectedCountries.length > 0) {
            countryFilter = selectedCountries.join(',');
        }
        // Tampilkan overlay loading
        showAdxTrafficLoader();
        // Destroy existing DataTable if exists
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().destroy();
        }
        // AJAX request
        $.ajax({
            url: '/management/admin/page_adx_traffic_country',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                selected_account: accountFilter,
                selected_domains: domainFilter,
                selected_countries: countryFilter
            },
            headers: {
                'X-CSRFToken': csrftoken
            },
            success: function (response) {
                hideAdxTrafficLoader();
                if (response && response.status) {
                    showAdxTrafficResults();
                    updateSummaryBoxes(response.summary);
                    initializeDataTable(response.data);
                    if (response.data && response.data.length > 0) {
                        createCountryMap(response.data);
                        $('#charts_section').show();
                    } else {
                        $('#charts_section').hide();
                    }
                } else {
                    hideAdxTrafficResults();
                    var errorMsg = response.error || 'Terjadi kesalahan yang tidak diketahui';
                    alert('Error: ' + errorMsg);
                }
            },
            error: function (xhr, status, error) {
                hideAdxTrafficLoader();
                hideAdxTrafficResults();
                report_eror(xhr, status);
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

    function getExportMetaTrafficCountry() {
        var start = $('#tanggal_dari').val();
        var end = $('#tanggal_sampai').val();
        var titleText = 'Traffic AdX Per Negara';
        var periodText = 'Periode ' + formatDateID(start) + ' s/d ' + formatDateID(end);

        var accounts = getSelectedTextList('#account_filter');
        var domainsRaw = String($('#domain_filter').val() || '').trim();
        var domains = domainsRaw ? domainsRaw.split(',').map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }) : [];

        return {
            titleText: titleText,
            periodText: periodText,
            accountText: accounts.length ? ('Account: ' + accounts.join(', ')) : '',
            domainText: domains.length ? ('Domain: ' + domains.join(', ')) : ''
        };
    }

    // Fungsi untuk update summary boxes
    function updateSummaryBoxes(data) {
        data = data || {};
        var totalCtrRatio = parseFloat(data.total_ctr || 0) || 0;
        $('#total_impressions').text(formatNumber(data.total_impressions || 0));
        $('#total_clicks').text(formatNumber(data.total_clicks || 0));
        $('#total_ctr').text(totalCtrRatio > 0 ? formatNumber(totalCtrRatio * 100, 2) + '%' : '0%');
        $('#total_revenue').text(formatCurrencyIDR(data.total_revenue || 0));
    }

    // Fungsi untuk inisialisasi DataTable
    function initializeDataTable(data) {
        window.__adxTrafficCountryRows = (data && Array.isArray(data)) ? data : [];

        var tableData = [];
        if (window.__adxTrafficCountryRows.length) {
            window.__adxTrafficCountryRows.forEach(function (row, idx) {
                var code = String(row.country_code || '').toUpperCase();
                var flagHtml = '';
                if (code) {
                    flagHtml = '<img src="https://flagcdn.com/16x12/' + code.toLowerCase() + '.png" alt="' + escapeHtml(code) + '" width="16" height="12">';
                }
                var countryName = escapeHtml(row.country_name || '-');
                var cellCountry = '<div class="country-cell">' + flagHtml
                    + '<span class="country-name" title="' + countryName + '">' + countryName + '</span></div>';
                var cellCode = code
                    ? '<span class="country-code-badge">' + escapeHtml(code) + '</span>'
                    : '-';

                var impressionsNum = Number(row.impressions || 0);
                var clicksNum = Number(row.clicks || 0);
                var ctrNum = parseFloat(row.ctr);
                if (isNaN(ctrNum)) ctrNum = 0;
                var cpcNum = parseFloat(row.cpc || 0) || 0;
                var ecpmNum = parseFloat(row.ecpm || 0) || 0;
                var revenueNum = parseFloat(row.revenue || 0) || 0;

                var btnDetail = '<button type="button" class="btn btn-sm btn-outline-primary btn-adx-traffic-country-detail btn-detail-row" data-row-index="' + idx + '" title="Detail">'
                    + '<i class="bi bi-eye-fill" aria-hidden="true"></i>'
                    + '</button>';

                tableData.push([
                    cellCountry,
                    cellCode,
                    impressionsNum,
                    clicksNum,
                    ctrNum,
                    cpcNum,
                    ecpmNum,
                    revenueNum,
                    btnDetail
                ]);
            });
        }
        // Destroy existing DataTable if it exists
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().destroy();
        }
        var table = $('#table_traffic_country').DataTable({
            data: tableData,
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
            buttons: [
                {
                    extend: 'excel',
                    text: 'Export Excel',
                    className: 'btn btn-success',
                    exportOptions: { columns: ':visible:not(.no-export)' },
                    title: function () { 
                        var meta = getExportMetaTrafficCountry();
                        let title = 'Traffic AdX Per Negara';
                        if (meta.periodText) title += ' ' + meta.periodText;
                        if (meta.accountText) title += ' ' + meta.accountText;
                        if (meta.domainText) title += ' ' + meta.domainText;
                        return title;
                    },
                    customize: function (xlsx) {
                        var meta = getExportMetaTrafficCountry();
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

                        for (var i = headerRows.length; i >= 1; i--) {
                            var txt = escapeXmlText(headerRows[i - 1]);
                            var rowXml = '<row r="' + i + '"><c t="inlineStr" r="A' + i + '" s="51"><is><t>' + txt + '</t></is></c></row>';
                            $('sheetData', sheet).prepend(rowXml);
                        }

                        var merges = $('mergeCells', sheet);
                        var mergeXml = '';
                        for (var m = 1; m <= headerRows.length; m++) {
                            mergeXml += '<mergeCell ref="A' + m + ':N' + m + '"/>';
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
                    exportOptions: { columns: ':visible:not(.no-export)' },
                    title: function () { 
                        var meta = getExportMetaTrafficCountry();
                        let title = 'Traffic AdX Per Negara';
                        if (meta.periodText) title += ' ' + meta.periodText;
                        if (meta.accountText) title += ' ' + meta.accountText;
                        if (meta.domainText) title += ' ' + meta.domainText;
                        return title;
                    },
                    customize: function (doc) {
                        var meta = getExportMetaTrafficCountry();
                        var inserts = [];
                        inserts.push({ text: meta.periodText, style: 'header', alignment: 'center', margin: [0, 0, 0, 6] });
                        if (meta.accountText) inserts.push({ text: meta.accountText, alignment: 'center', margin: [0, 0, 0, 4] });
                        if (meta.domainText) inserts.push({ text: meta.domainText, alignment: 'center', margin: [0, 0, 0, 8] });
                        doc.content.splice(1, 0, ...inserts);
                    }
                },
                {
                    extend: 'copy',
                    text: 'Copy',
                    className: 'btn btn-info',
                    exportOptions: { columns: ':visible:not(.no-export)' },
                    customize: function (txt) {
                        var meta = getExportMetaTrafficCountry();
                        var header = meta.titleText + '\n' + meta.periodText;
                        if (meta.accountText) header += '\n' + meta.accountText;
                        if (meta.domainText) header += '\n' + meta.domainText;
                        header += '\n\n';
                        return header + txt;
                    }
                },
                {
                    extend: 'csv',
                    text: 'Export CSV',
                    className: 'btn btn-primary',
                    exportOptions: { columns: ':visible:not(.no-export)' },
                    customize: function (csv) {
                        var meta = getExportMetaTrafficCountry();
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
                    exportOptions: { columns: ':visible:not(.no-export)' },
                    title: function () { 
                        var meta = getExportMetaTrafficCountry();
                        let title = '<h3 style="text-align:center;margin:0">Traffic AdX Per Negara</h3>';
                        if (meta.periodText) title += ' ' + meta.periodText;
                        if (meta.accountText) title += ' ' + meta.accountText;
                        if (meta.domainText) title += ' ' + meta.domainText;
                        return title;
                    },
                    messageTop: function () {
                        var meta = getExportMetaTrafficCountry();
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
                    exportOptions: { columns: ':visible:not(.no-export)' }
                }
            ],
            columnDefs: [
                { targets: [1, 4, 8], className: 'text-center' },
                { targets: [2, 3, 5, 6, 7], className: 'text-right' },
                {
                    targets: [2, 3],
                    type: 'num',
                    render: function (data, type) {
                        if (type === 'display') return Number(data || 0).toLocaleString('id-ID');
                        return Number(data || 0);
                    }
                },
                {
                    targets: 4,
                    type: 'num',
                    render: function (data, type) {
                        var v = parseFloat(data);
                        if (isNaN(v)) v = 0;
                        if (type === 'display') return v.toFixed(2) + '%';
                        return v;
                    }
                },
                {
                    targets: [5, 6, 7],
                    type: 'num',
                    render: function (data, type) {
                        if (type === 'display') return formatCurrencyIDR(data || 0);
                        return Number(data || 0);
                    }
                },
                {
                    targets: 8,
                    orderable: false,
                    searchable: false,
                    className: 'text-center no-export'
                }
            ]
        });

        // Paksa urutan setelah inisialisasi untuk memastikan tidak tertimpa
        table.order([7, 'desc']).draw();

        $('#table_traffic_country tbody')
            .off('click', '.btn-adx-traffic-country-detail')
            .on('click', '.btn-adx-traffic-country-detail', function () {
                var idx = parseInt($(this).attr('data-row-index') || '0', 10);
                var row = (window.__adxTrafficCountryRows || [])[idx] || {};

                $('#adxTrafficCountryDetailCountryName').text(escapeHtml(row.country_name || '-'));
                $('#adxTrafficCountryDetailCountryCode').text(escapeHtml(row.country_code || '-'));

                var ctr = parseFloat(row.ctr);
                if (isNaN(ctr)) ctr = 0;

                $('#adxTrafficCountryDetailImpressions').text(formatNumber(row.impressions || 0));
                $('#adxTrafficCountryDetailClicks').text(formatNumber(row.clicks || 0));
                $('#adxTrafficCountryDetailCtr').text(formatNumber(ctr, 2) + ' %');
                $('#adxTrafficCountryDetailCpc').text(formatCurrencyIDR(row.cpc || 0));
                $('#adxTrafficCountryDetailEcpm').text(formatCurrencyIDR(row.ecpm || 0));
                $('#adxTrafficCountryDetailRevenue').text(formatCurrencyIDR(row.revenue || 0));

                $('#adxTrafficCountryDetailTotalRequests').text(formatNumber(row.total_requests || 0));
                $('#adxTrafficCountryDetailResponsesServed').text(formatNumber(row.responses_served || 0));

                var matchRate = parseFloat(row.match_rate);
                if (isNaN(matchRate)) matchRate = 0;
                var fillRate = parseFloat(row.fill_rate);
                if (isNaN(fillRate)) fillRate = 0;
                var avPct = parseFloat(row.active_view_pct_viewable);
                if (isNaN(avPct)) avPct = 0;
                var avTime = parseFloat(row.active_view_avg_time_sec);
                if (isNaN(avTime)) avTime = 0;

                $('#adxTrafficCountryDetailMatchRate').text(matchRate.toFixed(2) + ' %');
                $('#adxTrafficCountryDetailFillRate').text(fillRate.toFixed(2) + ' %');
                $('#adxTrafficCountryDetailActiveViewPctViewable').text(avPct.toFixed(2) + ' %');
                $('#adxTrafficCountryDetailActiveViewAvgTimeSec').text(avTime.toFixed(2));

                $('#adxTrafficCountryDetailModal').modal('show');
            });

        try { table.columns.adjust(); } catch (e) {}
    }

    // Fungsi untuk membuat peta dengan Highcharts Maps
    function createCountryMap(data) {
        if (!data || data.length === 0) {
            if (window.countryMapInstance) {
                try { window.countryMapInstance.destroy(); } catch (e) { }
                window.countryMapInstance = null;
            }
            $('#charts_section').hide();
            $('#btnToggleCountryMap').hide();
            return;
        }

        var mapData = [];
        data.forEach(function (item) {
            var revenue = parseFloat(item.revenue) || 0;
            var countryCode = item.country_code;
            if (revenue > 0 && countryCode) {
                mapData.push({
                    'hc-key': String(countryCode).toLowerCase(),
                    code: countryCode,
                    name: item.country_name || 'Unknown',
                    value: revenue
                });
            }
        });

        if (window.countryMapInstance) {
            try { window.countryMapInstance.destroy(); } catch (e) { }
            window.countryMapInstance = null;
        }

        try {
            if (typeof Highcharts === 'undefined' || !Highcharts.mapChart) {
                throw new Error('Highcharts Maps library not loaded');
            }

            $('#worldMap').css({ height: '480px', width: '100%', display: 'block', visibility: 'visible' }).empty();

            if (mapData.length === 0) {
                $('#worldMap').html('<div class="adx-traffic-empty" style="padding:80px 24px;"><div class="adx-traffic-empty-title">Tidak ada data pendapatan</div><div>Negara yang dipilih belum memiliki pendapatan pada periode ini.</div></div>');
                $('#btnToggleCountryMap').show();
                setCountryMapVisible(isCountryMapVisible(), false);
                return;
            }

            var theme = getAdxChartTheme();
            var nullAreaColor = isAdxDarkTheme() ? '#334155' : '#e2e8f0';
            var borderColor = isAdxDarkTheme() ? '#475569' : '#cbd5e1';
            var legendBg = isAdxDarkTheme() ? 'rgba(15, 23, 42, 0.88)' : 'rgba(255, 255, 255, 0.94)';

            var ranges = isAdxDarkTheme()
                ? [
                    { from: null, to: null, color: nullAreaColor, name: 'Tidak ada data' },
                    { from: 0, to: 50000, color: '#312e81', name: 'Rp 0 – 50 rb' },
                    { from: 50000, to: 100000, color: '#3730a3', name: 'Rp 50 rb – 100 rb' },
                    { from: 100000, to: 500000, color: '#4338ca', name: 'Rp 100 rb – 500 rb' },
                    { from: 500000, to: 1000000, color: '#4f46e5', name: 'Rp 500 rb – 1 jt' },
                    { from: 1000000, to: 5000000, color: '#6366f1', name: 'Rp 1 jt – 5 jt' },
                    { from: 5000000, to: 10000000, color: '#818cf8', name: 'Rp 5 jt – 10 jt' },
                    { from: 10000000, to: Infinity, color: '#c7d2fe', name: '> Rp 10 jt' }
                ]
                : [
                    { from: null, to: null, color: nullAreaColor, name: 'Tidak ada data' },
                    { from: 0, to: 50000, color: '#eef2ff', name: 'Rp 0 – 50 rb' },
                    { from: 50000, to: 100000, color: '#c7d2fe', name: 'Rp 50 rb – 100 rb' },
                    { from: 100000, to: 500000, color: '#a5b4fc', name: 'Rp 100 rb – 500 rb' },
                    { from: 500000, to: 1000000, color: '#818cf8', name: 'Rp 500 rb – 1 jt' },
                    { from: 1000000, to: 5000000, color: '#6366f1', name: 'Rp 1 jt – 5 jt' },
                    { from: 5000000, to: 10000000, color: '#4f46e5', name: 'Rp 5 jt – 10 jt' },
                    { from: 10000000, to: Infinity, color: '#312e81', name: '> Rp 10 jt' }
                ];

            loadWorldMapTopology().then(function (topology) {
                window.countryMapInstance = Highcharts.mapChart('worldMap', {
                    chart: {
                        map: topology,
                        backgroundColor: 'transparent',
                        style: { fontFamily: 'inherit' },
                        spacing: [8, 8, 8, 8]
                    },
                    title: { text: null },
                    credits: { enabled: false },
                    mapNavigation: {
                        enabled: true,
                        enableButtons: true,
                        buttonOptions: {
                            verticalAlign: 'bottom',
                            theme: {
                                fill: isAdxDarkTheme() ? '#1e293b' : '#ffffff',
                                'stroke-width': 1,
                                stroke: borderColor,
                                r: 8,
                                states: { hover: { fill: '#6366f1', style: { color: '#fff' } } }
                            }
                        }
                    },
                    colorAxis: {
                        min: 0,
                        dataClasses: ranges.map(function (range) {
                            return { from: range.from, to: range.to, color: range.color, name: range.name };
                        })
                    },
                    legend: {
                        title: { text: 'Tingkat Pendapatan', style: { color: theme.text, fontWeight: '700', fontSize: '12px' } },
                        align: 'left',
                        verticalAlign: 'bottom',
                        floating: true,
                        layout: 'vertical',
                        backgroundColor: legendBg,
                        borderColor: theme.tooltipBorder,
                        borderRadius: 10,
                        borderWidth: 1,
                        padding: 10,
                        itemStyle: { color: theme.text, fontSize: '11px' },
                        itemMarginBottom: 4,
                        symbolRadius: 4,
                        symbolHeight: 12
                    },
                    series: [{
                        name: 'Pendapatan',
                        data: mapData,
                        joinBy: ['hc-key', 'hc-key'],
                        nullColor: nullAreaColor,
                        borderColor: borderColor,
                        borderWidth: 0.6,
                        allAreas: true,
                        states: {
                            hover: { color: '#f59e0b', borderColor: '#d97706' },
                            select: { color: '#ec4899' }
                        },
                        tooltip: {
                            backgroundColor: theme.tooltipBg,
                            borderColor: theme.tooltipBorder,
                            borderRadius: 10,
                            style: { color: theme.text },
                            pointFormat: '<b>{point.name}</b><br/>Kode: <b>{point.code}</b><br/>Pendapatan: <b>Rp {point.value:,.0f}</b>',
                            nullFormat: '<b>{point.name}</b><br/>Tidak ada data traffic'
                        }
                    }],
                    exporting: {
                        enabled: true,
                        buttons: {
                            contextButton: {
                                theme: {
                                    fill: isAdxDarkTheme() ? '#1e293b' : '#ffffff',
                                    stroke: borderColor,
                                    r: 8
                                },
                                menuItems: ['viewFullscreen', 'separator', 'downloadPNG', 'downloadJPEG', 'downloadPDF', 'downloadSVG']
                            }
                        }
                    }
                });

                $('#btnToggleCountryMap').show();
                setCountryMapVisible(isCountryMapVisible(), false);
            }).catch(function (mapErr) {
                console.error('[ERROR] Failed to load world map topology:', mapErr);
                $('#worldMap').html('<div class="adx-traffic-empty" style="padding:60px 24px;"><div class="adx-traffic-empty-title">Gagal memuat peta</div><div>' + escapeHtml(mapErr.message || String(mapErr)) + '</div></div>');
            });
        } catch (error) {
            console.error('[ERROR] Failed to create map:', error);
            $('#worldMap').html('<div class="adx-traffic-empty" style="padding:60px 24px;"><div class="adx-traffic-empty-title">Gagal memuat peta</div><div>' + escapeHtml(error.message) + '</div></div>');
        }
    }
    function report_eror(jqXHR, exception) {
        var msg = '';
        if (typeof jqXHR === 'string') {
            alert(jqXHR);
            return;
        }
        if (jqXHR && jqXHR.status === 0) {
            msg = 'TIDAK ADA KONEKSI.\n TOLONG HUBUNGI DEVELOPER';
        } else if (jqXHR && jqXHR.status == 404) {
            msg = 'Requested page not found. [404]';
        } else if (jqXHR && jqXHR.status == 500) {
            msg = 'Internal Server Error [500].';
        } else if (exception === 'parsererror') {
            msg = 'Requested JSON parse failed.';
        } else if (exception === 'timeout') {
            msg = 'Time out error.';
        } else if (exception === 'abort') {
            msg = 'Ajax request aborted.';
        } else {
            msg = 'Uncaught Error.\n' + (jqXHR && jqXHR.responseText ? jqXHR.responseText : String(exception || ''));
        }
        alert(msg);
    }
});

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