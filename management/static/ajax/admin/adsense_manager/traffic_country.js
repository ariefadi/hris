/**
 * Reference Ajax AdSense Traffic Per Country
 */
function normalizeDomainFilter(selected_domain) {
    if (Array.isArray(selected_domain)) {
        return selected_domain.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
    }
    return String(selected_domain || '').trim();
}

function isAdsenseDarkTheme() {
    return document.documentElement.getAttribute('data-theme') === 'dark';
}

function getAdsenseChartTheme() {
    var dark = isAdsenseDarkTheme();
    return {
        text: dark ? '#e2e8f0' : '#334155',
        muted: dark ? '#94a3b8' : '#64748b',
        grid: dark ? 'rgba(148, 163, 184, 0.12)' : 'rgba(15, 23, 42, 0.08)',
        tooltipBg: dark ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.98)',
        tooltipBorder: dark ? 'rgba(255,255,255,0.1)' : 'rgba(15, 23, 42, 0.1)'
    };
}

function showAdsenseTrafficLoader(message) {
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

function hideAdsenseTrafficLoader() {
    if (window.HrisLoader && typeof window.HrisLoader.forceHide === 'function') {
        window.HrisLoader.forceHide();
        return;
    }
    $('#overlay').hide();
}

function showAdsenseTrafficResults() {
    $('#adsenseTrafficEmptyState').hide();
    $('#adsenseTrafficResults').show();
}

function hideAdsenseTrafficResults() {
    $('#adsenseTrafficResults').hide();
    $('#adsenseTrafficEmptyState').show();
}

function formatNumber(num, decimals) {
    decimals = decimals === undefined ? 0 : decimals;
    if (num === null || num === undefined || isNaN(num)) return '0';
    return parseFloat(num).toLocaleString('id-ID', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

function formatCurrencyIDR(value) {
    var numValue = parseFloat(String(value || '').replace(/[$,]/g, ''));
    if (isNaN(numValue)) numValue = 0;
    return 'Rp ' + Math.round(numValue).toLocaleString('id-ID');
}

var ADSENSE_COUNTRY_MAP_VISIBLE_KEY = 'adsenseTrafficCountryMapVisible';

function isCountryMapVisible() {
    try {
        return localStorage.getItem(ADSENSE_COUNTRY_MAP_VISIBLE_KEY) !== '0';
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
    window.__adsenseCountryMapVisible = !!visible;
    try {
        localStorage.setItem(ADSENSE_COUNTRY_MAP_VISIBLE_KEY, visible ? '1' : '0');
    } catch (e) { }

    var $section = $('#charts_section');
    var $wrap = $section.find('.adsense-country-map-wrap');
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
    report_eror = function (jqXHR, exception) {
        // Support pemanggilan report_eror("pesan")
        if (typeof jqXHR === 'string') {
            alert(jqXHR);
            return;
        }

        var msg = '';
        var status = (jqXHR && typeof jqXHR.status !== 'undefined') ? jqXHR.status : null;
        if (status === 0) {
            msg = 'TIDAK ADA KONEKSI.\n TOLONG HUBUNGI DEVELOPER';
        } else if (status == 404) {
            msg = 'Requested page not found. [404]';
        } else if (status == 500) {
            msg = 'Internal Server Error [500].';
        } else if (exception === 'parsererror') {
            msg = 'Requested JSON parse failed.';
        } else if (exception === 'timeout') {
            msg = 'Time out error.';
        } else if (exception === 'abort') {
            msg = 'Ajax request aborted.';
        } else {
            var detail = '';
            try {
                detail = (jqXHR && jqXHR.responseText) ? jqXHR.responseText : '';
            } catch (e) {
                detail = '';
            }
            msg = 'Uncaught Error.\n' + (detail || exception || 'Unknown error');
        }
        alert(msg);
    };
    // Initialize date pickers (Flatpickr first, fallback ke jQuery datepicker)
    var today = new Date();
    if (typeof flatpickr !== 'undefined') {
        flatpickr('#tanggal_dari', {
            dateFormat: 'Y-m-d',
            defaultDate: today
        });
        flatpickr('#tanggal_sampai', {
            dateFormat: 'Y-m-d',
            defaultDate: today
        });
    } else {
        // Fallback: gunakan jQuery datepicker jika Flatpickr tidak tersedia
        $('#tanggal_dari').datepicker({
            format: 'yyyy-mm-dd',
            autoclose: true,
            todayHighlight: true
        }).datepicker('setDate', today);
        $('#tanggal_sampai').datepicker({
            format: 'yyyy-mm-dd',
            autoclose: true,
            todayHighlight: true
        }).datepicker('setDate', today);
    }
    // Initialize Select2 for account filter
    $('#account_filter').select2({
        placeholder: '-- Pilih Account Terdaftar --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });

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
            url: '/management/admin/adsense_domain_suggest',
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
    let allAccountOptions = $('#account_filter').html();  
    // Initialize Select2 for country filter (multi-select with tags)
    $('#country_filter').select2({
        placeholder: 'Pilih Negara (Opsional)',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4',
        tags: true,
        tokenSeparators: [','],
        closeOnSelect: false
    });
    hideAdsenseTrafficResults();

    $('#btn_load_data').click(function (e) {
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#account_filter").val();
        var selected_domains = normalizeDomainFilter($("#domain_filter").val());
        if (tanggal_dari != "" && tanggal_sampai != "") {
            e.preventDefault();
            showAdsenseTrafficLoader();
            load_country_options(selected_account);
            load_adsense_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selected_domains);
        } else {
            alert('Silakan pilih tanggal dari dan sampai');
        }
    });
    $('#btnToggleCountryMap').on('click', function (e) {
        e.preventDefault();
        setCountryMapVisible(!window.__adsenseCountryMapVisible, true);
    });
    // Fungsi untuk memuat opsi negara ke select2
    function load_country_options(selected_account) {
        // Simpan pilihan country yang sudah dipilih sebelumnya
        var previouslySelected = $("#country_filter").val() || [];
        var accountFilter = '';
        if (Array.isArray(selected_account)) {
            accountFilter = selected_account.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
        } else {
            accountFilter = String(selected_account || '').trim();
        }
        $.ajax({
            url: '/management/admin/get_countries_adsense',
            type: 'GET',
            dataType: 'json',
            data: {
                'selected_accounts': accountFilter
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

    // Fungsi untuk load data traffic per country
    function load_adsense_traffic_country_data(tanggal_dari, tanggal_sampai, selected_account, selected_domains) {
        var startDate = tanggal_dari;
        var endDate = tanggal_sampai;
        var selected_account = selected_account;
        var selectedDomains = String(selected_domains || '').trim();
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
        var countryFilter = '';
        if (selectedCountries && selectedCountries.length > 0) {
            countryFilter = selectedCountries.join(',');
        }
        showAdsenseTrafficLoader();
        // Destroy existing DataTable if exists
        if ($.fn.DataTable.isDataTable('#table_traffic_country')) {
            $('#table_traffic_country').DataTable().destroy();
        }
        // AJAX request
        $.ajax({
            url: '/management/admin/adsense_traffic_country_data',
            type: 'GET',
            data: {
                start_date: startDate,
                end_date: endDate,
                selected_account: accountFilter,
                selected_domains: selectedDomains,
                selected_countries: countryFilter
            },
            headers: {
                'X-CSRFToken': csrftoken
            },
            success: function (response) {
                hideAdsenseTrafficLoader();
                if (response && response.status) {
                    showAdsenseTrafficResults();
                    updateSummaryBoxes(response.summary);
                    initializeDataTable(response.data);
                    if (response.data && response.data.length > 0) {
                        createCountryMap(response.data);
                        $('#charts_section').show();
                    } else {
                        $('#charts_section').hide();
                    }
                } else {
                    hideAdsenseTrafficResults();
                    alert('Error: ' + (response.error || 'Terjadi kesalahan yang tidak diketahui'));
                }
            },
            error: function (xhr, status, error) {
                hideAdsenseTrafficLoader();
                hideAdsenseTrafficResults();
                report_eror(xhr, status || error);
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
        var titleText = 'Traffic AdSense Per Negara';
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
        window.__adsenseTrafficCountryRows = (data && Array.isArray(data)) ? data : [];

        var tableData = [];
        if (window.__adsenseTrafficCountryRows.length) {
            window.__adsenseTrafficCountryRows.forEach(function (row, idx) {
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

                var btnDetail = '<button type="button" class="btn btn-sm btn-outline-primary btn-adsense-traffic-country-detail btn-detail-row" data-row-index="' + idx + '" title="Detail">'
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
                        let title = 'Traffic AdSense Per Negara';
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
                            mergeXml += '<mergeCell ref="A' + m + ':I' + m + '"/>';
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
                        let title = 'Traffic AdSense Per Negara';
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
                        let title = '<h3 style="text-align:center;margin:0">Traffic AdSense Per Negara</h3>';
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
                    exportOptions: { columns: ':visible' }
                }
            ],
            columnDefs: [
                { targets: [1, 4, 8], className: 'text-center' },
                { targets: [2, 3, 5, 6, 7], className: 'text-right' },
                {
                    targets: [2, 3],
                    type: 'num',
                    render: function (data, type) {
                        if (type === 'display') return formatNumber(data || 0);
                        return Number(data || 0);
                    }
                },
                {
                    targets: 4,
                    type: 'num',
                    render: function (data, type) {
                        var v = parseFloat(data);
                        if (isNaN(v)) v = 0;
                        if (type === 'display') return formatNumber(v, 2) + ' %';
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
                { targets: 8, orderable: false, searchable: false, className: 'text-center no-export' }
            ]
        });

        // Paksa urutan setelah inisialisasi untuk memastikan tidak tertimpa
        table.order([7, 'desc']).draw();

        $('#table_traffic_country tbody')
            .off('click', '.btn-adsense-traffic-country-detail')
            .on('click', '.btn-adsense-traffic-country-detail', function () {
                var idx = parseInt($(this).attr('data-row-index') || '0', 10);
                var row = (window.__adsenseTrafficCountryRows || [])[idx] || {};
                var code = String(row.country_code || '').toUpperCase();

                $('#adsenseTrafficCountryDetailCountryName').text(row.country_name || '-');
                $('#adsenseTrafficCountryDetailCountryCode').text(code || '-');

                var $flag = $('#adsenseTrafficCountryDetailFlag');
                if (code) {
                    $flag.attr('src', 'https://flagcdn.com/32x24/' + code.toLowerCase() + '.png')
                        .attr('alt', code).show();
                } else {
                    $flag.hide();
                }

                var ctr = parseFloat(row.ctr);
                if (isNaN(ctr)) ctr = 0;

                $('#adsenseTrafficCountryDetailImpressions').text(formatNumber(row.impressions || 0));
                $('#adsenseTrafficCountryDetailClicks').text(formatNumber(row.clicks || 0));
                $('#adsenseTrafficCountryDetailCtr').text(formatNumber(ctr, 2) + ' %');
                $('#adsenseTrafficCountryDetailCpc').text(formatCurrencyIDR(row.cpc || 0));
                $('#adsenseTrafficCountryDetailEcpm').text(formatCurrencyIDR(row.ecpm || 0));
                $('#adsenseTrafficCountryDetailRevenue').text(formatCurrencyIDR(row.revenue || 0));

                $('#adsenseTrafficCountryDetailPageViews').text(formatNumber(row.page_views || 0));
                $('#adsenseTrafficCountryDetailPageViewsRpm').text(formatCurrencyIDR(row.page_views_rpm || 0));
                $('#adsenseTrafficCountryDetailAdRequests').text(formatNumber(row.ad_requests || 0));

                var cov = parseFloat(row.ad_requests_coverage);
                if (isNaN(cov)) cov = 0;
                var avv = parseFloat(row.active_view_viewability);
                if (isNaN(avv)) avv = 0;
                var avm = parseFloat(row.active_view_measurability);
                if (isNaN(avm)) avm = 0;
                var avt = parseFloat(row.active_view_time);
                if (isNaN(avt)) avt = 0;

                $('#adsenseTrafficCountryDetailAdRequestsCoverage').text(cov.toFixed(2) + ' %');
                $('#adsenseTrafficCountryDetailActiveViewViewability').text(avv.toFixed(2) + ' %');
                $('#adsenseTrafficCountryDetailActiveViewMeasurability').text(avm.toFixed(2) + ' %');
                $('#adsenseTrafficCountryDetailActiveViewTime').text(avt.toFixed(2));

                $('#adsenseTrafficCountryDetailModal').modal('show');
            });

        try { table.columns.adjust(); } catch (e) {}
    }

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

            $('#worldMapAdsense').css({ height: '480px', width: '100%', display: 'block', visibility: 'visible' }).empty();

            if (mapData.length === 0) {
                $('#worldMapAdsense').html('<div class="adsense-traffic-empty" style="padding:80px 24px;"><div class="adsense-traffic-empty-title">Tidak ada data pendapatan</div><div>Negara yang dipilih belum memiliki pendapatan pada periode ini.</div></div>');
                $('#charts_section').show();
                $('#btnToggleCountryMap').show();
                setCountryMapVisible(isCountryMapVisible(), false);
                return;
            }

            var theme = getAdsenseChartTheme();
            var nullAreaColor = isAdsenseDarkTheme() ? '#334155' : '#e2e8f0';
            var borderColor = isAdsenseDarkTheme() ? '#475569' : '#cbd5e1';
            var legendBg = isAdsenseDarkTheme() ? 'rgba(15, 23, 42, 0.88)' : 'rgba(255, 255, 255, 0.94)';

            var ranges = isAdsenseDarkTheme()
                ? [
                    { from: null, to: null, color: nullAreaColor, name: 'Tidak ada data' },
                    { from: 0, to: 50000, color: '#064e3b', name: 'Rp 0 – 50 rb' },
                    { from: 50000, to: 100000, color: '#047857', name: 'Rp 50 rb – 100 rb' },
                    { from: 100000, to: 500000, color: '#059669', name: 'Rp 100 rb – 500 rb' },
                    { from: 500000, to: 1000000, color: '#10b981', name: 'Rp 500 rb – 1 jt' },
                    { from: 1000000, to: 5000000, color: '#34d399', name: 'Rp 1 jt – 5 jt' },
                    { from: 5000000, to: 10000000, color: '#6ee7b7', name: 'Rp 5 jt – 10 jt' },
                    { from: 10000000, to: Infinity, color: '#a7f3d0', name: '> Rp 10 jt' }
                ]
                : [
                    { from: null, to: null, color: nullAreaColor, name: 'Tidak ada data' },
                    { from: 0, to: 50000, color: '#ecfdf5', name: 'Rp 0 – 50 rb' },
                    { from: 50000, to: 100000, color: '#a7f3d0', name: 'Rp 50 rb – 100 rb' },
                    { from: 100000, to: 500000, color: '#6ee7b7', name: 'Rp 100 rb – 500 rb' },
                    { from: 500000, to: 1000000, color: '#34d399', name: 'Rp 500 rb – 1 jt' },
                    { from: 1000000, to: 5000000, color: '#10b981', name: 'Rp 1 jt – 5 jt' },
                    { from: 5000000, to: 10000000, color: '#059669', name: 'Rp 5 jt – 10 jt' },
                    { from: 10000000, to: Infinity, color: '#047857', name: '> Rp 10 jt' }
                ];

            window.countryMapInstance = Highcharts.mapChart('worldMapAdsense', {
                chart: {
                    map: 'custom/world',
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
                            fill: isAdsenseDarkTheme() ? '#1e293b' : '#ffffff',
                            'stroke-width': 1,
                            stroke: borderColor,
                            r: 8,
                            states: { hover: { fill: '#059669', style: { color: '#fff' } } }
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
                                fill: isAdsenseDarkTheme() ? '#1e293b' : '#ffffff',
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
        } catch (error) {
            console.error('[ERROR] Failed to create map:', error);
            $('#worldMapAdsense').html('<div class="adsense-traffic-empty" style="padding:60px 24px;"><div class="adsense-traffic-empty-title">Gagal memuat peta</div><div>' + escapeHtml(error.message) + '</div></div>');
        }
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