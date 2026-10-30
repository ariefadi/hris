(function () {
    'use strict';

    var cfg = window.HRIS_DOMAIN_CHECKER || {};
    var table = null;
    var rows = [];
    var checking = false;
    var currentFilter = 'all';
    var statusChart = null;
    var urgencyChart = null;

    function escapeHtml(text) {
        return String(text == null ? '' : text)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function toneLabel(tone) {
        if (tone === 'healthy') return 'Aman';
        if (tone === 'watch') return 'Perlu pantau';
        if (tone === 'soon') return 'Harus renew';
        if (tone === 'expired') return 'Expired';
        if (tone === 'unregistered') return 'Tidak terdaftar';
        if (tone === 'unknown') return 'Tidak diketahui';
        return 'Belum dicek';
    }

    function selectedDomains() {
        var out = [];
        document.querySelectorAll('.dc-row-check:checked').forEach(function (el) {
            var domain = el.getAttribute('data-domain');
            if (domain) out.push(domain);
        });
        return out;
    }

    function setFilter(filter) {
        currentFilter = filter || 'all';
        document.querySelectorAll('.dc-box[data-filter], .dc-chip[data-filter]').forEach(function (el) {
            var match = el.getAttribute('data-filter') === currentFilter;
            el.classList.toggle('is-active', el.classList.contains('dc-box') && match);
            el.classList.toggle('is-on', el.classList.contains('dc-chip') && match);
        });
        if (!table) return;
        $.fn.dataTable.ext.search = $.fn.dataTable.ext.search.filter(function (fn) {
            return fn._dcFilter !== true;
        });
        var fn = function (settings, _data, dataIndex) {
            if (!table || settings.nTable !== table.table().node()) return true;
            var row = table.row(dataIndex).data() || {};
            var tone = String(row.tone || '');
            if (currentFilter === 'all') return true;
            if (currentFilter === 'soon') return tone === 'soon' || tone === 'expired';
            return tone === currentFilter;
        };
        fn._dcFilter = true;
        $.fn.dataTable.ext.search.push(fn);
        table.draw();
    }

    function setSummary(summary) {
        summary = summary || {};
        document.getElementById('dcTotal').textContent = summary.total || 0;
        document.getElementById('dcHealthy').textContent = summary.healthy || 0;
        document.getElementById('dcWatch').textContent = summary.watch || 0;
        document.getElementById('dcSoon').textContent = summary.soon || 0;
        document.getElementById('dcExpired').textContent = summary.expired || 0;
        var unreg = document.getElementById('dcUnregistered');
        if (unreg) unreg.textContent = summary.unregistered || 0;
        renderCharts(summary);
    }

    function isDark() {
        return document.documentElement.getAttribute('data-theme') === 'dark';
    }

    function chartTextColor() {
        return isDark() ? '#e2e8f0' : '#0f172a';
    }

    function destroyChart(chart) {
        try { if (chart && chart.destroy) chart.destroy(); } catch (err) {}
        return null;
    }

    function renderCharts(summary) {
        if (!window.Highcharts || !Highcharts.chart) return;
        summary = summary || {};
        var pieData = [
            { name: 'Aman', y: Number(summary.healthy || 0), color: '#10b981' },
            { name: 'Perlu pantau', y: Number(summary.watch || 0), color: '#f59e0b' },
            { name: 'Harus renew', y: Number(summary.soon || 0), color: '#f43f5e' },
            { name: 'Expired', y: Number(summary.expired || 0), color: '#dc2626' },
            { name: 'Tidak terdaftar', y: Number(summary.unregistered || 0), color: '#64748b' },
            { name: 'Belum dicek', y: Number(summary.pending || 0) + Number(summary.unknown || 0), color: '#94a3b8' }
        ].filter(function (item) { return item.y > 0; });

        statusChart = destroyChart(statusChart);
        statusChart = Highcharts.chart('dcStatusChart', {
            chart: { type: 'pie', backgroundColor: 'transparent', height: 300 },
            title: { text: 'Komposisi status domain', style: { color: chartTextColor(), fontWeight: 800 } },
            subtitle: { text: 'Klik potongan untuk memfilter tabel', style: { color: isDark() ? '#94a3b8' : '#64748b' } },
            tooltip: { pointFormat: '<b>{point.y}</b> domain ({point.percentage:.1f}%)' },
            credits: { enabled: false },
            plotOptions: {
                pie: {
                    innerSize: '58%',
                    allowPointSelect: true,
                    cursor: 'pointer',
                    dataLabels: { enabled: true, format: '{point.name}: {point.y}', style: { color: chartTextColor(), textOutline: 'none' } },
                    point: {
                        events: {
                            click: function () {
                                var map = { 'Aman': 'healthy', 'Perlu pantau': 'watch', 'Harus renew': 'soon', 'Expired': 'expired', 'Tidak terdaftar': 'unregistered', 'Belum dicek': 'pending' };
                                setFilter(map[this.name] || 'all');
                            }
                        }
                    }
                }
            },
            series: [{ name: 'Domain', colorByPoint: true, data: pieData.length ? pieData : [{ name: 'Belum ada data', y: 1, color: '#cbd5e1' }] }]
        });

        var urgent = rows.filter(function (row) {
            return row.days_left != null && row.days_left !== '';
        }).sort(function (a, b) {
            return Number(a.days_left) - Number(b.days_left);
        }).slice(0, 12);
        var colors = { healthy: '#10b981', watch: '#f59e0b', soon: '#f43f5e', expired: '#dc2626', pending: '#94a3b8', unknown: '#94a3b8', unregistered: '#64748b' };

        urgencyChart = destroyChart(urgencyChart);
        urgencyChart = Highcharts.chart('dcUrgencyChart', {
            chart: { type: 'bar', backgroundColor: 'transparent', height: 300 },
            title: { text: 'Domain paling mendesak', style: { color: chartTextColor(), fontWeight: 800 } },
            subtitle: { text: urgent.length ? 'Diurutkan dari sisa hari paling sedikit' : 'Cek domain dulu untuk menampilkan grafik', style: { color: isDark() ? '#94a3b8' : '#64748b' } },
            xAxis: { categories: urgent.map(function (row) { return row.domain; }), labels: { style: { color: chartTextColor() } } },
            yAxis: { title: { text: 'Sisa hari', style: { color: chartTextColor() } }, labels: { style: { color: chartTextColor() } }, gridLineColor: isDark() ? 'rgba(148,163,184,.18)' : 'rgba(15,23,42,.08)' },
            legend: { enabled: false },
            credits: { enabled: false },
            tooltip: { pointFormat: '<b>{point.y}</b> hari' },
            series: [{
                name: 'Sisa hari',
                data: urgent.map(function (row) {
                    return { y: Number(row.days_left), color: colors[row.tone] || '#6366f1' };
                })
            }]
        });
    }

    function exportMeta() {
        return {
            titleText: 'Domain Expired',
            filenameBase: 'domain_expired',
            extraLines: [
                'Filter: ' + (currentFilter === 'all' ? 'Semua' : toneLabel(currentFilter)),
                'Diekspor ' + new Date().toLocaleString('id-ID')
            ]
        };
    }

    function collectExport() {
        var list = table ? table.rows({ search: 'applied' }).data().toArray() : rows;
        return {
            headers: ['Domain', 'Akun Google', 'Sumber', 'Status', 'Expired', 'Sisa Hari', 'Registrar', 'Cek via', 'Last Checked', 'Catatan'],
            rows: (list || []).map(function (row) {
                return [
                    row.domain || '',
                    row.account_label || '',
                    row.channel_label || '',
                    toneLabel(row.tone),
                    row.expired_date || '',
                    row.days_left == null ? '' : String(row.days_left),
                    row.registrar || '',
                    row.source || '',
                    row.last_checked || '',
                    row.notes || ''
                ];
            })
        };
    }

    function runPageExport(kind) {
        var api = window.HrisTableExport;
        if (!api) {
            window.alert('Modul export belum siap. Muat ulang halaman.');
            return;
        }
        var collected = collectExport();
        var meta = exportMeta();
        if (!collected.rows.length) {
            window.alert('Tidak ada data untuk diekspor.');
            return;
        }
        if (kind === 'excel') api.exportExcel(meta, collected.headers, collected.rows, { sheetName: 'Domain Expired' });
        else if (kind === 'pdf') api.exportPdf(meta, collected.headers, collected.rows, { sheetName: 'Domain Expired', pageOrientation: 'landscape' });
        else if (kind === 'csv') api.exportCsv(meta, collected.headers, collected.rows);
        else if (kind === 'gsheet') api.exportGoogleSpreadsheet(meta, collected.headers, collected.rows);
        else if (kind === 'copy') {
            var lines = [collected.headers.join('\t')].concat(collected.rows.map(function (row) { return row.join('\t'); }));
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(lines.join('\n'));
            }
        }
    }

    function mountExportButtons() {
        var wrap = document.getElementById('dcExportButtons');
        if (!wrap || wrap.dataset.ready === '1') return;
        wrap.dataset.ready = '1';
        var buttons = [
            { kind: 'copy', label: 'Copy', cls: 'btn btn-sm btn-info' },
            { kind: 'excel', label: 'Export Excel', cls: 'btn btn-sm btn-success' },
            { kind: 'pdf', label: 'Export PDF', cls: 'btn btn-sm btn-danger' },
            { kind: 'csv', label: 'Export CSV', cls: 'btn btn-sm btn-primary' }
        ];
        buttons.forEach(function (item) {
            var btn = document.createElement('button');
            btn.type = 'button';
            btn.className = item.cls;
            btn.textContent = item.label;
            btn.addEventListener('click', function () { runPageExport(item.kind); });
            wrap.appendChild(btn);
        });
    }

    function daysCell(row) {
        if (row.days_left == null || row.days_left === '') {
            return '<span class="dc-pill is-pending">-</span>';
        }
        var days = Number(row.days_left);
        var text = days < 0 ? ('Lewat ' + Math.abs(days) + ' hari') : (days + ' hari');
        return '<span class="dc-pill is-' + escapeHtml(row.tone || 'pending') + '">' + escapeHtml(text) + '</span>';
    }

    function renderTable(list) {
        rows = list || [];
        if (table) {
            table.clear().destroy();
            table = null;
        }
        table = $('#dcTable').DataTable({
            data: rows,
            pageLength: 25,
            order: [[4, 'asc']],
            language: {
                search: '',
                searchPlaceholder: 'Cari...',
                lengthMenu: 'Tampil _MENU_',
                info: '_START_–_END_ dari _TOTAL_ domain',
                infoEmpty: 'Tidak ada domain',
                zeroRecords: 'Tidak ada domain pada filter ini',
                paginate: { previous: 'Sebelumnya', next: 'Berikutnya' }
            },
            dom: 'lrtip',
            columns: [
                {
                    data: 'domain',
                    orderable: false,
                    className: 'text-center no-export',
                    render: function (domain) {
                        return '<input type="checkbox" class="dc-row-check" data-domain="' + escapeHtml(domain) + '">';
                    }
                },
                {
                    data: 'domain',
                    render: function (domain, _t, row) {
                        var src = (row.channels || []).map(function (ch) {
                            return '<span class="dc-src">' + escapeHtml(ch === 'adsense' ? 'AdSense' : 'ADX') + '</span>';
                        }).join(' ');
                        return '<div class="dc-domain">' + escapeHtml(domain) + '</div>'
                            + '<span class="dc-sub">' + escapeHtml(row.account_label || '-') + (src ? ' · ' + src : '') + '</span>';
                    }
                },
                {
                    data: 'tone',
                    render: function (tone) {
                        return '<span class="dc-pill is-' + escapeHtml(tone || 'pending') + '">' + escapeHtml(toneLabel(tone)) + '</span>';
                    }
                },
                {
                    data: 'expired_date',
                    render: function (v) { return v ? escapeHtml(v) : '<span class="dc-sub">Belum ada tanggal</span>'; }
                },
                {
                    data: 'days_left',
                    className: 'text-center',
                    render: function (v, type, row) {
                        if (type === 'sort' || type === 'type') {
                            if (v == null || v === '') return 99999;
                            return Number(v);
                        }
                        return daysCell(row);
                    }
                },
                {
                    data: 'registrar',
                    render: function (v, _t, row) {
                        var note = row.notes ? '<span class="dc-sub">' + escapeHtml(row.notes) + '</span>' : '';
                        return '<div>' + escapeHtml(v || '-') + '</div>' + note;
                    }
                },
                {
                    data: 'last_checked',
                    render: function (v, _t, row) {
                        var via = row.source ? '<span class="dc-src">' + escapeHtml(row.source) + '</span>' : '';
                        return '<div>' + escapeHtml(v || 'Belum dicek') + '</div>' + (via ? '<span class="dc-sub">' + via + '</span>' : '');
                    }
                },
                {
                    data: 'domain',
                    orderable: false,
                    className: 'no-export',
                    render: function (domain) {
                        return '<button type="button" class="btn btn-xs btn-outline-primary dc-check-btn" data-detail="' + escapeHtml(domain) + '"><i class="bi bi-eye mr-1"></i>Detail</button>';
                    }
                }
            ],
            createdRow: function (row, data) {
                $(row).addClass('is-' + (data.tone || 'pending'));
            }
        });
        setFilter(currentFilter);
    }

    function loadData() {
        return $.getJSON(cfg.dataUrl).done(function (res) {
            if (!res || !res.status) return;
            rows = res.rows || [];
            setSummary(res.summary || {});
            renderTable(rows);
        });
    }

    function setProgress(done, total, label) {
        var wrap = document.getElementById('dcProgressWrap');
        var bar = document.getElementById('dcProgressBar');
        var text = document.getElementById('dcProgressLabel');
        if (!wrap || !bar) return;
        wrap.style.display = total ? 'block' : 'none';
        var pct = total ? Math.round((done / total) * 100) : 0;
        bar.style.width = pct + '%';
        if (text) text.textContent = label || ('Mengecek domain ' + done + ' dari ' + total);
    }

    function checkDomains(list) {
        if (checking) return;
        list = (list || []).filter(Boolean);
        if (!list.length) {
            if (window.hrisSwalFire) window.hrisSwalFire({ icon: 'info', title: 'Pilih domain', text: 'Centang domain yang ingin dicek, atau gunakan Cek Semua Domain.' });
            else window.alert('Pilih domain yang ingin dicek.');
            return;
        }
        checking = true;
        ['dcCheckAll', 'dcCheckSelected'].forEach(function (id) {
            var btn = document.getElementById(id);
            if (btn) btn.disabled = true;
        });
        var queue = list.slice();
        var total = queue.length;
        var done = 0;
        setProgress(0, total, 'Mulai cek ' + total + ' domain...');

        function finish() {
            checking = false;
            ['dcCheckAll', 'dcCheckSelected'].forEach(function (id) {
                var btn = document.getElementById(id);
                if (btn) btn.disabled = false;
            });
            setProgress(total, total, 'Selesai. Memuat hasil terbaru...');
            loadData().always(function () {
                window.setTimeout(function () { setProgress(0, 0); }, 800);
            });
        }

        function next() {
            if (!queue.length) {
                finish();
                return;
            }
            var batch = queue.splice(0, 6);
            $.ajax({
                url: cfg.checkUrl,
                method: 'POST',
                contentType: 'application/json',
                data: JSON.stringify({ domains: batch })
            }).always(function () {
                done += batch.length;
                setProgress(Math.min(done, total), total);
                window.setTimeout(next, 250);
            });
        }
        next();
    }

    var detailDomain = '';

    function dash(value) {
        return value ? escapeHtml(String(value)) : '<span class="dc-sub">Tidak tersedia</span>';
    }

    function kv(label, value) {
        return '<div class="dc-kv"><div class="k">' + escapeHtml(label) + '</div><div class="v">' + dash(value) + '</div></div>';
    }

    function kpi(label, value, sub) {
        return '<div class="dc-kpi"><div class="k">' + escapeHtml(label) + '</div><div class="v">' + dash(value) + '</div>'
            + (sub ? '<div class="s">' + escapeHtml(sub) + '</div>' : '') + '</div>';
    }

    function daysText(days) {
        if (days == null || days === '') return '-';
        days = Number(days);
        if (isNaN(days)) return '-';
        return days < 0 ? ('Lewat ' + Math.abs(days) + ' hari') : (days + ' hari');
    }

    function renderDetail(payload) {
        var d = (payload && payload.detail) || {};
        var g = (payload && payload.google) || {};
        var local = (payload && payload.local) || {};
        var title = document.getElementById('dcDetailTitle');
        var pills = document.getElementById('dcDetailPills');
        var body = document.getElementById('dcDetailBody');
        if (title) title.textContent = d.domain || detailDomain || '-';
        var tone = 'pending';
        if (d.not_found) tone = 'unregistered';
        else if (d.days_left != null && d.days_left !== '') {
            var n = Number(d.days_left);
            if (n < 0) tone = 'expired';
            else if (n <= 30) tone = 'soon';
            else if (n <= 90) tone = 'watch';
            else tone = 'healthy';
        }
        if (pills) {
            pills.innerHTML = '<span class="dc-pill is-' + tone + '">' + escapeHtml(toneLabel(tone)) + '</span>'
                + (d.source ? ' <span class="dc-src">' + escapeHtml(d.source) + '</span>' : '')
                + (d.domain_type ? ' <span class="dc-src">' + escapeHtml(d.domain_type) + '</span>' : '');
        }
        var ns = [].concat(d.nameservers || []);
        if (local.nameservers && local.nameservers.length && !ns.length) ns = [].concat(local.nameservers);
        var nsHtml = ns.length
            ? '<div class="dc-ns">' + ns.map(function (item) { return '<span>' + escapeHtml(item) + '</span>'; }).join('') + '</div>'
            : '<span class="dc-sub">Tidak tersedia</span>';
        var accounts = (g.accounts || []).map(function (acc) {
            return escapeHtml((acc.account_name || acc.user_mail || acc.account_id || '-') + (acc.channel ? ' · ' + String(acc.channel).toUpperCase() : ''));
        }).join('<br>');
        var events = (d.events || []).map(function (ev) {
            return '<div class="dc-event"><span>' + escapeHtml(ev.action || '-') + '</span><strong>' + escapeHtml(ev.date || '-') + '</strong></div>';
        }).join('');
        var price = d.price || 'Tidak dipublikasikan';
        var registered = d.registered_date || local.registration_date || '';
        var expired = d.expired_date || local.expiration_date || '';
        var html = ''
            + '<div class="dc-detail-kpis">'
            + kpi('Tanggal pembelian', registered, d.age_years != null ? ('Usia ' + d.age_years + ' tahun') : 'Dari event registrasi RDAP')
            + kpi('Tanggal expired', expired, daysText(d.days_left))
            + kpi('Periode sewa', d.tenure_years ? (d.tenure_years + ' tahun') : '-', 'Registrasi sampai expired')
            + kpi('Terakhir diubah', d.last_changed || '-', d.last_rdap_update ? ('RDAP ' + d.last_rdap_update) : '')
            + '</div>'
            + '<div class="dc-price"><div class="k">Harga beli / perpanjang</div><div class="v">' + escapeHtml(price) + '</div>'
            + '<div class="s">' + escapeHtml(d.price_note || '') + '</div></div>'
            + '<div class="dc-kv-grid">'
            + kv('Registrar', d.registrar || local.registrar)
            + kv('IANA ID', d.registrar_iana)
            + kv('Situs registrar', d.registrar_url)
            + kv('WHOIS server', d.whois_server)
            + kv('Email abuse', d.abuse_email)
            + kv('Telepon abuse', d.abuse_phone)
            + kv('Status registry', d.status)
            + kv('DNSSEC', d.dnssec)
            + kv('Registry ID', d.handle)
            + kv('Registrant', d.registrant_org || d.registrant_name)
            + kv('IP lokal', local.primary_ip)
            + kv('Provider DNS', local.provider)
            + '</div>'
            + '<div class="dc-kv" style="margin-top:14px;"><div class="k">Nameserver</div>' + nsHtml + '</div>'
            + '<div class="dc-kv" style="margin-top:12px;"><div class="k">Akun Google</div><div class="v">' + (accounts || dash(g.account_label || g.google_mail)) + '</div></div>'
            + (local.notes ? '<div class="dc-kv" style="margin-top:12px;"><div class="k">Catatan inventory</div><div class="v">' + escapeHtml(local.notes) + '</div></div>' : '')
            + (d.note ? '<div class="dc-kv" style="margin-top:12px;"><div class="k">Catatan cek</div><div class="v">' + escapeHtml(d.note) + '</div></div>' : '')
            + (events ? '<div style="margin-top:14px;"><div class="dc-kv"><div class="k">Riwayat event RDAP</div></div>' + events + '</div>' : '')
            + (d.whois_raw ? '<details style="margin-top:14px;"><summary style="cursor:pointer;font-weight:800;">WHOIS mentah</summary><pre class="dc-whois">' + escapeHtml(d.whois_raw) + '</pre></details>' : '');
        if (body) body.innerHTML = html;
    }

    function openDetail(domain, refreshTable) {
        domain = String(domain || '').trim();
        if (!domain || !cfg.detailUrl) return;
        detailDomain = domain;
        var title = document.getElementById('dcDetailTitle');
        var pills = document.getElementById('dcDetailPills');
        var body = document.getElementById('dcDetailBody');
        var refreshBtn = document.getElementById('dcDetailRefresh');
        if (title) title.textContent = domain;
        if (pills) pills.innerHTML = '';
        if (body) body.innerHTML = '<div class="dc-loading">Mengambil rincian RDAP/WHOIS untuk <b>' + escapeHtml(domain) + '</b>...</div>';
        if (refreshBtn) refreshBtn.disabled = true;
        $('#dcDetailModal').modal('show');
        $.getJSON(cfg.detailUrl, { domain: domain }).done(function (res) {
            if (!res || !res.status) {
                if (body) body.innerHTML = '<div class="dc-loading">' + escapeHtml((res && res.error) || 'Gagal memuat rincian domain.') + '</div>';
                return;
            }
            renderDetail(res);
            if (refreshTable) loadData();
        }).fail(function (xhr) {
            var msg = (xhr.responseJSON && xhr.responseJSON.error) || 'Gagal memuat rincian domain.';
            if (body) body.innerHTML = '<div class="dc-loading">' + escapeHtml(msg) + '</div>';
        }).always(function () {
            if (refreshBtn) refreshBtn.disabled = false;
        });
    }

    $(function () {
        mountExportButtons();
        loadData();
        $('#dcReload').on('click', loadData);
        $('#dcSearch').on('input', function () {
            if (table) table.search(this.value || '').draw();
        });
        $('#dcCheckAll').on('click', function () {
            checkDomains(rows.map(function (r) { return r.domain; }));
        });
        $('#dcCheckSelected').on('click', function () {
            checkDomains(selectedDomains());
        });
        $('#dcCheckAllBox').on('change', function () {
            var on = this.checked;
            document.querySelectorAll('.dc-row-check').forEach(function (el) { el.checked = on; });
        });
        $(document).on('click', '[data-filter]', function () {
            setFilter(this.getAttribute('data-filter'));
        });
        $(document).on('click', '[data-detail]', function () {
            openDetail(this.getAttribute('data-detail'), true);
        });
        $('#dcDetailRefresh').on('click', function () {
            if (detailDomain) openDetail(detailDomain, true);
        });
    });
})();
