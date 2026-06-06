function isAdsenseDarkTheme() {
  return document.documentElement.getAttribute('data-theme') === 'dark';
}

function getAdsenseChartTheme() {
  var dark = isAdsenseDarkTheme();
  return {
    text: dark ? '#e2e8f0' : '#334155',
    muted: dark ? '#94a3b8' : '#64748b',
    grid: dark ? 'rgba(148, 163, 184, 0.12)' : 'rgba(15, 23, 42, 0.08)',
    bg: dark ? '#1e293b' : '#ffffff',
    tooltipBg: dark ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.98)',
    tooltipBorder: dark ? 'rgba(255,255,255,0.1)' : 'rgba(15, 23, 42, 0.1)',
    primary: '#059669',
    primaryLight: 'rgba(5, 150, 105, 0.18)',
    palette: ['#059669', '#10b981', '#06b6d4', '#6366f1', '#8b5cf6', '#f59e0b', '#ef4444', '#ec4899', '#14b8a6', '#3b82f6']
  };
}

function showAdsenseSummaryLoader(message) {
  var msg = String(message || 'Memuat data AdSense Summary...').trim();
  if (window.HrisLoader && typeof window.HrisLoader.show === 'function') {
    window.HrisLoader.show(msg);
    return;
  }
  if (typeof $ !== 'undefined' && $('#overlay').length) {
    $('#overlay').attr('data-loader-message', msg).show();
  }
}

function hideAdsenseSummaryLoader() {
  if (window.HrisLoader && typeof window.HrisLoader.forceHide === 'function') {
    window.HrisLoader.forceHide();
    return;
  }
  if (typeof $ !== 'undefined' && $('#overlay').length) {
    $('#overlay').hide();
  }
}

function showAdsenseSummaryResults() {
  var empty = document.getElementById('adsenseSummaryEmptyState');
  var results = document.getElementById('adsenseSummaryResults');
  if (empty) empty.style.display = 'none';
  if (results) results.style.display = '';
}

function hideAdsenseSummaryResults() {
  var empty = document.getElementById('adsenseSummaryEmptyState');
  var results = document.getElementById('adsenseSummaryResults');
  if (results) results.style.display = 'none';
  if (empty) empty.style.display = '';
}

document.addEventListener('DOMContentLoaded', () => {
  const startInput = document.getElementById('start_date');
  const endInput = document.getElementById('end_date');
  const accountSelect = document.getElementById('account_filter');
  const domainInput = document.getElementById('domain_filter');
  const countrySelect = document.getElementById('country_filter');
  const btnLoad = document.getElementById('btn_load_summary');
  const infoBox = document.getElementById('summary_info');
  const infoSection = document.getElementById('summary_info_section');
  const chartCanvas = document.getElementById('chart_revenue_daily');
  const impressionsCanvas = document.getElementById('impressionsChart');
  const revenueCanvas = document.getElementById('revenueChart');
  const revenueDailyCard = document.getElementById('adsenseRevenueDaily');
  const chartsSection = document.getElementById('charts_section');
  let revenueChart = null;
  let trafficCharts = { impressions: null, revenue: null };

  const setVisible = (el, visible) => {
    if (!el) return;
    el.style.display = visible ? '' : 'none';
  };

  hideAdsenseSummaryResults();

  const IDR_RATE = 1;
  let currencyCode = 'IDR';

  const formatIDR = (n) => {
    const num = Number(n || 0);
    return 'Rp ' + Math.round(num).toLocaleString('id-ID');
  };
  const formatPercent = (n) => {
    const num = Number(n || 0);
    return `${num.toFixed(2)}%`;
  };
  const formatNumber = (n) => {
    const num = Number(n || 0);
    return new Intl.NumberFormat('id-ID').format(num);
  };

  const toISO = (d) => {
    const pad = (x) => String(x).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  };

  const setDefaultDates = () => {
    const today = new Date();
    startInput.value = toISO(today);
    endInput.value = toISO(today);
    if (startInput._flatpickr) startInput._flatpickr.setDate(startInput.value, true);
    if (endInput._flatpickr) endInput._flatpickr.setDate(endInput.value, true);
  };

  const loadAccounts = async () => {
    if (accountSelect && accountSelect.options && accountSelect.options.length > 0) return;
    try {
      const res = await fetch('/management/admin/ads_account_list');
      const json = await res.json();
      if (!json.status) return;
      (json.data || []).forEach((acc) => {
        const opt = document.createElement('option');
        opt.value = acc.account_id;
        opt.textContent = acc.account_name || acc.account_id;
        accountSelect.appendChild(opt);
      });
    } catch (err) {
      showInfoMessage(`Error load accounts: ${err.message}`, 'danger');
    }
  };

  const getSelectedAccountsCsv = () => {
    try {
      const v = (typeof $ !== 'undefined' && accountSelect) ? $(accountSelect).val() : null;
      if (Array.isArray(v) && v.length) return v.join(',');
    } catch (e) {}
    if (!accountSelect) return '';
    if (accountSelect.multiple) {
      const vals = Array.from(accountSelect.selectedOptions || []).map(o => o.value).filter(Boolean);
      return vals.join(',');
    }
    return accountSelect.value || '';
  };

  const initDomainSelect2 = () => {
    if (!domainInput) return;
    try {
      if (domainInput.hasAttribute('size')) domainInput.removeAttribute('size');
      domainInput.style.height = '';
    } catch (e) {}
    if (typeof $ === 'undefined' || !$.fn || !$.fn.select2) return;
    try {
      $(domainInput).select2({
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
            const selected_accounts = getSelectedAccountsCsv();
            return {
              q: (params && params.term) ? params.term : '',
              start_date: (startInput && startInput.value) ? startInput.value : '',
              end_date: (endInput && endInput.value) ? endInput.value : '',
              selected_account: selected_accounts
            };
          },
          processResults: function (data) {
            return { results: (data && data.results) ? data.results : [] };
          },
          cache: true
        },
        createTag: function (params) {
          const term = String((params && params.term) ? params.term : '').trim();
          if (!term) return null;
          return { id: term, text: term, newTag: true };
        }
      });
    } catch (e) {}
  };

  const getSelectedCountriesCsv = () => {
    try {
      const v = (typeof $ !== 'undefined' && countrySelect) ? $(countrySelect).val() : null;
      if (Array.isArray(v) && v.length) return v.join(',');
    } catch (e) {}
    if (!countrySelect) return '';
    const vals = Array.from(countrySelect.selectedOptions || []).map(o => o.value).filter(Boolean);
    return vals.join(',');
  };

  const getSelectedDomainsCsv = () => {
    if (!domainInput) return '';
    try {
      if (typeof $ !== 'undefined') {
        const v = $(domainInput).val();
        if (Array.isArray(v) && v.length) return v.join(',');
      }
    } catch (e) {}
    try {
      const tag = String(domainInput.tagName || '').toLowerCase();
      if (tag === 'select' && domainInput.multiple) {
        const vals = Array.from(domainInput.selectedOptions || []).map(o => o.value).filter(Boolean);
        return vals.join(',');
      }
    } catch (e) {}
    const raw = String(domainInput.value || '').trim();
    if (!raw) return '';
    return raw.split(',').map(s => String(s || '').trim()).filter(Boolean).join(',');
  };

  const loadCountryOptions = async () => {
    try {
      if (!countrySelect) return;
      const selected_accounts = getSelectedAccountsCsv();
      const previouslySelected = (typeof $ !== 'undefined') ? ($(countrySelect).val() || []) : Array.from(countrySelect.selectedOptions || []).map(o => o.value);
      const params = new URLSearchParams();
      if (selected_accounts) params.set('selected_accounts', selected_accounts);
      const url = `/management/admin/get_countries_adsense?${params.toString()}`;
      const res = await fetch(url);
      const json = await res.json();
      const countries = json.countries || [];

      countrySelect.innerHTML = '';
      const validPrevious = new Set();
      countries.forEach((c) => {
        const code = (c.code || '').trim();
        const name = c.name || code;
        const isSel = previouslySelected.includes(code);
        if (isSel) validPrevious.add(code);
        const opt = document.createElement('option');
        opt.value = code;
        opt.textContent = name;
        if (isSel) opt.selected = true;
        countrySelect.appendChild(opt);
      });

      if (typeof $ !== 'undefined') {
        $(countrySelect).val(Array.from(validPrevious)).trigger('change.select2');
      }
    } catch (err) {
      showInfoMessage(`Error load countries: ${err.message}`, 'danger');
    }
  };

  const showInfoMessage = (html, type) => {
    if (!infoBox || !infoSection) return;
    infoBox.innerHTML = html;
    if (type === 'danger') {
      infoBox.style.borderColor = 'rgba(239, 68, 68, 0.35)';
      infoBox.style.background = 'linear-gradient(135deg, rgba(239, 68, 68, 0.08), rgba(248, 113, 113, 0.05))';
    } else {
      infoBox.style.borderColor = '';
      infoBox.style.background = '';
    }
    setVisible(infoSection, true);
  };

  const hideInfoMessage = () => {
    setVisible(infoSection, false);
    if (infoBox) infoBox.innerHTML = '';
  };

  const updateSummaryBoxes = (sum) => {
    const totalImpressions = sum.total_impressions ?? 0;
    const totalClicks = sum.total_clicks ?? 0;
    const totalRevenue = sum.total_revenue ?? 0;
    const ctr = (sum.ctr ?? sum.avg_ctr ?? 0);
    const cpc = (sum.cpc ?? sum.avg_cpc ?? 0);
    const ecpm = (sum.ecpm ?? sum.avg_ecpm ?? 0);

    document.getElementById('sum_impressions').textContent = formatNumber(totalImpressions);
    document.getElementById('sum_clicks').textContent = formatNumber(totalClicks);
    document.getElementById('sum_ctr').textContent = formatPercent(ctr);
    const rate = currencyCode === 'USD' ? IDR_RATE : 1;
    document.getElementById('sum_cpc').textContent = formatIDR(Number(cpc || 0) * rate);
    document.getElementById('sum_ecpm').textContent = formatIDR(Number(ecpm || 0) * rate);
    document.getElementById('sum_revenue').textContent = formatIDR(Number(totalRevenue || 0) * rate);

    const hasData = (sum.total_impressions || 0) > 0 ||
      (sum.total_clicks || 0) > 0 ||
      (sum.total_revenue || 0) > 0;

    if (!hasData) {
      showInfoMessage(
        '<strong><i class="bi bi-info-circle mr-1"></i> Tidak ada data AdSense</strong><br>' +
        'Kemungkinan penyebab: akun baru/belum aktif, tidak ada traffic pada periode ini, atau ads belum terpasang. ' +
        'Coba rentang tanggal lain atau periksa konfigurasi AdSense.',
        'info'
      );
    } else {
      hideInfoMessage();
    }
  };

  const renderRevenueChart = (rows) => {
    if (!Array.isArray(rows) || rows.length === 0) {
      if (revenueChart) {
        revenueChart.destroy();
        revenueChart = null;
      }
      setVisible(revenueDailyCard, false);
      return;
    }

    const theme = getAdsenseChartTheme();
    const dailyRevenue = new Map();
    rows.forEach((r) => {
      const d = String(r.date || '');
      if (!d) return;
      const prev = dailyRevenue.get(d) || 0;
      dailyRevenue.set(d, prev + Number(r.revenue || 0));
    });

    const labels = Array.from(dailyRevenue.keys()).sort();
    const rate = currencyCode === 'USD' ? IDR_RATE : 1;
    const revenues = labels.map(d => Number(dailyRevenue.get(d) || 0) * rate);

    const formattedLabels = labels.map((d) => {
      try {
        const dt = new Date(d + 'T00:00:00');
        return dt.toLocaleDateString('id-ID', { day: 'numeric', month: 'short' });
      } catch (e) {
        return d;
      }
    });

    const gradientFill = (context) => {
      const chart = context.chart;
      const { ctx, chartArea } = chart;
      if (!chartArea) return theme.primaryLight;
      const g = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
      g.addColorStop(0, 'rgba(5, 150, 105, 0.35)');
      g.addColorStop(1, 'rgba(5, 150, 105, 0.02)');
      return g;
    };

    const data = {
      labels: formattedLabels,
      datasets: [{
        label: 'Pendapatan Harian (Rp)',
        data: revenues,
        borderColor: theme.primary,
        backgroundColor: gradientFill,
        tension: 0.35,
        fill: true,
        pointRadius: 4,
        pointHoverRadius: 6,
        pointBackgroundColor: theme.primary,
        pointBorderColor: '#fff',
        pointBorderWidth: 2,
        borderWidth: 3
      }]
    };

    const options = {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: {
          ticks: { color: theme.muted, maxRotation: 0, autoSkip: true, font: { size: 11 } },
          grid: { color: theme.grid, drawBorder: false }
        },
        y: {
          beginAtZero: true,
          ticks: {
            color: theme.muted,
            font: { size: 11 },
            callback: (v) => 'Rp ' + formatNumber(v)
          },
          grid: { color: theme.grid, drawBorder: false }
        }
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: theme.tooltipBg,
          titleColor: theme.text,
          bodyColor: theme.muted,
          borderColor: theme.tooltipBorder,
          borderWidth: 1,
          padding: 12,
          cornerRadius: 10,
          callbacks: {
            label: (ctx) => ' Pendapatan: Rp ' + formatNumber(ctx.raw || 0)
          }
        }
      }
    };

    if (revenueChart) {
      revenueChart.data = data;
      revenueChart.options = options;
      revenueChart.update();
    } else if (chartCanvas && typeof Chart !== 'undefined') {
      revenueChart = new Chart(chartCanvas.getContext('2d'), {
        type: 'line',
        data,
        options
      });
    }
    setVisible(revenueDailyCard, true);
  };

  const generateTrafficCountryCharts = (data) => {
    if (!Array.isArray(data) || data.length === 0) {
      setVisible(chartsSection, false);
      if (trafficCharts.impressions) { trafficCharts.impressions.destroy(); trafficCharts.impressions = null; }
      if (trafficCharts.revenue) { trafficCharts.revenue.destroy(); trafficCharts.revenue = null; }
      return;
    }

    const theme = getAdsenseChartTheme();
    const sorted = data.slice().sort((a, b) => (Number(b.impressions || 0) - Number(a.impressions || 0))).slice(0, 10);
    const labels = sorted.map(item => {
      const name = item.country_name || item.country_code || 'Unknown';
      return name.length > 18 ? name.slice(0, 16) + '…' : name;
    });
    const impressions = sorted.map(item => Number(item.impressions || 0));
    const revenue = sorted.map(item => Number(item.revenue || 0));

    if (typeof Chart === 'undefined') return;

    const chartDefaults = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: theme.text, font: { size: 11, weight: '600' } } },
        tooltip: {
          backgroundColor: theme.tooltipBg,
          titleColor: theme.text,
          bodyColor: theme.muted,
          borderColor: theme.tooltipBorder,
          borderWidth: 1,
          padding: 12,
          cornerRadius: 8
        }
      },
      scales: {
        x: {
          ticks: { color: theme.muted, maxRotation: 45, minRotation: 0, font: { size: 10 } },
          grid: { color: theme.grid, drawBorder: false }
        },
        y: {
          ticks: { color: theme.muted, font: { size: 10 } },
          grid: { color: theme.grid, drawBorder: false }
        }
      }
    };

    if (impressionsCanvas) {
      if (trafficCharts.impressions) trafficCharts.impressions.destroy();
      trafficCharts.impressions = new Chart(impressionsCanvas.getContext('2d'), {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            label: 'Impresi',
            data: impressions,
            backgroundColor: 'rgba(5, 150, 105, 0.78)',
            borderColor: theme.primary,
            borderWidth: 0,
            borderRadius: 8,
            borderSkipped: false,
            maxBarThickness: 42
          }]
        },
        options: Object.assign({}, chartDefaults, {
          plugins: Object.assign({}, chartDefaults.plugins, { legend: { display: false } })
        })
      });
    }

    if (revenueCanvas) {
      if (trafficCharts.revenue) trafficCharts.revenue.destroy();
      const doughnutColors = theme.palette.slice(0, labels.length);
      trafficCharts.revenue = new Chart(revenueCanvas.getContext('2d'), {
        type: 'doughnut',
        data: {
          labels,
          datasets: [{
            label: 'Pendapatan',
            data: revenue,
            backgroundColor: doughnutColors.map(c => c + 'cc'),
            borderColor: theme.bg,
            borderWidth: 3,
            hoverOffset: 8
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          cutout: '62%',
          plugins: {
            legend: {
              position: 'right',
              labels: { color: theme.text, font: { size: 10 }, boxWidth: 12, padding: 10 }
            },
            tooltip: {
              backgroundColor: theme.tooltipBg,
              titleColor: theme.text,
              bodyColor: theme.muted,
              borderColor: theme.tooltipBorder,
              borderWidth: 1,
              callbacks: {
                label: (ctx) => ' Rp ' + formatNumber(ctx.raw || 0)
              }
            }
          }
        }
      });
    }

    setVisible(chartsSection, true);
  };

  const loadSummary = async () => {
    const start_date = startInput.value;
    const end_date = endInput.value;
    const selected_account = getSelectedAccountsCsv();
    const selected_domains = getSelectedDomainsCsv();
    const selected_countries = getSelectedCountriesCsv();
    hideInfoMessage();

    showAdsenseSummaryLoader();

    try {
      const p1 = new URLSearchParams({ start_date, end_date });
      if (selected_account) p1.set('selected_account', selected_account);
      if (selected_domains) p1.set('selected_domains', selected_domains);
      if (selected_countries) p1.set('selected_countries', selected_countries);

      const p2 = new URLSearchParams({ start_date, end_date });
      if (selected_account) p2.set('selected_account', selected_account);
      if (selected_domains) p2.set('selected_domains', selected_domains);
      if (selected_countries) p2.set('selected_countries', selected_countries);

      const [accountRes, countryRes] = await Promise.all([
        fetch(`/management/admin/adsense_traffic_account_data?${p1.toString()}`),
        fetch(`/management/admin/adsense_traffic_country_data?${p2.toString()}`)
      ]);

      const accountJson = await accountRes.json();
      const countryJson = await countryRes.json();

      if (!accountJson.status) throw new Error(accountJson.error || 'Failed to load traffic account');
      if (!countryJson.status) throw new Error(countryJson.error || 'Failed to load traffic country');

      currencyCode = 'IDR';
      showAdsenseSummaryResults();
      updateSummaryBoxes(accountJson.summary || {});
      renderRevenueChart(accountJson.data || []);
      generateTrafficCountryCharts(countryJson.data || []);
      hideAdsenseSummaryLoader();
    } catch (err) {
      hideAdsenseSummaryLoader();
      hideAdsenseSummaryResults();
      showAdsenseSummaryResults();
      showInfoMessage(`<strong>Error:</strong> ${err.message}`, 'danger');
    }
  };

  btnLoad.addEventListener('click', loadSummary);

  if (typeof flatpickr !== 'undefined') {
    flatpickr(startInput, { dateFormat: 'Y-m-d' });
    flatpickr(endInput, { dateFormat: 'Y-m-d' });
  }

  setDefaultDates();

  if (typeof $ !== 'undefined') {
    $('#account_filter').select2({
      theme: 'bootstrap4',
      placeholder: 'Pilih account',
      allowClear: true,
      width: '100%'
    });

    $('#country_filter').select2({
      theme: 'bootstrap4',
      placeholder: 'Pilih country (opsional)',
      allowClear: true,
      width: '100%',
      tags: true,
      tokenSeparators: [','],
      closeOnSelect: false
    });

    $('#account_filter').on('change', function () {
      loadCountryOptions();
    });
  }

  initDomainSelect2();
  loadAccounts();
  loadCountryOptions();
});
