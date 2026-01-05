document.addEventListener('DOMContentLoaded', () => {
  const startInput = document.getElementById('start_date');
  const endInput = document.getElementById('end_date');
  const accountSelect = document.getElementById('account_filter');
  const countrySelect = document.getElementById('country_filter');
  const btnLoad = document.getElementById('btn_load_summary');
  const infoBox = document.getElementById('summary_info');
  const chartCanvas = document.getElementById('chart_revenue_daily');
  const impressionsCanvas = document.getElementById('impressionsChart');
  const revenueCanvas = document.getElementById('revenueChart');
  let revenueChart = null;
  let trafficCharts = { impressions: null, revenue: null };

  const IDR_RATE = 1;
  let currencyCode = 'IDR';

  const formatIDR = (n) => {
    const num = Number(n || 0);
    return new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', maximumFractionDigits: 0 }).format(num);
  };
  const formatPercent = (n) => {
    const num = Number(n || 0);
    return `${num.toFixed(2)}%`;
  };
  const formatNumber = (n) => {
    const num = Number(n || 0);
    return new Intl.NumberFormat('en-US').format(num);
  };

  const toISO = (d) => {
    const pad = (x) => String(x).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
  };

  const setDefaultDates = () => {
    // Default 7 hari terakhir: dari hari ini - 6 sampai hari ini
    const today = new Date();
    const start = new Date(today);
    start.setDate(today.getDate() - 6);
    startInput.value = toISO(start);
    endInput.value = toISO(today);
    // Sinkronkan jika flatpickr tersedia
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
      infoBox.style.display = 'block';
      infoBox.classList.remove('alert-info');
      infoBox.classList.add('alert-danger');
      infoBox.textContent = `Error load accounts: ${err.message}`;
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

  const getSelectedCountriesCsv = () => {
    try {
      const v = (typeof $ !== 'undefined' && countrySelect) ? $(countrySelect).val() : null;
      if (Array.isArray(v) && v.length) return v.join(',');
    } catch (e) {}
    if (!countrySelect) return '';
    const vals = Array.from(countrySelect.selectedOptions || []).map(o => o.value).filter(Boolean);
    return vals.join(',');
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
      infoBox.style.display = 'block';
      infoBox.classList.remove('alert-info');
      infoBox.classList.add('alert-danger');
      infoBox.textContent = `Error load countries: ${err.message}`;
    }
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
    
    // Check if all data is zero and show informative message
    const hasData = (sum.total_impressions || 0) > 0 || 
                   (sum.total_clicks || 0) > 0 || 
                   (sum.total_revenue || 0) > 0;
    
    if (!hasData) {
      infoBox.style.display = 'block';
      infoBox.classList.remove('alert-danger');
      infoBox.classList.add('alert-info');
      infoBox.innerHTML = `
        <strong>No AdSense data found</strong><br>
        This could mean:<br>
        • Your AdSense account is new or not yet active<br>
        • No traffic or ad impressions for the selected period<br>
        • AdSense ads are not properly configured on your website<br>
        Try selecting a different date range or check your AdSense setup.
      `;
    }
  };

  const renderRevenueChart = (rows) => {
    if (!Array.isArray(rows) || rows.length === 0) {
      if (revenueChart) {
        revenueChart.destroy();
        revenueChart = null;
      }
      return;
    }

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

    const data = {
      labels,
      datasets: [
        {
          label: 'Pendapatan Harian (Rp)',
          data: revenues,
          borderColor: '#0d6efd',
          backgroundColor: 'rgba(13,110,253,0.15)',
          tension: 0.3,
          fill: true,
        }
      ]
    };

    const options = {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          ticks: { maxRotation: 0, autoSkip: true },
        },
        y: {
          beginAtZero: true,
          title: { display: true, text: 'Rp' }
        }
      },
      plugins: {
        legend: { display: true },
        tooltip: { mode: 'index', intersect: false }
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
  };

  const generateTrafficCountryCharts = (data) => {
    if (!Array.isArray(data) || data.length === 0) {
      if (typeof $ !== 'undefined') $('#charts_section').hide();
      if (trafficCharts.impressions) { trafficCharts.impressions.destroy(); trafficCharts.impressions = null; }
      if (trafficCharts.revenue) { trafficCharts.revenue.destroy(); trafficCharts.revenue = null; }
      return;
    }

    const sorted = data.slice().sort((a, b) => (Number(b.impressions || 0) - Number(a.impressions || 0))).slice(0, 10);
    const labels = sorted.map(item => item.country_name || item.country_code || 'Unknown');
    const impressions = sorted.map(item => Number(item.impressions || 0));
    const revenue = sorted.map(item => Number(item.revenue || 0));

    if (typeof Chart === 'undefined') return;

    if (impressionsCanvas) {
      if (trafficCharts.impressions) trafficCharts.impressions.destroy();
      trafficCharts.impressions = new Chart(impressionsCanvas.getContext('2d'), {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            label: 'Total Impressions',
            data: impressions,
            backgroundColor: 'rgba(54, 162, 235, 0.6)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1
          }]
        },
        options: {
          responsive: true,
          scales: { y: { beginAtZero: true } }
        }
      });
    }

    if (revenueCanvas) {
      if (trafficCharts.revenue) trafficCharts.revenue.destroy();
      trafficCharts.revenue = new Chart(revenueCanvas.getContext('2d'), {
        type: 'doughnut',
        data: {
          labels,
          datasets: [{
            label: 'Total Revenue',
            data: revenue,
            backgroundColor: [
              '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
              '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#FF6384'
            ]
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: true,
          aspectRatio: 2
        }
      });
    }

    if (typeof $ !== 'undefined') $('#charts_section').show();
  };

  const loadSummary = async () => {
    const start_date = startInput.value;
    const end_date = endInput.value;
    const selected_account = getSelectedAccountsCsv();
    const selected_countries = getSelectedCountriesCsv();
    infoBox.style.display = 'none';

    try {
      if (typeof $ !== 'undefined' && $("#overlay").length) {
        $("#overlay").show();
      }

      const p1 = new URLSearchParams({ start_date, end_date });
      if (selected_account) p1.set('selected_account', selected_account);
      if (selected_countries) p1.set('selected_countries', selected_countries);

      const p2 = new URLSearchParams({ start_date, end_date });
      if (selected_account) p2.set('selected_account', selected_account);
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
      updateSummaryBoxes(accountJson.summary || {});
      renderRevenueChart(accountJson.data || []);
      generateTrafficCountryCharts(countryJson.data || []);

      if (typeof $ !== 'undefined' && $("#overlay").length) {
        $("#overlay").hide();
      }
    } catch (err) {
      infoBox.style.display = 'block';
      infoBox.classList.remove('alert-info');
      infoBox.classList.add('alert-danger');
      infoBox.textContent = `Error load summary: ${err.message}`;
      if (typeof $ !== 'undefined' && $("#overlay").length) {
        $("#overlay").hide();
      }
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

  loadAccounts();
  loadCountryOptions();
});