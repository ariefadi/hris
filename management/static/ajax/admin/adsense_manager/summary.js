document.addEventListener('DOMContentLoaded', () => {
  const startInput = document.getElementById('start_date');
  const endInput = document.getElementById('end_date');
  const accountSelect = document.getElementById('account_filter');
  const btnLoad = document.getElementById('btn_load_summary');
  const infoBox = document.getElementById('summary_info');
  const chartCanvas = document.getElementById('chart_revenue_daily');
  let revenueChart = null;

  // Kurs default untuk konversi USD -> IDR (bisa disesuaikan)
  const IDR_RATE = 1; // 1 USD = Rp 16.000 (digunakan hanya jika currency = USD)
  let currencyCode = 'IDR'; // default, akan di-set dari respons backend jika tersedia

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
    try {
      const res = await fetch('/management/admin/adsense_credentials_list');
      const json = await res.json();
      if (!json.status) throw new Error(json.error || 'Failed to load accounts');
      (json.data || []).forEach((acc) => {
        const opt = document.createElement('option');
        opt.value = acc.user_mail;
        opt.textContent = acc.account_name || acc.user_mail;
        accountSelect.appendChild(opt);
      });
    } catch (err) {
      infoBox.style.display = 'block';
      infoBox.classList.remove('alert-info');
      infoBox.classList.add('alert-danger');
      infoBox.textContent = `Error load accounts: ${err.message}`;
    }
  };

  const getSelectedAccount = () => {
    const opt = accountSelect.value;
    return opt || '';
  };

  const updateSummaryBoxes = (sum) => {
    document.getElementById('sum_impressions').textContent = formatNumber(sum.total_impressions || 0);
    document.getElementById('sum_clicks').textContent = formatNumber(sum.total_clicks || 0);
    document.getElementById('sum_ctr').textContent = formatPercent(sum.ctr || 0);
    const rate = currencyCode === 'USD' ? IDR_RATE : 1;
    document.getElementById('sum_cpc').textContent = formatIDR((sum.cpc || 0) * rate);
    document.getElementById('sum_ecpm').textContent = formatIDR((sum.ecpm || 0) * rate);
    document.getElementById('sum_revenue').textContent = formatIDR((sum.total_revenue || 0) * rate);
    
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

  const renderRevenueChart = (daily) => {
    if (!Array.isArray(daily) || daily.length === 0) {
      // Bersihkan chart jika tidak ada data
      if (revenueChart) {
        revenueChart.destroy();
        revenueChart = null;
      }
      return;
    }

    const labels = daily.map(d => d.date);
    const rate = currencyCode === 'USD' ? IDR_RATE : 1;
    const revenues = daily.map(d => Number(d.revenue || 0) * rate);

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

  const loadSummary = async () => {
    const start_date = startInput.value;
    const end_date = endInput.value;
    const account_filter = getSelectedAccount();
    infoBox.style.display = 'none';
    
    console.log('Loading AdSense summary with params:', { start_date, end_date, account_filter });
    
    try {
      const params = new URLSearchParams({ start_date, end_date });
      if (account_filter) params.set('account_filter', account_filter);
      
      const url = `/management/admin/adsense_summary_data/?${params.toString()}`;
      console.log('Fetching URL:', url);
      
      const res = await fetch(url);
      console.log('Response status:', res.status);
      
      const json = await res.json();
      console.log('Response data:', json);
      
      if (!json.status) throw new Error(json.error || 'Failed to load summary');
      currencyCode = (json.currency || 'IDR');
      updateSummaryBoxes(json.summary || {});
      renderRevenueChart(json.daily || []);
      
      console.log('Summary boxes updated successfully');
    } catch (err) {
      console.error('Error loading summary:', err);
      infoBox.style.display = 'block';
      infoBox.classList.remove('alert-info');
      infoBox.classList.add('alert-danger');
      infoBox.textContent = `Error load summary: ${err.message}`;
    }
  };

  btnLoad.addEventListener('click', loadSummary);
  // Inisialisasi Flatpickr jika tersedia
  if (typeof flatpickr !== 'undefined') {
    flatpickr(startInput, { dateFormat: 'Y-m-d' });
    flatpickr(endInput, { dateFormat: 'Y-m-d' });
  }
  setDefaultDates();
  loadAccounts();
  
  // Initialize Select2 for sites dropdown
  $('#account_filter').select2({
    theme: 'bootstrap4',
    placeholder: 'Pilih akun AdSense',
    allowClear: true,
    width: '100%'
  });
});