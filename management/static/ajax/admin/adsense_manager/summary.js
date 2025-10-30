document.addEventListener('DOMContentLoaded', () => {
  const startInput = document.getElementById('start_date');
  const endInput = document.getElementById('end_date');
  const sitesSelect = document.getElementById('selected_sites');
  const btnLoad = document.getElementById('btn_load_summary');
  const infoBox = document.getElementById('summary_info');

  const formatCurrency = (n) => {
    const num = Number(n || 0);
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(num);
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
    // Set to September 2024 to ensure we have historical data
    const firstDay = new Date(2024, 8, 1); // September 1, 2024 (month is 0-indexed)
    const lastDay = new Date(2024, 8, 30);  // September 30, 2024
    
    startInput.value = toISO(firstDay);
    endInput.value = toISO(lastDay);
  };

  const loadSites = async () => {
    try {
      const res = await fetch('/management/admin/adsense_sites_list');
      const json = await res.json();
      if (!json.status) throw new Error(json.error || 'Failed to load sites');
      (json.data || []).forEach((s) => {
        const opt = document.createElement('option');
        opt.value = s.site_id;
        opt.textContent = s.site_name || s.site_id;
        sitesSelect.appendChild(opt);
      });
    } catch (err) {
      infoBox.style.display = 'block';
      infoBox.classList.remove('alert-info');
      infoBox.classList.add('alert-danger');
      infoBox.textContent = `Error load sites: ${err.message}`;
    }
  };

  const getSelectedSites = () => {
    return Array.from(sitesSelect.selectedOptions).map((o) => o.value).join(',');
  };

  const updateSummaryBoxes = (sum) => {
    document.getElementById('sum_impressions').textContent = formatNumber(sum.total_impressions || 0);
    document.getElementById('sum_clicks').textContent = formatNumber(sum.total_clicks || 0);
    document.getElementById('sum_ctr').textContent = formatPercent(sum.ctr || 0);
    document.getElementById('sum_cpc').textContent = formatCurrency(sum.cpc || 0);
    document.getElementById('sum_ecpm').textContent = formatCurrency(sum.ecpm || 0);
    document.getElementById('sum_revenue').textContent = formatCurrency(sum.total_revenue || 0);
    
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

  const loadSummary = async () => {
    const start_date = startInput.value;
    const end_date = endInput.value;
    const selected_sites = getSelectedSites();
    infoBox.style.display = 'none';
    
    console.log('Loading AdSense summary with params:', { start_date, end_date, selected_sites });
    
    try {
      const params = new URLSearchParams({ start_date, end_date });
      if (selected_sites) params.set('selected_sites', selected_sites);
      
      const url = `/management/admin/adsense_summary_data/?${params.toString()}`;
      console.log('Fetching URL:', url);
      
      const res = await fetch(url);
      console.log('Response status:', res.status);
      
      const json = await res.json();
      console.log('Response data:', json);
      
      if (!json.status) throw new Error(json.error || 'Failed to load summary');
      updateSummaryBoxes(json.summary || {});
      
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
  setDefaultDates();
  loadSites();
});