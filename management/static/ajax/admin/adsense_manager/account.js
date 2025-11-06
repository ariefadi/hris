document.addEventListener('DOMContentLoaded', () => {
  const tbodyAccounts = document.getElementById('tbody_adsense_account');
  const tbodyUnits = document.getElementById('tbody_adsense_units');
  const info = document.getElementById('account_info');
  const btn = document.getElementById('btn_reload');
  const selectCreds = document.getElementById('select_credentials');

  let selectedUserMail = '';

  const loadCredentials = async () => {
    try {
      const res = await fetch('/management/admin/adsense_credentials_list');
      const json = await res.json();
      if (!json.status) throw new Error(json.error || 'Gagal memuat kredensial');
      const creds = json.data || [];
      selectCreds.innerHTML = '';
      if (!creds.length) {
        const opt = document.createElement('option');
        opt.value = '';
        opt.textContent = 'Tidak ada kredensial aktif';
        selectCreds.appendChild(opt);
        selectedUserMail = '';
        return;
      }
      creds.forEach((c, idx) => {
        const opt = document.createElement('option');
        opt.value = c.user_mail;
        opt.textContent = `${c.account_name} (${c.user_mail})`;
        selectCreds.appendChild(opt);
        if (idx === 0) selectedUserMail = c.user_mail;
      });

      // Initialize Select2 if available
      if (window.$ && $('#select_credentials').select2) {
        $('#select_credentials').select2({
          placeholder: 'Pilih Kredensial AdSense',
          allowClear: true,
          width: '100%',
          theme: 'bootstrap4'
        });
        $('#select_credentials').val(selectedUserMail).trigger('change');
      }
    } catch (err) {
      selectCreds.innerHTML = '<option value="">Gagal memuat kredensial</option>';
      console.error(err);
    }
  };

  const renderAccounts = (rows) => {
    tbodyAccounts.innerHTML = '';
    rows.forEach((r, idx) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="text-center">${idx + 1}</td>
        <td>${r.account_id || '-'}</td>
        <td>${r.user_mail || '-'}</td>
        <td class="text-center">${r.site_count ?? '-'}</td>
        <td class="text-center">${r.authorized ? '<span class="badge bg-success">Yes</span>' : '<span class="badge bg-secondary">No</span>'}</td>
      `;
      tbodyAccounts.appendChild(tr);
    });
  };

  const renderAdUnits = (rows) => {
    tbodyUnits.innerHTML = '';
    rows.forEach((u, idx) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="text-center">${idx + 1}</td>
        <td>${u.ad_client || '-'}</td>
        <td>${u.ad_unit_name || '-'}</td>
      `;
      tbodyUnits.appendChild(tr);
    });
  };

  const loadAccounts = async () => {
    info.style.display = 'none';
    try {
      if (window.$) { $('#overlay').show(); }
      const url = selectedUserMail ? `/management/admin/page_adsense_account?user_mail=${encodeURIComponent(selectedUserMail)}` : '/management/admin/page_adsense_account';
      const res = await fetch(url);
      const json = await res.json();
      if (!json.status) throw new Error(json.error || 'Gagal memuat data akun');
      const accounts = json.data || [];
      const units = json.ad_units || [];

      renderAccounts(accounts);
      renderAdUnits(units);

      if (!accounts.length && !units.length) {
        info.style.display = 'block';
        info.classList.remove('alert-danger');
        info.classList.add('alert-info');
        info.textContent = 'Tidak ada data AdSense untuk kredensial ini.';
      }
      if (window.$) { $('#overlay').hide(); }
    } catch (err) {
      if (window.$) { $('#overlay').hide(); }
      info.style.display = 'block';
      info.classList.remove('alert-info');
      info.classList.add('alert-danger');
      info.textContent = `Error: ${err.message}`;
    }
  };

  selectCreds.addEventListener('change', (e) => {
    selectedUserMail = e.target.value;
  });
  if (window.$ && $('#select_credentials').on) {
    $('#select_credentials').on('change', function() {
      selectedUserMail = this.value;
    });
  }
  btn.addEventListener('click', loadAccounts);

  // init
  loadCredentials().then(loadAccounts);
});