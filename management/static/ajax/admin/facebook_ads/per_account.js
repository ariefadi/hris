/**
 * Reference Ajax Traffic Per Account Js
 */

function normalizeDomainFilter(selected_domain) {
    if (Array.isArray(selected_domain)) {
        return selected_domain.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
    }
    return String(selected_domain || '').trim();
}

function showHrisFacebookLoader(message) {
    var msg = String(message || 'Memuat data...').trim() || 'Memuat data...';
    if (window.HrisLoader && typeof window.HrisLoader.show === 'function') {
        window.HrisLoader.show(msg);
        return;
    }
    var $overlay = $('#overlay');
    if ($overlay.length) {
        $overlay.attr('data-loader-message', msg).stop(true, true).fadeIn(200);
    }
}

function hideHrisFacebookLoader() {
    if (window.HrisLoader && typeof window.HrisLoader.forceHide === 'function') {
        window.HrisLoader.forceHide();
        return;
    }
    $('#overlay').stop(true, true).fadeOut(200);
}

function appendCsvUnique(currentValue, item) {
    const list = String(currentValue || '').split(',').map(function(v){ return String(v || '').trim().toUpperCase(); }).filter(Boolean);
    const token = String(item || '').trim().toUpperCase();
    if (token && list.indexOf(token) < 0) list.push(token);
    return list.join(',');
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
    $('.select2').select2()
    if (window.HrisDatepicker) {
        HrisDatepicker.initRange('#tanggal_dari', '#tanggal_sampai', { skipDefaults: true });
    }
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    })
    const isCreateCampaignPage = $('#table_create_campaign_meta').length > 0;
    if (!isCreateCampaignPage && $('#modalCreateMetaCampaign').length) {
        $('#modalCreateMetaCampaign').remove();
    }
    $('#select_domain').select2({
        placeholder: 'ketik subdomain…',
        allowClear: true,
        width: '100%',
        theme: 'bootstrap4',
        tags: true,
        tokenSeparators: [','],
        minimumInputLength: 1,
        ajax: {
            url: '/management/admin/facebook_domain_suggest',
            dataType: 'json',
            delay: 250,
            data: function (params) {
                var selected_account = $('#select_account').val();
                var selected_account_csv = Array.isArray(selected_account)
                    ? selected_account.join(',')
                    : (selected_account || '');
                return {
                    q: params.term || '',
                    start_date: $('#tanggal_dari').val() || '',
                    end_date: $('#tanggal_sampai').val() || '',
                    selected_account: selected_account_csv
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

    $('#btn_load_data').click(function (e) {
        e.preventDefault();
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#select_account").val() || '%';
        var data_account = selected_account ? selected_account : '%';
        var selected_domain = normalizeDomainFilter($('#select_domain').val());
        var data_domain = selected_domain ? selected_domain : '%';
        if(tanggal_dari && tanggal_dari !== '' && data_account!="") {
            var $btn = $('#btn_load_data');
            var btnHtml = $btn.html();
            $btn.prop('disabled', true);
            destroy_table_data_per_account_facebook();
            table_data_per_account_facebook(tanggal_dari, tanggal_sampai, data_account, data_domain, function onDone() {
                $btn.prop('disabled', false).html(btnHtml);
            });
        }    
    });
    let wizardStep = 1;
    const stepGuide = {
        1: 'Isi data Campaign terlebih dahulu: nama, account, objective, dan status.',
        2: 'Lengkapi Ad Set: budget, audience, optimization, dan jadwal.',
        3: 'Lengkapi Ad Creative: identity, konten iklan, dan URL tracking.'
    };
    const requiredByStep = {
        1: ['#modal_campaign_name','#modal_account','#modal_objective','#modal_status'],
        2: ['#modal_adset_name','#modal_conversion_location','#modal_optimization_goal','#modal_daily_budget','#modal_location_include_countries'],
        3: ['#modal_ad_name','#modal_page_id','#modal_website_url','#modal_cta']
    };
    const fieldLabel = {
        '#modal_campaign_name':'Nama Campaign','#modal_account':'Account','#modal_objective':'Objective','#modal_status':'Status',
        '#modal_adset_name':'Nama Set Iklan','#modal_conversion_location':'Konversi','#modal_optimization_goal':'Target Kinerja','#modal_daily_budget':'Daily Budget','#modal_location_include_countries':'Lokasi Termasuk (Negara)',
        '#modal_ad_name':'Nama Ad','#modal_page_id':'Page ID','#modal_website_url':'Website URL','#modal_cta':'CTA'
    };
    const updateStepProgress = function(){
        const req = requiredByStep[wizardStep] || [];
        let filled = 0; const missing = [];
        req.forEach(function(sel){
            const v = String($(sel).val() || '').trim();
            if(v) filled += 1; else missing.push(fieldLabel[sel] || sel);
        });
        const pct = req.length ? Math.round((filled/req.length)*100) : 0;
        $('#metaStepProgressLabel').text(pct + '% lengkap');
        $('#metaStepProgressBar').css('width', pct + '%');
        $('#metaStepMissing').text(missing.length ? ('Belum lengkap: ' + missing.join(', ')) : 'Semua field wajib sudah lengkap.');
        $('#metaStepGuide').text(stepGuide[wizardStep] || 'Lengkapi field wajib di step ini lalu klik tombol aksi.');
    };
    const syncCampaignBudgetFields = function(){
        const type = String($('#modal_campaign_budget_type').val() || 'none');
        const isDaily = type === 'daily';
        const isLifetime = type === 'lifetime';
        $('#modal_campaign_daily_budget').prop('disabled', !isDaily).prop('required', isDaily);
        $('#modal_campaign_lifetime_budget').prop('disabled', !isLifetime).prop('required', isLifetime);
    };
    const setWizardStep = function (step) {
        wizardStep = Math.max(1, Math.min(3, Number(step) || 1));
        ['1','2','3'].forEach(function (n) {
            $('#chip'+n).toggleClass('active', Number(n) === wizardStep);
            $('#pane'+n).toggleClass('active', Number(n) === wizardStep);
        });
        $('#btnWizardPrev').prop('disabled', wizardStep === 1);
        $('#btnWizardNext').prop('disabled', wizardStep === 3);
        $('#btnWizardCreateCampaign').toggle(wizardStep === 1);
        $('#btnWizardCreateAdsetAd').toggle(wizardStep >= 2);
        updateStepProgress();
    };

    $('#btnWizardPrev').click(function(){ setWizardStep(wizardStep - 1); });
    $('#btnWizardNext').click(function(){ setWizardStep(wizardStep + 1); });

    $('#btnWizardCreateCampaign').click(function(){
        const accountId = String($('#modal_account').val() || '').trim();
        const campaignName = String($('#modal_campaign_name').val() || '').trim();
        const objective = String($('#modal_objective').val() || 'OUTCOME_TRAFFIC').trim();
        const status = String($('#modal_status').val() || 'PAUSED').trim();
        const buyingType = String($('#modal_campaign_buying_type').val() || 'AUCTION').trim();
        const specialCategory = String($('#modal_campaign_special_category').val() || 'NONE').trim();
        const campaignBudgetType = String($('#modal_campaign_budget_type').val() || 'none').trim();
        const campaignDailyBudget = String($('#modal_campaign_daily_budget').val() || '').trim();
        const campaignLifetimeBudget = String($('#modal_campaign_lifetime_budget').val() || '').trim();
        const campaignSpendCap = String($('#modal_campaign_spend_cap').val() || '').trim();
        if (!accountId || !campaignName) { alert('Account dan nama campaign wajib diisi.'); return; }
        if (campaignBudgetType === 'daily') {
            const dailyNum = Number(campaignDailyBudget || 0);
            if (!campaignDailyBudget || !Number.isFinite(dailyNum) || dailyNum <= 0) {
                alert('Anggaran Harian Kampanye wajib diisi dan harus lebih dari 0.');
                $('#modal_campaign_daily_budget').focus();
                return;
            }
        }
        const fd = new FormData();
        fd.append('account_id', accountId); fd.append('campaign_name', campaignName); fd.append('objective', objective); fd.append('status', status);
        fd.append('buying_type', buyingType); fd.append('special_ad_category', specialCategory);
        fd.append('campaign_budget_type', campaignBudgetType); fd.append('campaign_daily_budget', campaignDailyBudget);
        fd.append('campaign_lifetime_budget', campaignLifetimeBudget); fd.append('campaign_spend_cap', campaignSpendCap);
        $('#modal_create_result').removeClass('text-danger text-success').addClass('text-muted').text('Membuat campaign...');
        $.ajax({url:'/management/admin/create_campaign_per_account',method:'POST',data:fd,headers:{"X-CSRFToken": csrftoken},processData:false,contentType:false,dataType:'json',
            success:function(res){
                if(res&&res.success){
                    const cid = String(res.campaign_id||'');
                    $('#modal_campaign_id').val(cid);
                    if(!$('#modal_adset_name').val()) $('#modal_adset_name').val(campaignName + ' - ADSET 1');
                    if(!$('#modal_ad_name').val()) $('#modal_ad_name').val(campaignName + ' - AD 1');
                    $('#modal_create_result').removeClass('text-muted text-danger').addClass('text-success').text('Campaign berhasil dibuat. Lanjutkan ke Ad Set.');
                    $('#metaSummaryCampaignId').text(cid || '-');
                    setWizardStep(2);
                } else {
                    $('#modal_create_result').removeClass('text-muted text-success').addClass('text-danger').text((res&&res.message)?res.message:'Gagal membuat campaign');
                }
            },
            error:function(xhr){ let m='Terjadi kesalahan saat membuat campaign'; try{const r=JSON.parse(xhr.responseText||'{}'); if(r&&r.message)m=r.message;}catch(e){} $('#modal_create_result').removeClass('text-muted text-success').addClass('text-danger').text(m); }
        });
    });

    $('#btnWizardCreateAdsetAd').click(function(){
        const accountId = String($('#modal_account').val() || '').trim();
        const campaignId = String($('#modal_campaign_id').val() || '').trim();
        if (!accountId || !campaignId) { alert('Buat campaign dulu pada Step 1.'); return; }
        const fd = new FormData();
        fd.append('account_id', accountId); fd.append('campaign_id', campaignId); fd.append('status', String($('#modal_status').val()||'PAUSED'));
        fd.append('adset_name', String($('#modal_adset_name').val()||'')); fd.append('ad_name', String($('#modal_ad_name').val()||''));
        fd.append('daily_budget', String($('#modal_daily_budget').val()||'50000')); fd.append('lifetime_budget', String($('#modal_lifetime_budget').val()||''));
        fd.append('budget_type', String($('#modal_budget_type').val()||'daily')); fd.append('start_time', String($('#modal_start_time').val()||'')); fd.append('end_time', String($('#modal_end_time').val()||''));
        const includeCountries = String($('#modal_location_include_countries').val()||'ID').trim();
        fd.append('countries', includeCountries || 'ID');
        fd.append('location_include_countries', includeCountries || 'ID');
        fd.append('location_exclude_countries', String($('#modal_location_exclude_countries').val()||'').trim());
        fd.append('location_include_regions', String($('#modal_location_include_regions').val()||'').trim());
        fd.append('location_include_cities', String($('#modal_location_include_cities').val()||'').trim());
        fd.append('location_exclude_regions', String($('#modal_location_exclude_regions').val()||'').trim());
        fd.append('location_exclude_cities', String($('#modal_location_exclude_cities').val()||'').trim());
        fd.append('languages', JSON.stringify($('#modal_languages').val() || []));
        const detailedTargeting = ($('#modal_detailed_targeting').select2('data') || []).map(function(x){
            return { id: String(x.id || '').trim(), name: String(x.text || '').trim() };
        }).filter(function(x){ return x.id; });
        fd.append('detailed_targeting', JSON.stringify(detailedTargeting));
        fd.append('age_min', String($('#modal_age_min').val()||'18')); fd.append('age_max', String($('#modal_age_max').val()||'65'));
        const placementMode = String($('#modal_placement_mode').val()||'auto');
        fd.append('gender', String($('#modal_gender').val()||'all')); fd.append('advantage', String($('#modal_advantage').val()||'0')); fd.append('placement_mode', placementMode);
        const deviceMode = String($('input[name="modal_device_mode"]:checked').val() || 'all');
        const platforms = $('.modal-platform:checked').map(function(){ return String($(this).val()||'').trim(); }).get();
        const positions = {};
        $('.modal-position:checked').each(function(){
            const p = String($(this).data('platform') || '').trim();
            const v = String($(this).val() || '').trim();
            if (!p || !v) return;
            if (!positions[p]) positions[p] = [];
            if (positions[p].indexOf(v) < 0) positions[p].push(v);
        });
        fd.append('placement_device_mode', deviceMode);
        fd.append('placement_platforms', JSON.stringify(platforms));
        fd.append('placement_positions', JSON.stringify(positions));
        fd.append('asset_customization', placementMode === 'manual' ? '1' : '0');
        fd.append('conversion_location', String($('#modal_conversion_location').val()||'WEBSITE')); fd.append('optimization_goal', String($('#modal_optimization_goal').val()||'LINK_CLICKS'));
        fd.append('bid_strategy', String($('#modal_bid_strategy').val()||'LOWEST_COST_WITHOUT_CAP')); fd.append('bid_amount', String($('#modal_bid_amount').val()||''));
        fd.append('attribution_window', String($('#modal_attribution_window').val()||'7d_click_1d_view')); fd.append('dynamic_creative', String($('#modal_dynamic_creative').val()||'0'));
        fd.append('page_id', String($('#modal_page_id').val()||'')); fd.append('website_url', String($('#modal_website_url').val()||''));
        fd.append('instagram_actor_id', String($('#modal_instagram_actor_id').val()||''));
        fd.append('use_existing_post', String($('#modal_use_existing_post').val()||'0')); fd.append('existing_post_id', String($('#modal_existing_post_id').val()||''));
        fd.append('primary_text', String($('#modal_primary_text').val()||'')); fd.append('headline', String($('#modal_headline').val()||''));
        fd.append('description', String($('#modal_description').val()||'')); fd.append('display_link', String($('#modal_display_link').val()||'')); fd.append('caption', String($('#modal_caption').val()||''));
        fd.append('url_tags', String($('#modal_url_tags').val()||''));
        fd.append('cta_type', String($('#modal_cta').val()||'LEARN_MORE')); fd.append('pixel_id', String($('#modal_pixel_id').val()||''));
        $('#modal_create_result').removeClass('text-danger text-success').addClass('text-muted').text('Membuat ad set + ad...');
        $.ajax({url:'/management/admin/create_adset_ad_per_account',method:'POST',data:fd,headers:{"X-CSRFToken": csrftoken},processData:false,contentType:false,dataType:'json',
            success:function(res){
                if(res&&res.success){ $('#modal_create_result').removeClass('text-muted text-danger').addClass('text-success').text('Berhasil publish. AdSet: '+(res.adset_id||'-')+' | Ad: '+(res.ad_id||'-')); setWizardStep(3); }
                else { const step=(res&&res.step)?(' [step: '+res.step+']'):''; $('#modal_create_result').removeClass('text-muted text-success').addClass('text-danger').text(((res&&res.message)?res.message:'Gagal')+step); }
            },
            error:function(xhr){ let m='Terjadi kesalahan saat membuat ad set + ad'; try{const r=JSON.parse(xhr.responseText||'{}'); if(r&&r.message)m=r.message;}catch(e){} $('#modal_create_result').removeClass('text-muted text-success').addClass('text-danger').text(m); }
        });
    });

    const locationLabelMap = {};
    const countryNameMap = { ID:'Indonesia', MY:'Malaysia', PH:'Filipina', TH:'Thailand', SG:'Singapura' };

    $('.js-loc-reco').off('click.locReco').on('click.locReco', function(){
        const country = String($(this).data('country') || '').trim().toUpperCase();
        if (country) locationLabelMap['country:'+country] = countryNameMap[country] || country;
        const $inp = $('#modal_location_include_countries');
        $inp.val(appendCsvUnique($inp.val(), country)).trigger('input');
    });

    let gmap = null;
    let gmarkers = [];
    let gcircles = [];
    let gmapLoader = null;

    const getGoogleMapsApiKey = function(){
        return String(window.GOOGLE_MAPS_API_KEY || $('meta[name="google-maps-api-key"]').attr('content') || 'AIzaSyDXEjWpqoOm72hidRShr_Tn-43MGTkT5oE').trim();
    };

    const setMapNotice = function(msg, tone){
        let $n = $('#modal_location_map_notice');
        if (!$n.length) {
            $n = $('<div id="modal_location_map_notice" class="small mt-1"></div>');
            $('#modal_location_map_link').closest('.mt-1').after($n);
        }
        const t = String(tone || 'muted').trim();
        $n.removeClass('text-muted text-warning text-danger text-success').addClass('text-' + t).text(String(msg || ''));
    };

    const loadGoogleMapsApi = function(){
        if (window.google && window.google.maps) return Promise.resolve(true);
        const key = getGoogleMapsApiKey();
        if (!key) {
            setMapNotice('Marker interaktif belum aktif: Google Maps API key belum diset.', 'warning');
            return Promise.resolve(false);
        }
        if (gmapLoader) return gmapLoader;
        gmapLoader = new Promise(function(resolve){
            const cb = '__gmapsInit_' + Date.now();
            window[cb] = function(){ try { delete window[cb]; } catch(e) {} resolve(true); };
            const s = document.createElement('script');
            s.src = 'https://maps.googleapis.com/maps/api/js?key=' + encodeURIComponent(key) + '&callback=' + cb;
            s.async = true; s.defer = true;
            s.onerror = function(){ resolve(false); };
            document.head.appendChild(s);
        });
        return gmapLoader;
    };

    const toMapQuery = function(type, raw){
        const token = String(raw || '').trim();
        const display = getLocationDisplay(type, token) || token;
        if (!display) return '';
        if (type === 'country') {
            const cc = String(token || '').trim().toUpperCase();
            if (cc && cc.length <= 3) return countryNameMap[cc] || cc;
            return display;
        }
        const m = String(display).match(/^(.*?)\s*-\s*([A-Z]{2,3})(?:\s*\((.*?)\))?$/);
        if (m) {
            const name = String(m[1] || '').trim();
            const cc = String(m[2] || '').trim().toUpperCase();
            const region = String(m[3] || '').trim();
            const country = countryNameMap[cc] || cc;
            if (name && region) return name + ', ' + region + ', ' + country;
            if (name) return name + ', ' + country;
        }
        if (type === 'city' || type === 'region') return String(display).trim() + ', Indonesia';
        return String(display).trim();
    };

    const getMapQueries = function(preferredText){
        const picked = String(preferredText || '').trim();
        const includeCities = parseCsv($('#modal_location_include_cities').val()).map(function(v){ return toMapQuery('city', v); }).filter(Boolean);
        const includeRegions = parseCsv($('#modal_location_include_regions').val()).map(function(v){ return toMapQuery('region', v); }).filter(Boolean);
        const includeCountries = parseCsv($('#modal_location_include_countries').val()).map(function(v){ return toMapQuery('country', v); }).filter(Boolean);
        const fallbackQuery = String($('#modal_location_query').val() || '').trim();
        const out = [];
        const add = function(v){ const s = String(v || '').trim(); if (s && out.indexOf(s) < 0) out.push(s); };
        add(toMapQuery('city', picked) || picked);
        includeCities.slice(0, 5).forEach(add);
        includeRegions.slice(0, 5).forEach(add);
        includeCountries.slice(0, 2).forEach(add);
        if (!out.length) add(fallbackQuery || 'Indonesia');
        return out;
    };

    const updateLocationMapLink = function(preferredText){
        const queries = getMapQueries(preferredText);
        const qRaw = queries.join(' | ');
        const q = encodeURIComponent(qRaw || 'Indonesia');
        $('#modal_location_map_link').attr('href', 'https://www.google.com/maps/search/' + q);

        loadGoogleMapsApi().then(function(ok){
            const $iframe = $('#modal_location_map_iframe');
            if (!ok) {
                $iframe.show().attr('src', 'https://maps.google.com/maps?q=' + q + '&output=embed');
                $('#modal_location_map_canvas').remove();
                return;
            }
            setMapNotice('Marker interaktif aktif.', 'success');
            let $canvas = $('#modal_location_map_canvas');
            if (!$canvas.length) {
                $canvas = $('<div id="modal_location_map_canvas" class="meta-loc-map"></div>');
                $iframe.after($canvas).hide();
            }
            if (!gmap) gmap = new google.maps.Map($canvas.get(0), { center: { lat: -2.5, lng: 118 }, zoom: 5 });
            gmarkers.forEach(function(m){ m.setMap(null); });
            gcircles.forEach(function(c){ c.setMap(null); });
            gmarkers = [];
            gcircles = [];
            const bounds = new google.maps.LatLngBounds();
            const geocoder = new google.maps.Geocoder();
            let pending = queries.length;
            queries.forEach(function(name){
                geocoder.geocode({ address: name }, function(results, status){ 
                    if (status === 'OK' && results && results[0]) {
                        const p = results[0].geometry.location;
                        const marker = new google.maps.Marker({
                            map: gmap,
                            position: p,
                            title: name,
                            icon: {
                                path: google.maps.SymbolPath.CIRCLE,
                                scale: 8,
                                fillColor: '#1d4ed8',
                                fillOpacity: 1,
                                strokeColor: '#ffffff',
                                strokeWeight: 2
                            }
                        });
                        gmarkers.push(marker);
                        const circle = new google.maps.Circle({
                            map: gmap,
                            center: p,
                            radius: 40000,
                            strokeColor: '#1d4ed8',
                            strokeOpacity: 0.55,
                            strokeWeight: 2,
                            fillColor: '#60a5fa',
                            fillOpacity: 0.15
                        });
                        gcircles.push(circle);
                        bounds.extend(p);
                    }
                    pending -= 1;
                    if (pending <= 0) {
                        if (gmarkers.length > 1) gmap.fitBounds(bounds);
                        else if (gmarkers.length === 1) { gmap.setCenter(bounds.getCenter()); gmap.setZoom(9); }
                    }
                });
            });
        });
    };

    const resolveLocationTargetField = function(mode, type){
        const m = String(mode || 'include');
        const t = String(type || 'country');
        if (t === 'country') return m === 'exclude' ? '#modal_location_exclude_countries' : '#modal_location_include_countries';
        if (t === 'region') return m === 'exclude' ? '#modal_location_exclude_regions' : '#modal_location_include_regions';
        return m === 'exclude' ? '#modal_location_exclude_cities' : '#modal_location_include_cities';
    };

    const normalizeLocationText = function(type, text){
        let t = String(text || '').trim();
        if (!t) return '';
        t = t.replace(/\s*\(key:\s*[^)]+\)\s*/ig, '').trim();
        if (type === 'country') {
            const m = t.match(/^(.+?)\s*[-,]\s*[A-Z]{2,3}(?:\b.*)?$/);
            if (m && m[1]) t = String(m[1]).trim();
        }
        return t;
    };

    const getLocationDisplay = function(type, raw){
        const v = String(raw || '').trim();
        if (!v) return '';
        const key = String(type || '') + ':' + v;
        if (locationLabelMap[key]) return normalizeLocationText(type, locationLabelMap[key]);
        if (type === 'country') return countryNameMap[v.toUpperCase()] || v;
        return '';
    };

    const renderLocationResults = function(items){
        const $wrap = $('#modal_location_search_results');
        if (!Array.isArray(items) || !items.length) {
            $wrap.html('<div class="text-muted small">Belum ada hasil pencarian.</div>');
            return;
        }
        const html = items.map(function(it){
            const text = String(it.text || '-');
            const token = String(it.token || it.id || '').replace(/"/g, '&quot;');
            const safeText = text.replace(/"/g, '&quot;');
            return '<div class="meta-loc-item"><div class="small">'+text+'</div><button type="button" class="btn btn-xs btn-success js-loc-pick" data-token="'+token+'" data-text="'+safeText+'">Pilih</button></div>';
        }).join('');
        $wrap.html(html);
    };

    const parseCsv = function(v){
        return String(v || '').split(',').map(function(x){ return String(x || '').trim(); }).filter(Boolean);
    };

    const renderLocationSummary = function(){
        if (!$('#modal_location_summary').length) {
            $('#modal_location_search_results').after('<div id="modal_location_summary" class="mt-2"></div>');
        }
        const groups = [
            {sel:'#modal_location_include_countries', cls:'success', label:'Termasuk-Negara', type:'country'},
            {sel:'#modal_location_include_regions', cls:'success', label:'Termasuk-Wilayah', type:'region'},
            {sel:'#modal_location_include_cities', cls:'success', label:'Termasuk-Kota', type:'city'},
            {sel:'#modal_location_exclude_countries', cls:'danger', label:'Kecuali-Negara', type:'country'},
            {sel:'#modal_location_exclude_regions', cls:'danger', label:'Kecuali-Wilayah', type:'region'},
            {sel:'#modal_location_exclude_cities', cls:'danger', label:'Kecuali-Kota', type:'city'}
        ];
        const chips = [];
        groups.forEach(function(g){
            parseCsv($(g.sel).val()).forEach(function(v){
                const disp = getLocationDisplay(g.type, v);
                if (!disp) return;
                const safeDisp = String(disp).replace(/"/g, '&quot;');
                const safeSel = String(g.sel).replace(/"/g, '&quot;');
                const safeVal = String(v).replace(/"/g, '&quot;');
                chips.push('<span class="badge badge-'+g.cls+' mr-1 mb-1">'+g.label+': '+safeDisp+' <button type="button" class="btn btn-xs btn-light ml-1 js-loc-remove" title="Hapus" data-sel="'+safeSel+'" data-type="'+g.type+'" data-value="'+safeVal+'">&times;</button></span>');
            });
        });
        $('#modal_location_summary').html(chips.length ? chips.join('') : '<small class="text-muted">Belum ada lokasi terpilih.</small>');
    };

    const doLocationSearch = function(opts){
        const q = String($('#modal_location_query').val() || '').trim();
        const selectedAccount = String($('#modal_account').val() || '').trim();
        const mode = String($('#modal_location_mode').val() || 'include');
        const type = String($('#modal_location_type').val() || 'country');
        const silent = !!(opts && opts.silent);
        if (!selectedAccount) {
            if (!silent) alert('Pilih akun terlebih dahulu.');
            return;
        }
        if (q.length < 2) { renderLocationResults([]); return; }
        $('#modal_location_search_results').html('<div class="text-muted small">Mencari lokasi...</div>');
        $.ajax({
            url: '/management/admin/facebook_location_suggest',
            method: 'GET',
            dataType: 'json',
            data: { q: q, selected_account: selectedAccount, location_type: type },
            success: function(res){
                const rows = (res && res.results) ? res.results : [];
                renderLocationResults(rows);
                $('#modal_location_search_results').off('click.locPick').on('click.locPick', '.js-loc-pick', function(){
                    const $btn = $(this);
                    const token = String($btn.data('token') || '').trim();
                    const pickedText = String($btn.data('text') || '').trim();
                    if (token && pickedText) locationLabelMap[String(type || 'country') + ':' + token] = normalizeLocationText(type, pickedText);
                    const target = resolveLocationTargetField(mode, type);
                    const $target = $(target);
                    const before = String($target.val() || '');
                    const after = appendCsvUnique(before, token);
                    $target.val(after).trigger('input');
                    renderLocationSummary();
                    if (pickedText) {
                        updateLocationMapLink(pickedText);
                    } else if (target === '#modal_location_include_countries') {
                        updateLocationMapLink();
                    }
                    if (before !== after) {
                        $btn.removeClass('btn-success').addClass('btn-outline-success').text('Dipilih');
                        setTimeout(function(){ $btn.text('Pilih').removeClass('btn-outline-success').addClass('btn-success'); }, 900);
                    }
                });
            },
            error: function(){
                $('#modal_location_search_results').html('<div class="text-danger small">Gagal mengambil lokasi.</div>');
            }
        });
    };

    const $searchCol = $('#modal_location_search_results').closest('[class^="col-md-"]');
    const $mapCol = $('#modal_location_map_iframe').closest('[class^="col-md-"]');
    if ($searchCol.length && $mapCol.length) {
        $searchCol.removeClass('col-md-8').addClass('col-md-12');
        $mapCol.removeClass('col-md-4').addClass('col-md-12 mt-2');
        $mapCol.insertAfter($searchCol);
    }

    let locationTypingTimer = null;
    $('#modal_location_query').off('input.locSearch keydown.locSearch').on('input.locSearch', function(){
        clearTimeout(locationTypingTimer);
        locationTypingTimer = setTimeout(function(){ doLocationSearch({silent:true}); }, 280);
    }).on('keydown.locSearch', function(e){ if(e.key === 'Enter'){ e.preventDefault(); doLocationSearch(); }});
    $('#modal_location_search_btn').off('click.locSearch').on('click.locSearch', function(){ doLocationSearch(); });

    const $advanced = $([
        '#modal_location_include_countries','#modal_location_exclude_countries',
        '#modal_location_include_regions','#modal_location_include_cities',
        '#modal_location_exclude_regions','#modal_location_exclude_cities'
    ].join(',')).closest('[class^="col-md-"]');
    $advanced.hide();
    if (!$('#btn_location_advanced').length) {
        $('#modal_location_search_btn').after(' <button type="button" class="btn btn-outline-secondary" id="btn_location_advanced">Lanjutan</button>');
        $('#btn_location_advanced').on('click', function(){ $advanced.toggle(); });
    }

    const resolveUnknownLocationLabels = function(){
        const selectedAccount = String($('#modal_account').val() || '').trim();
        if (!selectedAccount) return;
        const groups = [
            {sel:'#modal_location_include_regions', type:'region'},
            {sel:'#modal_location_include_cities', type:'city'},
            {sel:'#modal_location_exclude_regions', type:'region'},
            {sel:'#modal_location_exclude_cities', type:'city'}
        ];
        groups.forEach(function(g){
            parseCsv($(g.sel).val()).forEach(function(token){
                const key = g.type + ':' + token;
                if (!token || locationLabelMap[key]) return;
                $.ajax({
                    url: '/management/admin/facebook_location_suggest',
                    method: 'GET',
                    dataType: 'json',
                    data: { q: token, selected_account: selectedAccount, location_type: g.type },
                    success: function(res){
                        const rows = (res && res.results) ? res.results : [];
                        const hit = (rows || []).find(function(r){ return String(r.token || r.id || '').trim() === String(token).trim(); });
                        if (hit && hit.text) {
                            locationLabelMap[key] = normalizeLocationText(g.type, hit.text);
                            renderLocationSummary();
                            updateLocationMapLink();
                        }
                    }
                });
            });
        });
    };

    $('#modal_location_include_countries,#modal_location_exclude_countries,#modal_location_include_regions,#modal_location_include_cities,#modal_location_exclude_regions,#modal_location_exclude_cities').off('input.locSummary change.locSummary').on('input.locSummary change.locSummary', function(){
        renderLocationSummary();
        updateLocationMapLink();
        resolveUnknownLocationLabels();
    });

    $(document).off('click.locRemove', '#modal_location_summary .js-loc-remove').on('click.locRemove', '#modal_location_summary .js-loc-remove', function(e){
        e.preventDefault();
        const $btn = $(this);
        const sel = String($btn.data('sel') || '').trim();
        const type = String($btn.data('type') || '').trim();
        const value = String($btn.data('value') || '').trim();
        if (!sel || !value) return;
        const arr = parseCsv($(sel).val()).filter(function(v){ return String(v).trim() !== value; });
        $(sel).val(arr.join(',')).trigger('input');
        if (type) delete locationLabelMap[type + ':' + value];
        renderLocationSummary();
        updateLocationMapLink();
    });
    $('#modal_account').off('change.locSummary').on('change.locSummary', function(){ resolveUnknownLocationLabels(); renderLocationSummary(); });
    renderLocationSummary();
    updateLocationMapLink();
    resolveUnknownLocationLabels();

    // Filter silang account-domain dinonaktifkan karena domain menggunakan freetext.
});

function table_data_per_account_facebook(tanggal_dari, tanggal_sampai, data_account, data_domain, onDone) {
    showHrisFacebookLoader('Memuat data per account Facebook...');
    $.ajax({
        url: '/management/admin/page_per_account_facebook?tanggal_dari=' + encodeURIComponent(tanggal_dari) + '&tanggal_sampai=' + encodeURIComponent(tanggal_sampai) + '&data_account=' + encodeURIComponent(data_account) + '&data_domain=' + encodeURIComponent(data_domain),
        method: 'GET',
        dataType: 'json',
        success: function (data_per_account) {
            destroy_table_data_per_account_facebook()
            const tanggal = new Date();
            judul = "Rekapitulasi Traffic Per Account Facebook";
            $.each(data_per_account.data_per_account, function (index, value) {
                const date_active = value.start_time;
                // Ubah ke format yang dikenali JavaScript (tambahkan titik dua di zona waktu)
                const isoFormatted = date_active.replace(/(\+|\-)(\d{2})(\d{2})$/, '$1$2:$3');
                const date = new Date(isoFormatted);
                // Ambil komponen tanggal dan waktu
                const year = date.getFullYear();
                const month = String(date.getMonth() + 1).padStart(2, '0');
                const day = String(date.getDate()).padStart(2, '0');
                const hours = String(date.getHours()).padStart(2, '0');
                const minutes = String(date.getMinutes()).padStart(2, '0');
                const seconds = String(date.getSeconds()).padStart(2, '0');
                // Gabungkan dalam format yang diinginkan
                const formatted = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                // Budget
                const budget = Number(value?.daily_budget) || 0;
                const formattedBudget = budget.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Spend
                const spend = Number(value?.spend) || 0;
                const formattedSpend = spend.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Impressions
                const impressions = Number(value?.impressions) || 0;
                const formattedImpressions = impressions.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Reach
                const reach = Number(value?.reach) || 0;
                const formattedReach = reach.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Frequency
                const frequency = Number(value?.frequency) || 0;
                const formattedFrequency = frequency.toFixed(1).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                // Clicks
                const clicks = Number(value?.clicks) || 0;
                const formattedClicks = clicks.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // CPR
                let data_cpr = value.cpr;
                let cpr_number = parseFloat(data_cpr)
                let cpr = cpr_number.toFixed(0).replace(',', '.');
                // Logika remark overspend
                const isOverBudget = spend > budget;
                const remarkBadge = isOverBudget
                    ? '<span class="badge badge-danger" style="color: white;">Overspend</span>'
                    : '<span class="badge badge-primary" style="color: white;">Normal</span>';
                var event_data = '<tr>';
                event_data += '<td class="text-center" style="font-size: 12px;">' + (index + 1) + '</td>';
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-secondary" style="color: white;">' + (value.account_name || 'N/A') + '</span></td>';
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-info" style="color: white;">' + value.campaign_name + '</span></td>';
                if(value.status == 'ACTIVE'){
                    event_data += '<td class="text-center" style="font-size: 12px;"><span class="badge badge-primary" style="color: white;">Aktif</span></td>';
                }else if(value.status == 'PAUSED'){
                    event_data += '<td class="text-center" style="font-size: 12px;"><span class="badge badge-warning" style="color: white;">Pause</span></td>';
                }else{
                    event_data += '<td class="text-center" style="font-size: 12px;"><span class="badge badge-danger" style="color: white;">Tidak <br> Aktif</span></td>';
                }
                event_data += ' <td class="text-center" style="font-size: 12px;">' + formatted + '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' +
                                    '<input type="text" class="form-control text-right" style="font-size: 12px;" id="daily_budget_'+value.campaign_id+'" value="' + formattedBudget + '">' +
                                    '<span id="autosave-status_'+value.campaign_id+'" class="badge badge-danger" style="color: white; font-size:10px;"></span>'
                              ' </td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedSpend +  '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedImpressions +  '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedReach +  '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedClicks + '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + formattedFrequency + '</td>';
                event_data += ' <td class="text-right" style="font-size: 12px;">' + cpr + '</td>';
                event_data += ' <td class="text-center" style="font-size: 12px;">' + remarkBadge + '</td>';
                event_data += '<td class="text-center">' +
                                    '<div class="form-check form-switch">' +
                                        '<input class="form-check-input" type="checkbox" id="switch_campaign_'+value.campaign_id+'" ' + 
                                            (value.status === 'ACTIVE' ? 'checked' : '') + '>' +
                                    '</div>' +
                                    '<span id="autosave-button_'+value.campaign_id+'" class="badge badge-danger" style="color: white; font-size:10px;"></span>'
                                '</td>';
                event_data += '</tr>';  
                $("#table_data_per_account_facebook tbody").append(event_data)
                let timeout = null;
                $(`#daily_budget_${value.campaign_id}`).on('input', function () {
                    clearTimeout(timeout); // Reset timer
                    timeout = setTimeout(function () {
                        let formData = new FormData();
                        let content = $(`#daily_budget_${value.campaign_id}`).val();
                        var data_account = $("#select_account option:selected").val();
                        formData.append('account_id', data_account);
                        formData.append('campaign_id', value.campaign_id);
                        formData.append('daily_budget', content);
                        $.ajax({
                            url: '/management/admin/update_daily_budget_per_campaign',
                            method: 'POST',
                            data: formData,
                            headers: { 
                                "X-CSRFToken": csrftoken 
                            },
                            processData: false,
                            contentType: false,
                            dataType: "json",
                            success: function (data) {
                                $(`#autosave-status_${value.campaign_id}`).text('Daily Budget Diubah');
                                setTimeout(function () {
                                    var tanggal_dari = $("#tanggal_dari").val();
                                    var tanggal_sampai = $("#tanggal_sampai").val();
                                    var data_sub_domain = normalizeDomainFilter($("#select_domain").val()) || '%';
                                    table_data_per_account_facebook(tanggal_dari, tanggal_sampai, data_account, data_sub_domain);
                                }, 1000);;
                            }
                        });    
                    });
                });
                $(`#switch_campaign_${value.campaign_id}`).on('change', function () {
                    clearTimeout(timeout); // Reset timer
                    timeout = setTimeout(function () {
                        let formData = new FormData();
                        let isChecked = $(`#switch_campaign_${value.campaign_id}`).prop('checked');
                        let status = isChecked ? 'ACTIVE' : 'PAUSED';
                        let data_account = $("#select_account option:selected").val() || '%';
                        formData.append('account_id', data_account);
                        formData.append('campaign_id', value.campaign_id);
                        formData.append('switch_campaign', status);
                        
                        // Tampilkan SweetAlert loading untuk individual switch
                        const loadingAlert = Swal.fire({
                            title: 'Mengupdate Status',
                            text: 'Mohon tunggu...',
                            icon: 'info',
                            allowOutsideClick: false,
                            allowEscapeKey: false,
                            showConfirmButton: false,
                            didOpen: () => {
                                Swal.showLoading();
                            }
                        });
                        
                        $.ajax({
                            url: '/management/admin/update_switch_campaign',
                            method: 'POST',
                            data: formData,
                            headers: { 
                                "X-CSRFToken": csrftoken 
                            },
                            processData: false,
                            contentType: false,
                            dataType: "json",
                            success: function (data) {
                                Swal.close();
                                
                                if (data.success) {
                                    // Tampilkan success alert
                                    Swal.fire({
                                        title: 'Berhasil!',
                                        text: data.message || 'Status campaign berhasil diubah',
                                        icon: 'success',
                                        timer: 1500,
                                        showConfirmButton: false
                                    });
                                    
                                    setTimeout(function () {
                                        var tanggal_dari = $("#tanggal_dari").val();
                                        var tanggal_sampai = $("#tanggal_sampai").val();
                                        var data_account = $("#select_account option:selected").val() || '%';
                                        var data_sub_domain = normalizeDomainFilter($("#select_domain").val()) || '%';
                                        table_data_per_account_facebook(tanggal_dari, tanggal_sampai, data_account, data_sub_domain);
                                        // Update header switch after individual switch update
                                        updateHeaderSwitch();
                                    }, 1000);
                                } else {
                                    // Tampilkan error alert dengan pesan dari server
                                    Swal.fire({
                                        title: 'Error!',
                                        text: data.message || 'Terjadi kesalahan saat mengubah status campaign',
                                        icon: 'error',
                                        confirmButtonText: 'OK'
                                    });
                                    
                                    // Kembalikan switch ke posisi sebelumnya
                                    $(`#switch_campaign_${value.campaign_id}`).prop('checked', !$(`#switch_campaign_${value.campaign_id}`).prop('checked'));
                                }
                            },
                            error: function(xhr, status, error) {
                                Swal.close();
                                
                                let errorMessage = 'Terjadi kesalahan saat mengubah status campaign';
                                
                                // Coba parse response JSON untuk mendapatkan pesan error yang lebih spesifik
                                try {
                                    const response = JSON.parse(xhr.responseText);
                                    if (response.message) {
                                        errorMessage = response.message;
                                    }
                                } catch (e) {
                                    // Jika tidak bisa parse JSON, gunakan pesan default
                                    if (xhr.status === 500) {
                                        errorMessage = 'Terjadi kesalahan server internal';
                                    } else if (xhr.status === 404) {
                                        errorMessage = 'Endpoint tidak ditemukan';
                                    } else if (xhr.status === 403) {
                                        errorMessage = 'Akses ditolak';
                                    }
                                }
                                
                                // Tampilkan error alert
                                Swal.fire({
                                    title: 'Error!',
                                    text: errorMessage,
                                    icon: 'error',
                                    confirmButtonText: 'OK'
                                });
                                
                                // Kembalikan switch ke posisi sebelumnya
                                $(`#switch_campaign_${value.campaign_id}`).prop('checked', !$(`#switch_campaign_${value.campaign_id}`).prop('checked'));
                            }
                        });    
                    }); // Delay 1 detik sebelum mengirim permintaan
                });
            })
            $.each(data_per_account.total_per_account, function (index, value) {
                // Budget
                const budget = Number(value?.total_budget) || 0;
                const totalBudget = budget.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Spend
                const spend = Number(value?.total_spend) || 0;
                const totalSpend = spend.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Impressions
                const impressions = Number(value?.total_impressions) || 0;
                const totalImpressions = impressions.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Reach
                const reach = Number(value?.total_reach) || 0;
                const totalReach = reach.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Clicks
                const clicks = Number(value?.total_click) || 0;
                const totalClicks = clicks.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // Frequency
                const frequency = Number(value?.total_frequency) || 0;
                const totalFrequency = frequency.toFixed(1).replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                // CPR
                let data_cpr = value.total_cpr;
                let cpr_number = parseFloat(data_cpr);

                // Hitung rata-rata berdasarkan jumlah data yang ada
                if (data_per_account.data_per_account && data_per_account.data_per_account.length > 0) {
                    cpr_number = cpr_number / data_per_account.data_per_account.length;
                }

                let totalCpr = cpr_number.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ".");
                $('#total_budget').text(totalBudget);
                $('#total_spend').text(totalSpend);
                $('#total_impressions').text(totalImpressions);
                $('#total_reach').text(totalReach);
                $('#total_clicks').text(totalClicks);
                $('#total_frequency').text(totalFrequency);
                $('#total_cpr').text(totalCpr);
            })
            var table = $('#table_data_per_account_facebook').DataTable({  
                "paging": true,
                "pageLength": 50,
                "lengthChange": true,
                "lengthMenu": [[10, 25, 50, 100, -1], [10, 25, 50, 100, "Semua"]],
                "searching": true,
                "ordering": true,
                responsive: false,
                autoWidth: false,
                dom: 'Blfrtip',
                searching: true,
                buttons: [
                    {
                        extend: 'excel',
                        filename: judul,
                        text: 'Download Excel',
                        title: judul,
                        messageTop: "laporan traffic per account facebook didownload pada "
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()+" "
                                    +tanggal.getDate()+"-"
                                    +(tanggal.getMonth()+1)+"-"
                                    +tanggal.getFullYear(),
                        exportOptions: {
                            columns: ':visible', 
                            columns: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],      // include remark, exclude switch
                            modifier: {
                                search: 'applied',      // sesuai filter pencarian
                                order: 'applied'        // sesuai urutan saat itu
                            }
                        },
                        customize: function (xlsx) {
                            const sheet = xlsx.xl.worksheets['sheet1.xml'];
                            // =========================
                            // Set column width secara manual (unit: character width)
                            // =========================
                            const colWidths = [5, 12, 13, 8, 12, 10, 10, 10, 10, 10, 10, 10]; // 💡 Sesuaikan berdasarkan % di HTML
                            const cols = $('cols', sheet);
                            cols.empty(); // Kosongkan default <col> dari DataTables
                            for (let i = 0; i < colWidths.length; i++) {
                                cols.append(
                                    `<col min="${i + 1}" max="${i + 1}" width="${colWidths[i]}" customWidth="1"/>`
                                );
                            }
                            
                        }
                    },
                    {
                        extend: 'pdf',
                        orientation: 'landscape',
                        pageSize: 'A4', 
                        filename: judul,
                        text: 'Download Pdf',
                        className: 'btn btn-warning',
                        title: judul,
                        messageBottom: "laporan traffic per account facebook didownload pada "
                                    +tanggal.getHours()+":"
                                    +tanggal.getMinutes()
                                    +" "+tanggal.getDate()
                                    +"-"+(tanggal.getMonth()+1)
                                    +"-"+tanggal.getFullYear(),
                        customize: function (doc) {
                            // Header style (bold + center)
                            doc.styles.tableHeader = {
                                bold: true,
                                fontSize: 11,
                                color: 'black',
                                alignment: 'center'
                            };

                            // Ambil body tabel (data + header)
                            const body = doc.content[1].table.body;
                            // Loop dari baris kedua (index 1, karena index 0 adalah header)
                            for (let i = 1; i < body.length; i++) {
                                if (body[i]) {
                                    if (body[i][0]) body[i][0].alignment = 'center';  // No
                                    if (body[i][1]) body[i][1].alignment = 'left';    // Account Name
                                    if (body[i][2]) body[i][2].alignment = 'left';    // Campaign Name
                                    if (body[i][3]) body[i][3].alignment = 'center';  // Status
                                    if (body[i][4]) body[i][4].alignment = 'center';  // Tanggal Mulai
                                    if (body[i][5]) body[i][5].alignment = 'right';   // Daily Budget
                                    if (body[i][6]) body[i][6].alignment = 'right';   // Spend
                                    if (body[i][7]) body[i][7].alignment = 'right';   // Impressions
                                    if (body[i][8]) body[i][8].alignment = 'right';   // Reach
                                    if (body[i][9]) body[i][9].alignment = 'right';   // Clicks
                                    if (body[i][10]) body[i][10].alignment = 'right'; // Frequency
                                    if (body[i][11]) body[i][11].alignment = 'right'; // CPR
                                    if (body[i][12]) body[i][12].alignment = 'center'; // Remark
                                }
                            }
                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]
                            // Manual width sesuai presentase kolom HTML (tanpa kolom switch)
                            doc.content[1].table.widths = ['3%', '12%', '13%', '8%', '9%', '12%', '9%', '9%', '9%', '9%', '9%', '8%', '8%'];
                        }
                    }
                ]
            });
            
            // FixedColumns removed as requested
            
            // Update header switch based on campaign statuses
             window.updateHeaderSwitch = function() {
                  const campaignSwitches = $('input[id^="switch_campaign_"]');
                  const totalCampaigns = campaignSwitches.length;
                  const activeCampaigns = campaignSwitches.filter(':checked').length;
                  const headerSwitch = $('#headerSwitch');
                 
                 if (totalCampaigns === 0) {
                     headerSwitch.prop('checked', false);
                     return;
                 }
                 
                 // Logic: active if all active, or if inactive campaigns are less than half
                 const inactiveCampaigns = totalCampaigns - activeCampaigns;
                 const shouldBeActive = activeCampaigns === totalCampaigns || inactiveCampaigns < (totalCampaigns / 2);
                 
                 headerSwitch.prop('checked', shouldBeActive);
             };
            
            // Toggle all campaigns based on header switch
            window.toggleAllCampaigns = function() {
                const headerSwitch = $('#headerSwitch');
                const isChecked = headerSwitch.is(':checked');
                const action = isChecked ? 'mengaktifkan' : 'menonaktifkan';
                
                if (confirm(`Apakah Anda yakin ingin ${action} semua campaign yang ditampilkan?`)) {
                    const campaignIds = [];
                    const targetStatus = isChecked ? 'ACTIVE' : 'PAUSED';
                    // Get all campaign IDs from the form-switch elements
                    $('input[id^="switch_campaign_"]').each(function() {
                        const switchId = $(this).attr('id');
                        const campaignId = switchId.replace('switch_campaign_', '');
                        campaignIds.push(campaignId);
                    });
                    if (campaignIds.length > 0) {
                        bulkUpdateCampaignStatus(campaignIds, targetStatus);
                    } else {
                        alert('Tidak ada campaign yang ditemukan.');
                        // Revert header switch if no campaigns found
                        headerSwitch.prop('checked', !isChecked);
                    }
                } else {
                    // Revert header switch if user cancels
                    headerSwitch.prop('checked', !isChecked);
                }
            };
            
            // Fungsi untuk menampilkan/menyembunyikan loading indicator dengan SweetAlert2
            function showBulkUpdateLoader(show) {
                if (show) {
                    // Tampilkan SweetAlert loading
                    Swal.fire({
                        title: 'Mengupdate Status Campaign',
                        html: 'Mohon tunggu, proses sedang berjalan...',
                        icon: 'info',
                        allowOutsideClick: false,
                        allowEscapeKey: false,
                        showConfirmButton: false,
                        didOpen: () => {
                            Swal.showLoading();
                        }
                    });
                } else {
                    // Tutup SweetAlert loading
                    Swal.close();
                }
            }
            
            // Call updateHeaderSwitch multiple times to ensure it works
            setTimeout(function() {
                updateHeaderSwitch();
            }, 300);
            
            setTimeout(function() {
                updateHeaderSwitch();
            }, 800);
            
            setTimeout(function() {
                updateHeaderSwitch();
            }, 1500);
            
            // Update header switch after table is loaded and on any switch change
             table.on('draw', function() {
                 // Use setTimeout to ensure DOM is fully updated
                 setTimeout(function() {
                     updateHeaderSwitch();
                     
                     // Add event listeners to individual campaign switches
                     $('input[id^="switch_campaign_"]').off('change.headerUpdate').on('change.headerUpdate', function() {
                         updateHeaderSwitch();
                     });
                 }, 100);
             });
             
             // Initial update after table initialization
             setTimeout(function() {
                 updateHeaderSwitch();
             }, 500);
            
            function bulkUpdateCampaignStatus(campaignIds, status) {
                const data_account = $("#select_account option:selected").val();
                
                // Validasi parameter sebelum mengirim request
                if (!campaignIds || campaignIds.length === 0) {
                    alert('Tidak ada campaign yang ditemukan untuk diupdate.');
                    return;
                }
                
                if (!status || (status !== 'ACTIVE' && status !== 'PAUSED')) {
                    alert('Status tidak valid.');
                    return;
                }
                
                // Jika tidak ada account yang dipilih, gunakan '%' sebagai default
                const accountId = data_account || '%';
                
                // Tampilkan loading indicator
                showBulkUpdateLoader(true);
                
                $.ajax({
                    url: '/management/admin/facebook_ads/bulk_update_campaign_status/',
                    type: 'POST',
                    data: {
                        'account_id': accountId,
                        'campaign_ids': JSON.stringify(campaignIds),
                        'status': status,
                        "X-CSRFToken": csrftoken 
                    },
                    success: function(response) {
                        // Sembunyikan loading indicator
                        showBulkUpdateLoader(false);
                        
                        if (response.success) {
                            // Show success message with SweetAlert2
                            Swal.fire({
                                title: 'Berhasil!',
                                text: response.message || 'Status campaign berhasil diupdate!',
                                icon: 'success',
                                timer: 1500,
                                showConfirmButton: false
                            });
                            
                            // Refresh the table
                            setTimeout(function() {
                                var tanggal_dari = $("#tanggal_dari").val();
                                var tanggal_sampai = $("#tanggal_sampai").val();
                                var data_account = $("#select_account option:selected").val() || '%';
                                var data_sub_domain = normalizeDomainFilter($("#select_domain").val()) || '%';
                                table_data_per_account_facebook(tanggal_dari, tanggal_sampai, data_account, data_sub_domain);
                                // Update header switch after bulk update
                                updateHeaderSwitch();
                            }, 1000);
                        } else {
                            Swal.fire({
                                title: 'Error!',
                                text: response.message || 'Gagal mengupdate status campaign',
                                icon: 'error',
                                confirmButtonText: 'OK'
                            });
                        }
                    },
                    error: function(xhr, status, error) {
                        // Sembunyikan loading indicator
                        showBulkUpdateLoader(false);
                        
                        let errorMessage = 'Terjadi kesalahan saat mengupdate status campaign';
                        
                        // Coba parse response JSON untuk mendapatkan pesan error yang lebih spesifik
                        try {
                            const response = JSON.parse(xhr.responseText);
                            if (response.message) {
                                errorMessage = response.message;
                            }
                        } catch (e) {
                            // Jika tidak bisa parse JSON, gunakan pesan default
                            if (xhr.status === 500) {
                                errorMessage = 'Terjadi kesalahan server internal';
                            } else if (xhr.status === 404) {
                                errorMessage = 'Endpoint tidak ditemukan';
                            } else if (xhr.status === 403) {
                                errorMessage = 'Akses ditolak';
                            }
                        }
                        
                        Swal.fire({
                            title: 'Error!',
                            text: errorMessage,
                            icon: 'error',
                            confirmButtonText: 'OK'
                        });
                    }
                });
            }
        },
        error: function (jqXHR, exception) {
            report_eror(jqXHR, exception);
        },
        complete: function () {
            hideHrisFacebookLoader();
            if (typeof onDone === 'function') onDone();
        }
    });
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

function destroy_table_data_per_account_facebook(){
    if ($.fn.DataTable.isDataTable('#table_data_per_account_facebook')) {
        $('#table_data_per_account_facebook').DataTable().clear().destroy();
    }
    $('#table_data_per_account_facebook tbody').empty();
}

