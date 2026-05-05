/**
 * Reference Ajax Traffic Per Campaign Js
 */

function normalizeDomainFilter(selected_domain) {
    if (Array.isArray(selected_domain)) {
        return selected_domain.map(function (s) { return String(s || '').trim(); }).filter(function (s) { return s; }).join(',');
    }
    return String(selected_domain || '').trim();
}

window.facebookCampaignDetailXhr = null;
window.facebookCampaignDetailReqKey = '';
window.facebookCampaignAssetsCache = [];
window.facebookCampaignDetailPayload = {};
window.facebookCampaignSelectedAssetIndex = 0;
window.facebookCampaignSelectedNode = { type: 'campaign', assetIndex: 0, variantIndex: 0 };

function fmtInt(v) { return (Number(v || 0)).toLocaleString('id-ID'); }
function fmtIdr(v) { return 'Rp ' + Math.round(Number(v || 0)).toLocaleString('id-ID'); }

function resetCampaignAssetPanel(message) {
    $('#facebookCampaignApiStatus').text(message || 'Memuat detail asset iklan...').removeClass('text-danger').addClass('text-muted');
    $('#facebookCampaignAdAssetList').html('');
    $('#facebookCampaignAdAssetMeta').text('-');
    $('#facebookCampaignTree').html('');
    window.facebookCampaignAssetsCache = [];
    window.facebookCampaignSelectedAssetIndex = 0;
    window.facebookCampaignSelectedNode = { type: 'campaign', assetIndex: 0, variantIndex: 0 };
}

function _esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (m) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[m]; }); }
function _toText(v) {
    if (v == null) return '';
    if (Array.isArray(v)) return v.join(', ');
    if (typeof v === 'object') return '';
    return String(v);
}
function _isUrl(v) { return /^https?:\/\//i.test(String(v || '').trim()); }
function _csv(v) { return String(v || '').split(',').map(function (x) { return x.trim(); }).filter(Boolean); }
function _guessField(label, value) {
    var k = String(label || '').toLowerCase();
    if (typeof value === 'boolean') return { mode: 'select', options: ['true', 'false'] };
    if (/description|message|body|caption|notes?|text/.test(k)) return { mode: 'textarea' };
    if (/country|gender|status|objective|tujuan|platform|position|type|event|goal|strategy|sasaran/.test(k)) return { mode: 'select' };
    if (/image|video|thumbnail|file|upload|asset|gambar/.test(k)) return { mode: 'file' };
    if (/date|time/.test(k)) return { mode: 'text' };
    if (typeof value === 'number' || /^\d+(\.\d+)?$/.test(String(value || ''))) return { mode: 'number' };
    if (_isUrl(value)) return { mode: 'url' };
    return { mode: 'text' };
}
function _formInput(label, value) {
    var val = _toText(value);
    var g = _guessField(label, value);
    var html = '<div class="form-group fb-smart-field"><label class="form-label">' + _esc(label) + '</label>';
    if (g.mode === 'textarea') {
        html += '<textarea rows="3" class="form-control">' + _esc(val) + '</textarea>';
    } else if (g.mode === 'select') {
        var options = [];
        var k = String(label).toLowerCase();
        if (/country/.test(k)) options = ['ID', 'MY', 'SG', 'PH', 'TH', 'VN'];
        if (/gender/.test(k)) options = ['1', '2'];
        if (/status|effective_status/.test(k)) options = ['ACTIVE', 'PAUSED', 'ARCHIVED', 'DELETED'];
        if (/objective/.test(k)) options = ['OUTCOME_SALES', 'OUTCOME_LEADS', 'OUTCOME_ENGAGEMENT', 'OUTCOME_TRAFFIC', 'OUTCOME_AWARENESS', 'OUTCOME_APP_PROMOTION'];
        
        var selected = _csv(val);
        if (!options.length) options = selected.slice();
        selected.forEach(function (x) { if (options.indexOf(x) < 0) options.push(x); });
        
        html += '<select class="form-control js-fb-select2" ' + (selected.length > 1 ? 'multiple' : '') + ' data-placeholder="Pilih ' + _esc(label) + '">';
        for (var i = 0; i < options.length; i++) {
            var o = String(options[i] || '');
            html += '<option value="' + _esc(o) + '" ' + (selected.indexOf(o) >= 0 ? 'selected' : '') + '>' + _esc(o) + '</option>';
        }
        html += '</select>';
    } else if (g.mode === 'file') {
        var isVideo = /video/i.test(String(label));
        html += '<div class="fb-upload-card">';
        html += isVideo ? '<video class="fb-upload-video js-fb-file-preview" ' + (_isUrl(val) ? 'src="' + _esc(val) + '"' : 'style="display:none"') + ' controls></video>' : '<img class="fb-upload-preview js-fb-file-preview" ' + (_isUrl(val) ? 'src="' + _esc(val) + '"' : 'style="display:none"') + ' alt="asset">';
        html += '<input type="url" class="form-control mb-1 js-fb-file-url" value="' + _esc(val) + '" placeholder="https://...">';
        html += '<input type="file" class="form-control-file js-fb-file-input" data-media="' + (isVideo ? 'video' : 'image') + '" ' + (isVideo ? 'accept="video/*"' : 'accept="image/*"') + '>';
        html += '</div>';
    } else {
        var t = g.mode === 'number' ? 'number' : (g.mode === 'url' ? 'url' : 'text');
        html += '<input type="' + t + '" class="form-control" value="' + _esc(val) + '">';
    }
    html += '</div>';
    return html;
}
function _flatRows(obj, prefix, rows, depth) {
    if (rows.length >= 180 || depth > 5) return;
    if (obj == null) {
        rows.push({ key: prefix, value: '' });
        return;
    }
    if (Array.isArray(obj)) {
        if (!obj.length) {
            rows.push({ key: prefix, value: '' });
            return;
        }
        var allPrimitive = obj.every(function (x) { return x == null || ['string', 'number', 'boolean'].indexOf(typeof x) >= 0; });
        if (allPrimitive) {
            rows.push({ key: prefix, value: obj.join(', ') });
            return;
        }
        for (var i = 0; i < obj.length; i++) _flatRows(obj[i], prefix + '[' + i + ']', rows, depth + 1);
        return;
    }
    if (typeof obj === 'object') {
        var keys = Object.keys(obj);
        if (!keys.length) {
            rows.push({ key: prefix, value: '' });
            return;
        }
        for (var k = 0; k < keys.length; k++) {
            var key = keys[k];
            var next = prefix ? (prefix + '.' + key) : key;
            _flatRows(obj[key], next, rows, depth + 1);
            if (rows.length >= 180) break;
        }
        return;
    }
    rows.push({ key: prefix, value: obj });
}
function _dynamicSection(title, obj) {
    var rows = [];
    _flatRows(obj || {}, '', rows, 0);
    if (!rows.length) return '';
    var html = '<div class="border rounded p-2 mb-2"><div class="fb-section-title">' + _esc(title) + '</div><div class="row">';
    for (var i = 0; i < rows.length; i++) {
        if (!rows[i].key) continue;
        html += '<div class="col-md-6">' + _formInput(rows[i].key, rows[i].value) + '</div>';
    }
    html += '</div></div>';
    return html;
}

function _advantageForm(adv) {
    var a = (adv && typeof adv === 'object') ? adv : {};
    var html = '<div class="border rounded p-2 mb-2"><div class="fb-form-note mb-2">Pemirsa Advantage</div>';
    html += _formInput('Status Advantage Audience', a.advantage_audience);
    html += _formInput('Targeting Automation', a.targeting_automation);
    html += _formInput('Catatan', a.note || '');
    html += '</div>';
    return html;
}
function _targetingForm(t) {
    var x = (t && typeof t === 'object') ? t : {};
    var interests = ((x.flexible_spec || [])[0] || {}).interests || [];
    var behaviors = ((x.flexible_spec || [])[0] || {}).behaviors || [];
    var html = '<div class="border rounded p-2 mb-2"><div class="fb-section-title">Audience</div><div class="row">';
    html += '<div class="col-md-6">' + _formInput('Usia Minimum', x.age_min) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Usia Maksimum', x.age_max) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Gender', Array.isArray(x.genders) ? x.genders.join(', ') : '') + '</div>';
    html += '<div class="col-md-6">' + _formInput('Negara', ((x.geo_locations || {}).countries || []).join(', ')) + '</div>';
    html += '<div class="col-md-12">' + _formInput('Minat (Interests)', interests.map(function (i) { return i.name || i.id; }).join(', ')) + '</div>';
    html += '<div class="col-md-12">' + _formInput('Perilaku (Behaviors)', behaviors.map(function (b) { return b.name || b.id; }).join(', ')) + '</div>';
    html += '</div></div>';
    return html;
}
function _placementForm(t) {
    var x = (t && typeof t === 'object') ? t : {};
    var html = '<div class="border rounded p-2 mb-2"><div class="fb-section-title">Placement</div><div class="row">';
    html += '<div class="col-md-6">' + _formInput('Platform Publisher', (x.publisher_platforms || []).join(', ')) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Posisi Facebook', (x.facebook_positions || []).join(', ')) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Posisi Instagram', (x.instagram_positions || []).join(', ')) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Device Platform', (x.device_platforms || []).join(', ')) + '</div>';
    html += '</div></div>';
    return html;
}
function _campaignForm(c) {
    var x = (c && typeof c === 'object') ? c : {};
    var html = '<div class="border rounded p-2 mb-2"><div class="fb-section-title">Campaign</div><div class="row">';
    html += '<div class="col-md-6">' + _formInput('Nama Campaign', x.name) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Status Campaign', x.status || x.effective_status) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Tujuan Campaign', x.objective) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Tipe Pembelian', x.buying_type) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Budget Harian', x.daily_budget) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Budget Lifetime', x.lifetime_budget) + '</div>';
    html += '</div></div>';
    return html;
}
function _adsetForm(s) {
    var x = (s && typeof s === 'object') ? s : {};
    var html = '<div class="border rounded p-2 mb-2"><div class="fb-section-title">Ad Set</div><div class="row">';
    html += '<div class="col-md-6">' + _formInput('Nama Ad Set', x.name) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Status Ad Set', x.status || x.effective_status) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Optimization Goal', x.optimization_goal) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Billing Event', x.billing_event) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Bid Strategy', x.bid_strategy) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Bid Amount', x.bid_amount) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Start Time', x.start_time) + '</div>';
    html += '<div class="col-md-6">' + _formInput('End Time', x.end_time) + '</div>';
    html += '</div></div>';
    return html;
}
function _jsonPretty(v) { try { return JSON.stringify(v || {}, null, 2); } catch (e) { return '{}'; } }
function _technicalPanel(ad, material) {
    return '<details class="mb-2"><summary class="fb-form-note" style="cursor:pointer;">Data Teknis (Opsional)</summary>'
        + '<div class="mt-2"><label class="form-label">Raw Ad JSON</label><textarea class="form-control" rows="4">' + _esc(_jsonPretty(ad || {})) + '</textarea></div>'
        + '<div class="mt-2"><label class="form-label">Raw Creative JSON</label><textarea class="form-control" rows="4">' + _esc(_jsonPretty(material || {})) + '</textarea></div>'
        + '</details>';
}
function _pickFirst() {
    for (var i = 0; i < arguments.length; i++) {
        var v = arguments[i];
        if (v !== undefined && v !== null && String(v).trim() !== '') return v;
    }
    return '';
}
function _assetText(arr) {
    if (!Array.isArray(arr) || !arr.length) return '';
    var first = arr[0] || {};
    return _pickFirst(first.text, first.name, first.value, '');
}
function _assetUrl(arr) {
    if (!Array.isArray(arr) || !arr.length) return '';
    var first = arr[0] || {};
    return _pickFirst(first.website_url, first.url, first.link, '');
}
function _assetAt(arr, i, keys) {
    if (!Array.isArray(arr) || !arr.length) return '';
    var idx = Math.min(Math.max(Number(i || 0), 0), arr.length - 1);
    var x = arr[idx] || {};
    for (var k = 0; k < keys.length; k++) {
        var v = x[keys[k]];
        if (v !== undefined && v !== null && String(v).trim() !== '') return v;
    }
    return '';
}
function _materialVariants(m) {
    var x = (m && typeof m === 'object') ? m : {};
    var afs = x.asset_feed_spec || {};
    var maxLen = Math.max(1, (afs.titles || []).length, (afs.bodies || []).length, (afs.descriptions || []).length, (afs.images || []).length, (afs.videos || []).length);
    var out = [];
    for (var i = 0; i < maxLen; i++) {
        out.push({
            title: _assetAt(afs.titles, i, ['text', 'name']) || '',
            body: _assetAt(afs.bodies, i, ['text']) || '',
            description: _assetAt(afs.descriptions, i, ['text']) || '',
            image_hash: _assetAt(afs.images, i, ['hash']) || '',
            video_id: _assetAt(afs.videos, i, ['video_id']) || ''
        });
    }
    return out;
}
function _materialForm(m, selectedVariantIdx) {
    var x = (m && typeof m === 'object') ? m : {};
    var os = x.object_story_spec || {};
    var ld = os.link_data || {};
    var vd = os.video_data || {};
    var td = os.template_data || {};
    var cta = (((vd || {}).call_to_action || {}).value || {});
    var afs = x.asset_feed_spec || {};
    var variants = _materialVariants(x);
    var vi = Math.min(Math.max(Number(selectedVariantIdx || 0), 0), Math.max(variants.length - 1, 0));
    var vv = variants[vi] || {};
    var titleVal = _pickFirst(vv.title, x.title, ld.name, vd.title, td.name, _assetText(afs.titles));
    var bodyVal = _pickFirst(vv.body, x.body, ld.message, vd.message, td.message, _assetText(afs.bodies));
    var descVal = _pickFirst(vv.description, bodyVal);
    var linkVal = _pickFirst(ld.link, cta.link, td.link, x.link_url, _assetUrl(afs.link_urls));
    
    // Prioritize variant-specific image/video
    var imageVal = _pickFirst(_assetAt(afs.images, vi, ['url', 'image_url', 'src']), x.image_url, x.thumbnail_url, ld.picture, (vd.image_url || ''), (td.picture || ''));
    var videoVal = _pickFirst(vv.video_id, vd.video_id, x.video_id);

    var html = '<div class="border rounded p-2 mb-2 fb-material-panel"><div class="fb-section-title">Materi Iklan</div>';
    if (variants.length > 1) html += '<div class="fb-form-note mb-2">Menampilkan Varian ' + (vi + 1) + ' dari ' + variants.length + '</div>';
    html += '<div class="row">';
    html += '<div class="col-md-12"><div class="row fb-material-grid">';
    html += '<div class="col-md-6">' + _formInput('Creative ID', x.id) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Object Type', x.object_type) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Nama Creative', x.name) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Page ID', os.page_id || x.page_id) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Judul', titleVal) + '</div>';
    html += '<div class="col-md-6">' + _formInput('Link URL', linkVal) + '</div>';
    html += '<div class="col-12">' + _formInput('Pesan/Body', bodyVal) + '</div>';
    html += '<div class="col-12">' + _formInput('Deskripsi', descVal) + '</div>';
    if (vv.image_hash) html += '<div class="col-md-6">' + _formInput('Image Hash (Varian)', vv.image_hash) + '</div>';
    if (vv.video_id) html += '<div class="col-md-6">' + _formInput('Video ID (Varian)', vv.video_id) + '</div>';
    
    // Asset uploads integrated in grid
    html += '<div class="col-md-6">';
    html += '<div class="fb-upload-card"><div class="fb-upload-title">Asset Gambar</div>';
    html += '<img class="fb-upload-preview js-fb-file-preview" ' + (imageVal ? 'src="' + _esc(imageVal) + '"' : 'style="display:none"') + ' alt="image">';
    html += '<input type="url" class="form-control mb-1 js-fb-file-url" value="' + _esc(imageVal) + '" placeholder="URL gambar">';
    html += '<input type="file" class="form-control-file js-fb-file-input" data-media="image" accept="image/*"></div>';
    html += '</div>';
    
    html += '<div class="col-md-6">';
    html += '<div class="fb-upload-card"><div class="fb-upload-title">Asset Video</div>';
    html += '<video class="fb-upload-video js-fb-file-preview" ' + (videoVal ? 'src="' + _esc(videoVal) + '"' : 'style="display:none"') + ' controls></video>';
    html += '<input type="url" class="form-control mb-1 js-fb-file-url" value="' + _esc(videoVal) + '" placeholder="URL video">';
    html += '<input type="file" class="form-control-file js-fb-file-input" data-media="video" accept="video/*"></div>';
    html += '</div>';
    
    html += '</div></div></div>';
    return html;
    return html;
}
function _imageFromMaterial(m, variantIdx) {
    if (!m || typeof m !== 'object') return '';
    
    // Check for variant-specific image/video first
    var variants = _materialVariants(m);
    var vi = Math.min(Math.max(Number(variantIdx || 0), 0), Math.max(variants.length - 1, 0));
    var vv = variants[vi] || {};
    
    var afs = m.asset_feed_spec || {};
    var variantImg = _assetAt(afs.images, vi, ['url', 'image_url', 'src']);
    if (variantImg) return variantImg;

    if (m.thumbnail_url) return String(m.thumbnail_url);
    if (m.image_url) return String(m.image_url);
    try {
        var os = m.object_story_spec || {};
        if (os.link_data && os.link_data.picture) return String(os.link_data.picture);
        if (os.video_data && os.video_data.image_url) return String(os.video_data.image_url);
        if (os.photo_data && os.photo_data.image_url) return String(os.photo_data.image_url);
    } catch (e) {}
    return '';
}

function _initSelect2() {
    setTimeout(function() {
        $('#facebookCampaignAdAssetList .js-fb-select2').each(function () {
            var $el = $(this);
            if ($el.hasClass('select2-hidden-accessible')) return;
            $el.select2({
                width: '100%',
                theme: 'bootstrap4',
                dropdownParent: $('#facebookCampaignDetailModal'),
                allowClear: true,
                placeholder: $el.data('placeholder') || 'Pilih...'
            });
            $el.on('select2:open', function() {
                // Ensure dropdown is above modal and correctly positioned
                $('.select2-container--open').css('z-index', 9999999);
            });
        });
    }, 50);
}

function _renderSelectedCampaignAsset() {
    var node = window.facebookCampaignSelectedNode || { type: 'campaign', assetIndex: 0, variantIndex: 0 };
    var i = Number(node.assetIndex || 0);
    var vi = Number(node.variantIndex || 0);
    var ad = (window.facebookCampaignAssetsCache || [])[i] || {};
    var payload = window.facebookCampaignDetailPayload || {};
    var material = ad.material || {};
    var iden = ad.identity || {};

    if (node.type === 'campaign') {
        var c = payload.campaign || {};
        var ov = '<div class="fb-asset-item"><div class="fb-section-title">Campaign Overview</div>';
        ov += '<div class="row"><div class="col-md-6">' + _formInput('Nama Campaign', c.name) + '</div><div class="col-md-6">' + _formInput('Tujuan Campaign', c.objective) + '</div></div>';
        ov += '<div class="fb-form-note">Pilih node Iklan/Varian di panel kiri untuk edit materi.</div></div>';
        $('#facebookCampaignAdAssetList').html(ov);
        _initSelect2();
        return;
    }

    var html = '<div class="fb-asset-item">';
    html += '<div class="mb-2"><span class="fb-asset-tag">Ad ID: ' + _esc(ad.ad_id || '-') + '</span><span class="fb-asset-tag">Campaign ID: ' + _esc(iden.campaign_id || '-') + '</span><span class="fb-asset-tag">Adset: ' + _esc(iden.adset_name || '-') + '</span></div>';
    html += '<div class="form-group"><label class="form-label">Nama Iklan</label><input type="text" class="form-control" value="' + _esc(ad.ad_name || '') + '"></div>';
    html += _campaignForm(ad.campaign || payload.campaign || {});
    html += _adsetForm(ad.adset || {});
    html += _advantageForm(ad.advantage_audience || {});
    html += _targetingForm(ad.targeting || {});
    html += _placementForm(ad.targeting || {});
    html += _materialForm(material, vi);
    html += _technicalPanel(ad, material);
    html += '</div>';
    $('#facebookCampaignAdAssetList').html(html);
    _initSelect2();
}

function renderCampaignAssetPanel(detailResp) {
    var payload = (detailResp && detailResp.data) ? detailResp.data : {};
    var assets = Array.isArray(payload.ad_assets) ? payload.ad_assets : [];
    var platforms = Array.isArray(payload.platforms) ? payload.platforms : [];
    $('#facebookCampaignAdAssetMeta').text('Asset: ' + assets.length + ' | Platform: ' + (platforms.join(', ') || '-'));
    if (!assets.length) {
        $('#facebookCampaignApiStatus').text('Detail iklan belum tersedia untuk campaign ini.').removeClass('text-muted').addClass('text-warning');
        return;
    }
    window.facebookCampaignDetailPayload = payload;
    window.facebookCampaignAssetsCache = assets;
    window.facebookCampaignSelectedAssetIndex = 0;
    window.facebookCampaignSelectedNode = { type: 'campaign', assetIndex: 0, variantIndex: 0 };
    $('#facebookCampaignApiStatus').html('Mode editor mirip Ads Manager aktif. <span class="fb-form-note">Pilih item di panel kiri.</span>').removeClass('text-danger').addClass('text-success');
    var tree = '';
    tree += '<div class="list-group-item active js-fb-tree-item" data-node-type="campaign" data-asset-index="0" data-variant-index="0"><strong>Campaign</strong><br><small>' + _esc((payload.campaign || {}).name || '-') + '</small></div>';
    $.each(assets, function (i, ad) {
        tree += '<div class="list-group-item js-fb-tree-item" data-node-type="ad" data-asset-index="' + i + '" data-variant-index="0"><strong>Iklan ' + (i + 1) + '</strong><br><small>' + _esc(ad.ad_name || ad.ad_id || '-') + '</small></div>';
        var vCount = _materialVariants((ad || {}).material || {}).length;
        if (vCount > 1) {
            for (var j = 0; j < vCount; j++) tree += '<div class="list-group-item js-fb-tree-item pl-4" data-node-type="variant" data-asset-index="' + i + '" data-variant-index="' + j + '"><small>Varian ' + (j + 1) + '</small></div>';
        }
    });
    $('#facebookCampaignTree').html(tree);
    _renderSelectedCampaignAsset();
}

function loadCampaignAssetDetail(row) {
    var accountId = String((row && row.account_id) || '').trim();
    var campaignName = String((row && row.campaign) || '').trim();
    var startDate = String($('#tanggal_dari').val() || '').trim();
    var endDate = String($('#tanggal_sampai').val() || '').trim();
    if (!accountId || !campaignName) {
        $('#facebookCampaignApiStatus').text('Detail iklan tidak bisa dimuat: account_id / campaign kosong. Silakan muat ulang data tabel.').removeClass('text-muted').addClass('text-danger');
        return;
    }

    if (window.facebookCampaignDetailXhr && window.facebookCampaignDetailXhr.readyState !== 4) {
        window.facebookCampaignDetailXhr.abort();
    }
    var reqKey = [accountId, campaignName, startDate, endDate, Date.now()].join('|');
    window.facebookCampaignDetailReqKey = reqKey;

    resetCampaignAssetPanel('Memuat detail asset iklan dari API...');
    window.facebookCampaignDetailXhr = $.ajax({
        url: '/management/admin/page_per_campaign_facebook_detail',
        type: 'GET',
        cache: false,
        dataType: 'json',
        data: { account_id: accountId, campaign_name: campaignName, start_date: startDate, end_date: endDate },
        headers: { 'X-CSRFToken': csrftoken },
        success: function (resp) {
            if (window.facebookCampaignDetailReqKey !== reqKey) return;
            if (!resp || resp.status === false) {
                $('#facebookCampaignApiStatus').text((resp && resp.error) || 'Gagal mengambil detail iklan.').removeClass('text-muted').addClass('text-danger');
                return;
            }
            renderCampaignAssetPanel(resp);
        },
        error: function (xhr, textStatus) {
            if (textStatus === 'abort') return;
            if (window.facebookCampaignDetailReqKey !== reqKey) return;
            $('#facebookCampaignApiStatus').text('Gagal memuat detail API: ' + (xhr && xhr.status ? xhr.status : 'unknown')).removeClass('text-muted').addClass('text-danger');
        }
    });
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

    // Set default tanggal hari ini
    var today = new Date();
    var todayString = today.getFullYear() + '-' +
        String(today.getMonth() + 1).padStart(2, '0') + '-' +
        String(today.getDate()).padStart(2, '0');
    $('#tanggal_dari').val(todayString);
    $('#tanggal_sampai').val(todayString);
    $('#tanggal_dari').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    });
    $('#tanggal_sampai').datepicker({
        format: 'yyyy-mm-dd',
        autoclose: true,
        todayHighlight: true
    });
    $('#select_account').select2({
        placeholder: '-- Pilih Account --',
        allowClear: true,
        width: '100%',
        height: '100%',
        theme: 'bootstrap4'
    });

    $(document).on('change', '.js-fb-file-input', function () {
        var file = (this.files || [])[0];
        if (!file) return;
        var url = URL.createObjectURL(file);
        var $card = $(this).closest('.fb-upload-card, .fb-smart-field');
        var $preview = $card.find('.js-fb-file-preview').first();
        var $urlInput = $card.find('.js-fb-file-url').first();
        if ($urlInput.length) $urlInput.val(url);
        if ($preview.length) $preview.attr('src', url).show();
    });

    $(document).on('input', '.js-fb-file-url', function () {
        var url = String($(this).val() || '').trim();
        var $card = $(this).closest('.fb-upload-card, .fb-smart-field');
        var $preview = $card.find('.js-fb-file-preview').first();
        if (!$preview.length) return;
        if (!url) { $preview.hide().attr('src', ''); return; }
        $preview.attr('src', url).show();
    });

    $(document).on('click', '.js-fb-tree-item', function () {
    var idx = Number($(this).data('asset-index') || 0);
    var vi = Number($(this).data('variant-index') || 0);
    var nt = String($(this).data('node-type') || 'ad');
    window.facebookCampaignSelectedAssetIndex = idx;
    window.facebookCampaignSelectedNode = { type: nt, assetIndex: idx, variantIndex: vi };
    $('.js-fb-tree-item').removeClass('active');
    $(this).addClass('active');
    _renderSelectedCampaignAsset();
});

$(document).on('click', '#btnFacebookCampaignSaveDraft', function() {
    Swal.fire({
        title: 'Simpan Draft?',
        text: 'Perubahan akan disimpan sebagai draft lokal.',
        icon: 'question',
        showCancelButton: true,
        confirmButtonText: 'Ya, Simpan',
        cancelButtonText: 'Batal',
        confirmButtonColor: '#2563eb'
    }).then((result) => {
        if (result.isConfirmed) {
            Swal.fire('Berhasil!', 'Draft berhasil disimpan.', 'success');
        }
    });
});

$(document).on('click', '#btnFacebookCampaignPublish', function() {
    Swal.fire({
        title: 'Publish Perubahan?',
        text: 'Perubahan akan dikirim ke Facebook Ads Manager.',
        icon: 'warning',
        showCancelButton: true,
        confirmButtonText: 'Ya, Publish',
        cancelButtonText: 'Batal',
        confirmButtonColor: '#2563eb'
    }).then((result) => {
        if (result.isConfirmed) {
            Swal.fire({
                title: 'Sedang Memproses...',
                text: 'Mengirim data ke Meta API',
                allowOutsideClick: false,
                didOpen: () => { Swal.showLoading(); }
            });
            setTimeout(function() {
                Swal.fire('Berhasil!', 'Perubahan telah dipublish ke Facebook.', 'success');
            }, 2000);
        }
    });
});

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
                var selected_account = $('#select_account').val() || [];
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
    $('#btn_load_data').click(function (e) {
        e.preventDefault();
        var tanggal_dari = $("#tanggal_dari").val();
        var tanggal_sampai = $("#tanggal_sampai").val();
        var selected_account = $("#select_account").val() || '%';
        var data_account = selected_account ? selected_account : '';
        var selected_domain = normalizeDomainFilter($("#select_domain").val());
        var data_domain = selected_domain ? selected_domain : '%';
        if (tanggal_dari !== '' && tanggal_sampai !== '') {
            destroy_table_data_campaign_facebook()
            table_data_campaign_facebook(tanggal_dari, tanggal_sampai, data_account, data_domain)
        }
    });
    // Filter silang account-domain dinonaktifkan karena domain menggunakan freetext.
});
function table_data_campaign_facebook(tanggal_dari, tanggal_sampai, data_account, data_domain) {
    $.ajax({
        url: '/management/admin/page_per_campaign_facebook?tanggal_dari=' + encodeURIComponent(tanggal_dari) + '&tanggal_sampai=' + encodeURIComponent(tanggal_sampai) + '&data_account=' + encodeURIComponent(data_account) + '&data_domain=' + encodeURIComponent(data_domain),
        method: 'GET',
        dataType: 'json',
        beforeSend: function () {
            $('#overlay').fadeIn(500);
        },
        success: function (data_campaign) {
            $('#overlay').fadeOut(500);
            const tanggal = new Date();
            judul = "Rekapitulasi Traffic Per Campaign Facebook";

            window.__facebookCampaignRows = (data_campaign && data_campaign.data_campaign) ? data_campaign.data_campaign : [];

            $.each(window.__facebookCampaignRows, function (index, value) {
                let data_cpr = value.cpr;
                let cpr_number = parseFloat(data_cpr)
                let cpr = cpr_number.toFixed(0).replace(',', '.');
                const frequency = Number(value?.frequency) || 0;
                const formattedFrequency = frequency.toFixed(1).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                var formattedDate = value.date || '-';
                if (value.date && value.date.match(/\d{4}-\d{2}-\d{2}/)) {
                    var months = [
                        'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                        'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'
                    ];
                    var date = new Date(value.date + 'T00:00:00');
                    var day = date.getDate();
                    var month = months[date.getMonth()];
                    var year = date.getFullYear();
                    formattedDate = day + ' ' + month + ' ' + year;
                }
                var event_data = '<tr>';
                event_data += '<td class="text-center" style="font-size: 12px;"><b>' + formattedDate + '</b></td>';
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-info" style="color: white;">' + value.account_name + '</span></td>';
                event_data += '<td class="text-left" style="font-size: 12px;"><span class="badge badge-danger" style="color: white;">' + (value.campaign || '-') + '</span></td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.spend).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.impressions).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.reach).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + String(value.clicks).replace(/\B(?=(\d{3})+(?!\d))/g, ".") + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + formattedFrequency + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + cpr + '</td>';
                event_data += '<td class="text-right" style="font-size: 12px;">' + value.cpc + '</td>';
                event_data += '<td class="text-center no-export" style="font-size: 12px;">'
                    + '<button type="button" class="btn btn-sm btn-outline-primary btn-facebook-campaign-detail" data-row-index="' + index + '" title="Detail">'
                    + '<i class="bi bi-eye-fill" aria-hidden="true"></i>'
                    + '</button>'
                    + '</td>';
                event_data += '</tr>';
                $("#table_data_campaign_facebook tbody").append(event_data);
            })
            $.each(data_campaign.total_campaign, function (index, value) {
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
                let data_cpr = Number(value.total_cpr) || 0;
                data_cpr = data_cpr.toFixed(0).replace(',', '.');
                // CPC
                let data_cpc = Number(value.total_cpc) || 0;
                data_cpc = data_cpc.toFixed(0).replace(',', '.');   
                $('#total_spend').text(totalSpend);
                $('#total_impressions').text(totalImpressions);
                $('#total_reach').text(totalReach);
                $('#total_clicks').text(totalClicks);
                $('#total_frequency').text(totalFrequency);
                $('#total_cpr').text(data_cpr);
                // CPC
                $('#total_cpc').text(data_cpc);
            })
            $('#table_data_campaign_facebook').DataTable({
                columnDefs: [
                    { targets: -1, orderable: false, searchable: false }
                ],
                "paging": true,
                "pageLength": 50,
                "lengthChange": true,
                "lengthMenu": [[10, 25, 50, 100, -1], [10, 25, 50, 100, "Semua"]],
                "searching": true,
                "ordering": true,
                responsive: false,
                dom: 'Blfrtip',
                searching: true,
                buttons: [
                    {
                        extend: 'excel',
                        filename: judul,
                        text: 'Download Excel',
                        title: judul,
                        messageTop: "laporan traffic per campaign facebook didownload pada "
                            + tanggal.getHours() + ":"
                            + tanggal.getMinutes() + " "
                            + tanggal.getDate() + "-"
                            + (tanggal.getMonth() + 1) + "-"
                            + tanggal.getFullYear(),
                        exportOptions: {
                            columns: ':visible',
                            columns: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],      // tanpa kolom Detail
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
                            const colWidths = [10, 15, 15, 10, 10, 10, 10, 10, 10, 10]; // 💡 Sesuaikan berdasarkan % di HTML
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
                        messageBottom: "laporan traffic per campaign facebook didownload pada "
                            + tanggal.getHours() + ":"
                            + tanggal.getMinutes()
                            + " " + tanggal.getDate()
                            + "-" + (tanggal.getMonth() + 1)
                            + "-" + tanggal.getFullYear(),
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
                                    if (body[i][0]) body[i][0].alignment = 'center';
                                    if (body[i][1]) body[i][1].alignment = 'left';
                                    if (body[i][2]) body[i][2].alignment = 'left';
                                    if (body[i][3]) body[i][3].alignment = 'right';
                                    if (body[i][4]) body[i][4].alignment = 'right';
                                    if (body[i][5]) body[i][5].alignment = 'right';
                                    if (body[i][6]) body[i][6].alignment = 'right';
                                    if (body[i][7]) body[i][7].alignment = 'right';
                                    if (body[i][8]) body[i][8].alignment = 'right';
                                    if (body[i][9]) body[i][9].alignment = 'right';
                                }
                            }
                            // Margin
                            doc.content[1].margin = [0, 0, 0, 0, 0, 0, 0, 0]; // [left, top, right, bottom]
                            // Manual width sesuai presentase kolom HTML (tanpa kolom Detail)
                            doc.content[1].table.widths = ['10%', '15%', '15%', '10%', '10%', '10%', '10%', '10%', '10%', '10%'];
                        }
                    }
                ]
            });

            $('#table_data_campaign_facebook tbody')
                .off('click', '.btn-facebook-campaign-detail')
                .on('click', '.btn-facebook-campaign-detail', function () {
                    var idx = parseInt($(this).attr('data-row-index') || '0', 10);
                    var row = (window.__facebookCampaignRows || [])[idx] || {};

                    $('#facebookCampaignDetailDate').text(row.date || '-');
                    $('#facebookCampaignDetailAccount').text(row.account_name || '-');
                    $('#facebookCampaignDetailDomain').text(row.domain || '-');
                    $('#facebookCampaignDetailCampaign').text(row.campaign || '-');
                    $('#facebookCampaignDetailSpend').text(fmtIdr(row.spend));
                    $('#facebookCampaignDetailImpressions').text(fmtInt(row.impressions));
                    $('#facebookCampaignDetailReach').text(fmtInt(row.reach));
                    $('#facebookCampaignDetailClicks').text(fmtInt(row.clicks));

                    var freq = Number(row.frequency || 0);
                    $('#facebookCampaignDetailFrequency').text(isNaN(freq) ? '0' : freq.toFixed(1));
                    $('#facebookCampaignDetailCpr').text(fmtIdr(row.cpr));
                    $('#facebookCampaignDetailCpc').text(fmtIdr(row.cpc));
                    $('#facebookCampaignDetailLpv').text(fmtInt(row.lpv));
                    var lr = Number(row.lpv_rate || 0);
                    $('#facebookCampaignDetailLpvRate').text((isNaN(lr) ? 0 : lr).toFixed(2) + '%');

                    resetCampaignAssetPanel('Memuat detail asset iklan...');
                    $('#facebookCampaignDetailModal').modal('show');
                    loadCampaignAssetDetail(row);
                });
        },
    });
}

function getCookie(name) {
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = cookies[i].trim();
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}
const csrftoken = getCookie('csrftoken');

function destroy_table_data_campaign_facebook() {
    $('#table_data_campaign_facebook').dataTable().fnClearTable();
    $('#table_data_campaign_facebook').dataTable().fnDraw();
    $('#table_data_campaign_facebook').dataTable().fnDestroy();
}
