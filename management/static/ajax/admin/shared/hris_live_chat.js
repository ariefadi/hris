(function () {
    'use strict';

    if (document.documentElement.classList.contains('hris-pwa-app')) return;
    try {
        if (window.matchMedia('(max-width: 991.98px)').matches) return;
    } catch (err) {}
    var root = document.getElementById('hrisLiveChatRoot');
    if (!root) return;

    var urls = {
        heartbeat: root.getAttribute('data-heartbeat-url') || '',
        messages: root.getAttribute('data-messages-url') || '',
        send: root.getAttribute('data-send-url') || '',
        read: root.getAttribute('data-read-url') || '',
        directory: root.getAttribute('data-directory-url') || '',
        group: root.getAttribute('data-group-url') || '',
        members: root.getAttribute('data-group-members-url') || '',
        forward: root.getAttribute('data-forward-url') || '',
        profile: root.getAttribute('data-profile-url') || '',
        pin: root.getAttribute('data-pin-url') || '',
        clear: root.getAttribute('data-clear-url') || '',
        deleteMsg: root.getAttribute('data-delete-url') || ''
    };
    var defaultFoto = root.getAttribute('data-default-foto') || '';
    var panel = document.getElementById('hrisLiveChatPanel');
    var fab = document.getElementById('hrisLiveChatFab');
    var navToggle = document.getElementById('hrisLiveChatToggle');
    var listEl = document.getElementById('hrisLiveChatList');
    var threadTitle = document.getElementById('hrisLiveChatThreadTitle');
    var threadNameEl = document.getElementById('hrisLiveChatThreadName');
    var threadSubEl = document.getElementById('hrisLiveChatThreadSub');
    var messagesEl = document.getElementById('hrisLiveChatMessages');
    var inputEl = document.getElementById('hrisLiveChatInput');
    var sendBtn = document.getElementById('hrisLiveChatSend');
    var statusEl = document.getElementById('hrisLiveChatStatus');
    var closeBtn = document.getElementById('hrisLiveChatClose');
    var backBtn = document.getElementById('hrisLiveChatBack');
    var fabBadge = document.getElementById('hrisLiveChatFabBadge');
    var navBadge = document.getElementById('hrisLiveChatNavBadge');
    var fileInput = document.getElementById('hrisLiveChatFile');
    var attachBtn = document.getElementById('hrisLiveChatAttach');
    var fileChip = document.getElementById('hrisLiveChatFileChip');
    var fileNameEl = document.getElementById('hrisLiveChatFileName');
    var fileClearBtn = document.getElementById('hrisLiveChatFileClear');
    var emojiBtn = document.getElementById('hrisLiveChatEmojiBtn');
    var emojiPanel = document.getElementById('hrisLiveChatEmojiPanel');
    var emojiCatsEl = document.getElementById('hrisLiveChatEmojiCats');
    var emojiGridEl = document.getElementById('hrisLiveChatEmojiGrid');
    var emojiSearchEl = document.getElementById('hrisLiveChatEmojiSearch');
    var replyBar = document.getElementById('hrisLiveChatReplyBar');
    var replyNameEl = document.getElementById('hrisLiveChatReplyName');
    var replyPreviewEl = document.getElementById('hrisLiveChatReplyPreview');
    var replyClearBtn = document.getElementById('hrisLiveChatReplyClear');
    var sheetEl = document.getElementById('hrisLiveChatSheet');
    var sheetTitleEl = document.getElementById('hrisLiveChatSheetTitle');
    var sheetBodyEl = document.getElementById('hrisLiveChatSheetBody');
    var sheetFootEl = document.getElementById('hrisLiveChatSheetFoot');
    var sheetCloseBtn = document.getElementById('hrisLiveChatSheetClose');
    var voiceBtn = document.getElementById('hrisLiveChatVoiceBtn');
    var voiceBar = document.getElementById('hrisLiveChatVoiceBar');
    var composeBar = document.getElementById('hrisLiveChatComposeBar');
    var voiceTimerEl = document.getElementById('hrisLiveChatVoiceTimer');
    var voiceCancelBtn = document.getElementById('hrisLiveChatVoiceCancel');
    var voiceSendBtn = document.getElementById('hrisLiveChatVoiceSend');
    var threadEl = document.getElementById('hrisLiveChatThread');
    var headerPinBtn = document.getElementById('hrisLiveChatHeaderPin');
    var headerClearBtn = document.getElementById('hrisLiveChatHeaderClear');
    var headerSearchBtn = document.getElementById('hrisLiveChatHeaderSearch');
    var searchBarEl = document.getElementById('hrisLiveChatSearchBar');
    var searchInputEl = document.getElementById('hrisLiveChatSearchInput');
    var searchCloseBtn = document.getElementById('hrisLiveChatSearchClose');
    var pinnedEl = document.getElementById('hrisLiveChatPinned');
    var profilePaneEl = document.getElementById('hrisLiveChatProfilePane');

    var EMOJI_RECENT_KEY = 'hrisLiveChatEmojiRecent';
    var searchDebounceTimer = null;
    var EMOJI_CATS = [
        { id: 'recent', icon: '🕒', label: 'Terbaru' },
        { id: 'smile', icon: '😊', label: 'Senyum' },
        { id: 'gesture', icon: '👍', label: 'Gestur' },
        { id: 'heart', icon: '❤️', label: 'Hati' },
        { id: 'animal', icon: '🐶', label: 'Hewan' },
        { id: 'food', icon: '🍔', label: 'Makanan' },
        { id: 'activity', icon: '⚽', label: 'Aktivitas' },
        { id: 'travel', icon: '✈️', label: 'Perjalanan' },
        { id: 'object', icon: '💡', label: 'Benda' },
        { id: 'symbol', icon: '✅', label: 'Simbol' }
    ];
    var EMOJI_DATA = {
        smile: [
            ['😀', 'senyum'], ['😃', 'senang'], ['😄', 'tertawa'], ['😁', 'gigi'], ['😆', 'ngakak'],
            ['😅', 'keringat'], ['🤣', 'rofl'], ['😂', 'nangis ketawa'], ['🙂', 'senyum tipis'], ['😉', 'kedip'],
            ['😊', 'malu'], ['😇', 'malaikat'], ['🥰', 'sayang'], ['😍', 'cinta'], ['🤩', 'bintang'],
            ['😘', 'cium'], ['😗', 'kiss'], ['😚', 'cium mata'], ['😋', 'lezat'], ['😛', 'lidah'],
            ['😜', 'kedip lidah'], ['🤪', 'gila'], ['😝', 'julur'], ['🤑', 'uang'], ['🤗', 'peluk'],
            ['🤭', 'tahan ketawa'], ['🤫', 'diam'], ['🤔', 'pikir'], ['🫡', 'hormat'], ['😐', 'netral'],
            ['😑', 'datar'], ['😶', 'bisu'], ['😏', 'sengir'], ['😒', 'bosan'], ['🙄', 'mata ke atas'],
            ['😬', 'canggung'], ['😮‍💨', 'legas'], ['🤥', 'bohong'], ['😌', 'lega'], ['😔', 'sedih'],
            ['😪', 'ngantuk'], ['🤤', 'ngiler'], ['😴', 'tidur'], ['😷', 'sakit'], ['🤒', 'demam'],
            ['🤕', 'perban'], ['🤢', 'mual'], ['🤮', 'muntah'], ['🥴', 'pusing'], ['😵', 'pingsan'],
            ['🤯', 'meledak'], ['🤠', 'koboi'], ['🥳', 'pesta'], ['😎', 'keren'], ['🤓', 'nerd'],
            ['🧐', 'teliti'], ['😕', 'bingung'], ['😟', 'khawatir'], ['🙁', 'cemberut'], ['☹️', 'sedih'],
            ['😮', 'kaget'], ['😯', 'terkejut'], ['😲', 'shock'], ['😳', 'malu banget'], ['🥺', 'memohon'],
            ['🥹', 'haru'], ['😢', 'nangis'], ['😭', 'nangis kencang'], ['😤', 'kesal'], ['😠', 'marah'],
            ['😡', 'ngamuk'], ['🤬', 'umpat'], ['😈', 'jahat'], ['👿', 'marah setan'], ['💀', 'tengkorak'],
            ['💩', 'tahi'], ['🤡', 'badut'], ['👻', 'hantu'], ['👽', 'alien'], ['🤖', 'robot'],
            ['🙈', 'tutup mata'], ['🙉', 'tutup telinga'], ['🙊', 'tutup mulut']
        ],
        gesture: [
            ['👋', 'halo'], ['🤚', 'angkat'], ['🖐️', 'telapak'], ['✋', 'stop'], ['🖖', 'vulcan'],
            ['👌', 'ok'], ['🤌', 'jepit'], ['✌️', 'damai'], ['🤞', 'semoga'], ['🤟', 'love you'],
            ['🤘', 'rock'], ['🤙', 'telepon'], ['👈', 'kiri'], ['👉', 'kanan'], ['👆', 'atas'],
            ['👇', 'bawah'], ['☝️', 'tunjuk'], ['👍', 'like bagus'], ['👎', 'tidak'], ['✊', 'tinju'],
            ['👊', 'pukul'], ['🤛', 'kiri tinju'], ['🤜', 'kanan tinju'], ['👏', 'tepuk'], ['🙌', 'hore'],
            ['👐', 'buka'], ['🤲', 'doa'], ['🤝', 'jabat'], ['🙏', 'makasih tolong'], ['💪', 'otot'],
            ['🫶', 'hati tangan'], ['✍️', 'tulis'], ['💅', 'kuku'], ['🤳', 'selfie']
        ],
        heart: [
            ['❤️', 'cinta'], ['🧡', 'oranye'], ['💛', 'kuning'], ['💚', 'hijau'], ['💙', 'biru'],
            ['💜', 'ungu'], ['🖤', 'hitam'], ['🤍', 'putih'], ['🤎', 'coklat'], ['💔', 'patah'],
            ['❣️', 'seru'], ['💕', 'dua hati'], ['💞', 'putar'], ['💓', 'berdetak'], ['💗', 'tumbuh'],
            ['💖', 'kilat'], ['💘', 'panah'], ['💝', 'kado'], ['💟', 'dekor'], ['💋', 'ciuman'],
            ['💌', 'surat'], ['💯', 'seratus'], ['🔥', 'api'], ['✨', 'kilau'], ['⭐', 'bintang'],
            ['🌟', 'bersinar'], ['💫', 'pusing'], ['🎉', 'pesta'], ['🎊', 'konfeti']
        ],
        animal: [
            ['🐶', 'anjing'], ['🐱', 'kucing'], ['🐭', 'tikus'], ['🐹', 'hamster'], ['🐰', 'kelinci'],
            ['🦊', 'rubah'], ['🐻', 'beruang'], ['🐼', 'panda'], ['🐨', 'koala'], ['🐯', 'harimau'],
            ['🦁', 'singa'], ['🐮', 'sapi'], ['🐷', 'babi'], ['🐸', 'katak'], ['🐵', 'monyet'],
            ['🐔', 'ayam'], ['🐧', 'penguin'], ['🐦', 'burung'], ['🐤', 'anak ayam'], ['🦆', 'bebek'],
            ['🦉', 'burung hantu'], ['🐺', 'serigala'], ['🐗', 'babi hutan'], ['🐴', 'kuda'], ['🦄', 'unicorn'],
            ['🐝', 'lebah'], ['🦋', 'kupu'], ['🐛', 'ulat'], ['🐢', 'kura'], ['🐍', 'ular'],
            ['🐙', 'gurita'], ['🦑', 'cumi'], ['🦐', 'udang'], ['🦞', 'lobster'], ['🦀', 'kepiting'],
            ['🐠', 'ikan'], ['🐟', 'ikan'], ['🐬', 'lumba'], ['🐳', 'paus'], ['🦈', 'hiu']
        ],
        food: [
            ['🍏', 'apel hijau'], ['🍎', 'apel'], ['🍐', 'pir'], ['🍊', 'jeruk'], ['🍋', 'lemon'],
            ['🍌', 'pisang'], ['🍉', 'semangka'], ['🍇', 'anggur'], ['🍓', 'stroberi'], ['🫐', 'blueberry'],
            ['🍈', 'melon'], ['🍒', 'ceri'], ['🍑', 'persik'], ['🥭', 'mangga'], ['🍍', 'nanas'],
            ['🥥', 'kelapa'], ['🥝', 'kiwi'], ['🍅', 'tomat'], ['🥑', 'alpukat'], ['🥦', 'brokoli'],
            ['🍞', 'roti'], ['🥐', 'croissant'], ['🧀', 'keju'], ['🥚', 'telur'], ['🍳', 'orak arik'],
            ['🥓', 'bacon'], ['🥩', 'daging'], ['🍗', 'ayam'], ['🍔', 'burger'], ['🍟', 'kentang'],
            ['🍕', 'pizza'], ['🌭', 'hotdog'], ['🥪', 'sandwich'], ['🌮', 'taco'], ['🌯', 'burrito'],
            ['🍜', 'mi'], ['🍝', 'pasta'], ['🍣', 'sushi'], ['🍱', 'bento'], ['🍚', 'nasi'],
            ['🍦', 'es krim'], ['🍩', 'donat'], ['🍪', 'kue'], ['🎂', 'kue ulang tahun'], ['🍰', 'cake'],
            ['☕', 'kopi'], ['🍵', 'teh'], ['🧋', 'boba'], ['🍺', 'bir'], ['🍻', 'toast']
        ],
        activity: [
            ['⚽', 'sepak bola'], ['🏀', 'basket'], ['🏈', 'football'], ['⚾', 'baseball'], ['🎾', 'tenis'],
            ['🏐', 'voli'], ['🏉', 'rugby'], ['🎱', 'biliar'], ['🏓', 'pingpong'], ['🏸', 'bulutangkis'],
            ['🥊', 'tinju'], ['⛳', 'golf'], ['🎯', 'target'], ['🎮', 'game'], ['🕹️', 'joystick'],
            ['🎲', 'dadu'], ['🧩', 'puzzle'], ['🎸', 'gitar'], ['🎹', 'piano'], ['🥁', 'drum'],
            ['🎤', 'mikrofon'], ['🎧', 'headset'], ['🎬', 'film'], ['🎨', 'seni'], ['📷', 'kamera']
        ],
        travel: [
            ['🚗', 'mobil'], ['🚕', 'taksi'], ['🚌', 'bis'], ['🚎', 'trolley'], ['🏎️', 'balap'],
            ['🚓', 'polisi'], ['🚑', 'ambulans'], ['🚒', 'pemadam'], ['🚚', 'truk'], ['🚜', 'traktor'],
            ['🛵', 'motor'], ['🏍️', 'motor balap'], ['🚲', 'sepeda'], ['🛴', 'skuter'], ['🚁', 'helikopter'],
            ['✈️', 'pesawat'], ['🚀', 'roket'], ['🛸', 'ufo'], ['🚢', 'kapal'], ['⛵', 'layar'],
            ['🏠', 'rumah'], ['🏢', 'kantor'], ['🏫', 'sekolah'], ['🏥', 'rumah sakit'], ['🏦', 'bank'],
            ['⛪', 'gereja'], ['🕌', 'masjid'], ['🗼', 'menara'], ['🗽', 'liberty'], ['🗿', 'moai']
        ],
        object: [
            ['⌚', 'jam'], ['📱', 'hp'], ['💻', 'laptop'], ['⌨️', 'keyboard'], ['🖥️', 'monitor'],
            ['🖨️', 'printer'], ['🖱️', 'mouse'], ['📷', 'kamera'], ['📹', 'video'], ['💡', 'lampu'],
            ['🔦', 'senter'], ['📔', 'buku'], ['📚', 'buku'], ['📝', 'catatan'], ['📁', 'folder'],
            ['📅', 'kalender'], ['📌', 'pin'], ['📎', 'klip'], ['🔑', 'kunci'], ['🔒', 'gembok'],
            ['🔨', 'palu'], ['🧰', 'toolkit'], ['🧲', 'magnet'], ['💰', 'uang'], ['💳', 'kartu'],
            ['📧', 'email'], ['📩', 'pesan'], ['🔔', 'lonceng'], ['🔇', 'senyap'], ['🔋', 'baterai']
        ],
        symbol: [
            ['❤️', 'cinta'], ['✅', 'centang'], ['❌', 'salah'], ['❗', 'penting'], ['❓', 'tanya'],
            ['‼️', 'seru'], ['⚠️', 'peringatan'], ['🚫', 'larangan'], ['♻️', 'daur'], ['✔️', 'ok'],
            ['➕', 'tambah'], ['➖', 'kurang'], ['➗', 'bagi'], ['✖️', 'kali'], ['♾️', 'tak hingga'],
            ['©️', 'copyright'], ['™️', 'tm'], ['🔴', 'merah'], ['🟠', 'oranye'], ['🟡', 'kuning'],
            ['🟢', 'hijau'], ['🔵', 'biru'], ['🟣', 'ungu'], ['⚫', 'hitam'], ['⚪', 'putih'],
            ['🆗', 'ok'], ['🆕', 'baru'], ['🆙', 'up'], ['🆒', 'cool'], ['🆓', 'gratis']
        ]
    };

    var audioCtx = null;
    var state = {
        open: false,
        peer: 'team',
        peerLabel: 'Chat Tim',
        lastId: '',
        messages: [],
        seen: {},
        heartbeatTimer: null,
        pollTimer: null,
        sending: false,
        pendingFile: null,
        lastUnread: 0,
        unreadReady: false,
        emojiOpen: false,
        emojiCat: 'smile',
        peerReadAt: '',
        peerOnline: false,
        replyTo: null,
        groups: [],
        conversations: [],
        directory: null,
        recordingVoice: false,
        pinnedMessageId: '',
        pinnedMessage: null,
        profileOpen: false,
        profileUserId: '',
        searchOpen: false,
        searchQuery: ''
    };

    var voiceRecorder = null;
    var voiceStream = null;
    var voiceChunks = [];
    var voiceTickTimer = null;
    var voiceStartedMs = 0;
    var voiceMime = 'audio/webm';

    function loadRecentEmoji() {
        try {
            var raw = localStorage.getItem(EMOJI_RECENT_KEY);
            var parsed = raw ? JSON.parse(raw) : [];
            return Array.isArray(parsed) ? parsed : [];
        } catch (e) {
            return [];
        }
    }

    function saveRecentEmoji(emoji) {
        var list = loadRecentEmoji().filter(function (item) { return item !== emoji; });
        list.unshift(emoji);
        list = list.slice(0, 24);
        try { localStorage.setItem(EMOJI_RECENT_KEY, JSON.stringify(list)); } catch (e) {}
    }

    function emojiMatches(item, query) {
        if (!query) return true;
        var q = query.toLowerCase();
        return item[0].indexOf(q) !== -1 || String(item[1] || '').toLowerCase().indexOf(q) !== -1;
    }

    function allEmojiPairs() {
        var out = [];
        Object.keys(EMOJI_DATA).forEach(function (key) {
            out = out.concat(EMOJI_DATA[key]);
        });
        return out;
    }

    function renderEmojiGrid(catId, query) {
        if (!emojiGridEl) return;
        while (emojiGridEl.firstChild) emojiGridEl.removeChild(emojiGridEl.firstChild);
        var items = [];
        if (query) {
            items = allEmojiPairs().filter(function (item) { return emojiMatches(item, query); });
        } else if (catId === 'recent') {
            items = loadRecentEmoji().map(function (ch) { return [ch, '']; });
        } else {
            items = EMOJI_DATA[catId] || [];
        }
        if (!items.length) {
            var empty = document.createElement('div');
            empty.className = 'hris-live-chat-emoji-empty';
            empty.textContent = catId === 'recent' ? 'Belum ada emoji terbaru.' : 'Tidak ada emoji.';
            emojiGridEl.appendChild(empty);
            return;
        }
        items.forEach(function (item) {
            var ch = item[0];
            var btn = document.createElement('button');
            btn.type = 'button';
            btn.textContent = ch;
            btn.title = item[1] || ch;
            btn.addEventListener('click', function (e) {
                e.preventDefault();
                insertEmoji(ch);
            });
            emojiGridEl.appendChild(btn);
        });
    }

    function renderEmojiCats(activeId) {
        if (!emojiCatsEl) return;
        while (emojiCatsEl.firstChild) emojiCatsEl.removeChild(emojiCatsEl.firstChild);
        EMOJI_CATS.forEach(function (cat) {
            var btn = document.createElement('button');
            btn.type = 'button';
            btn.textContent = cat.icon;
            btn.title = cat.label;
            if (cat.id === activeId) btn.className = 'is-on';
            btn.addEventListener('click', function () {
                state.emojiCat = cat.id;
                if (emojiSearchEl) emojiSearchEl.value = '';
                renderEmojiCats(cat.id);
                renderEmojiGrid(cat.id, '');
            });
            emojiCatsEl.appendChild(btn);
        });
    }

    function insertEmoji(emoji) {
        if (!inputEl) return;
        var start = inputEl.selectionStart != null ? inputEl.selectionStart : inputEl.value.length;
        var end = inputEl.selectionEnd != null ? inputEl.selectionEnd : inputEl.value.length;
        var val = inputEl.value || '';
        if (val.length + emoji.length > 2000) return;
        inputEl.value = val.slice(0, start) + emoji + val.slice(end);
        var pos = start + emoji.length;
        inputEl.focus();
        try { inputEl.setSelectionRange(pos, pos); } catch (e) {}
        saveRecentEmoji(emoji);
    }

    function setEmojiOpen(open) {
        state.emojiOpen = !!open;
        if (emojiPanel) {
            if (state.emojiOpen) emojiPanel.classList.add('is-open');
            else emojiPanel.classList.remove('is-open');
        }
        if (emojiBtn) emojiBtn.classList.toggle('is-on', state.emojiOpen);
        if (emojiBtn) emojiBtn.setAttribute('aria-expanded', state.emojiOpen ? 'true' : 'false');
        if (state.emojiOpen) {
            renderEmojiCats(state.emojiCat || 'smile');
            renderEmojiGrid(state.emojiCat || 'smile', emojiSearchEl ? emojiSearchEl.value : '');
        }
    }

    function csrfToken() {
        var el = root.querySelector('input[name=csrfmiddlewaretoken]');
        if (el && el.value) return el.value;
        var match = document.cookie.match(/(?:^|; )csrftoken=([^;]+)/);
        return match ? decodeURIComponent(match[1]) : '';
    }

    function escapeText(str) {
        return String(str || '');
    }

    function setBadge(count) {
        var n = parseInt(count, 10) || 0;
        var text = n > 99 ? '99+' : String(n);
        if (fabBadge) {
            fabBadge.textContent = text;
            if (n > 0) fabBadge.classList.add('is-on');
            else fabBadge.classList.remove('is-on');
        }
        if (navBadge) {
            if (n > 0) {
                navBadge.style.display = '';
                navBadge.textContent = text;
            } else {
                navBadge.style.display = 'none';
            }
        }
        if (state.unreadReady && n > (state.lastUnread || 0) && (!state.open || document.hidden)) {
            playNotifySound();
        }
        state.lastUnread = n;
        state.unreadReady = true;
    }

    function unlockAudio() {
        try {
            var Ctx = window.AudioContext || window.webkitAudioContext;
            if (!Ctx) return;
            if (!audioCtx) audioCtx = new Ctx();
            if (audioCtx.state === 'suspended') audioCtx.resume();
        } catch (e) {}
    }

    function playNotifySound() {
        unlockAudio();
        if (!audioCtx) return;
        var now = audioCtx.currentTime;
        function tone(freq, start, dur, gain) {
            var osc = audioCtx.createOscillator();
            var g = audioCtx.createGain();
            osc.type = 'sine';
            osc.frequency.setValueAtTime(freq, now + start);
            g.gain.setValueAtTime(0.0001, now + start);
            g.gain.exponentialRampToValueAtTime(gain, now + start + 0.02);
            g.gain.exponentialRampToValueAtTime(0.0001, now + start + dur);
            osc.connect(g);
            g.connect(audioCtx.destination);
            osc.start(now + start);
            osc.stop(now + start + dur + 0.05);
        }
        tone(880, 0, 0.11, 0.07);
        tone(1175, 0.11, 0.16, 0.06);
    }

    function formatFileSize(bytes) {
        var n = parseInt(bytes, 10) || 0;
        if (n < 1024) return n + ' B';
        if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
        return (n / (1024 * 1024)).toFixed(1) + ' MB';
    }

    function setPendingFile(file) {
        state.pendingFile = file || null;
        if (!fileChip) return;
        if (file) {
            fileChip.classList.add('is-on');
            if (fileNameEl) fileNameEl.textContent = file.name + ' (' + formatFileSize(file.size) + ')';
        } else {
            fileChip.classList.remove('is-on');
            if (fileInput) fileInput.value = '';
        }
    }

    function formatVoiceDuration(ms) {
        var sec = Math.max(0, Math.floor((ms || 0) / 1000));
        var m = Math.floor(sec / 60);
        var s = sec % 60;
        return m + ':' + String(s).padStart(2, '0');
    }

    function setVoiceUi(recording) {
        if (voiceBar) voiceBar.hidden = !recording;
        if (composeBar) composeBar.hidden = !!recording;
        if (voiceBtn) voiceBtn.classList.toggle('is-recording', !!recording);
    }

    function cleanupVoiceStream() {
        if (!voiceStream) return;
        voiceStream.getTracks().forEach(function (track) {
            try { track.stop(); } catch (e) {}
        });
        voiceStream = null;
    }

    function stopVoiceTimer() {
        if (!voiceTickTimer) return;
        clearInterval(voiceTickTimer);
        voiceTickTimer = null;
    }

    function cancelVoiceRecording() {
        state.recordingVoice = false;
        if (voiceRecorder && voiceRecorder.state !== 'inactive') {
            voiceRecorder._sendAfterStop = false;
            try { voiceRecorder.stop(); } catch (e) {}
        } else {
            cleanupVoiceStream();
            stopVoiceTimer();
            setVoiceUi(false);
        }
        voiceChunks = [];
        voiceRecorder = null;
    }

    function stopVoiceRecording(sendAfter) {
        if (!voiceRecorder || voiceRecorder.state === 'inactive') return;
        state.recordingVoice = false;
        voiceRecorder._sendAfterStop = !!sendAfter;
        try { voiceRecorder.stop(); } catch (e) {}
    }

    function startVoiceRecording() {
        if (state.recordingVoice || state.sending) return;
        if (!window.MediaRecorder || !navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
            window.alert('Peramban tidak mendukung rekaman suara. Pastikan situs HTTPS dan izin mikrofon diaktifkan.');
            return;
        }
        unlockAudio();
        navigator.mediaDevices.getUserMedia({ audio: true }).then(function (stream) {
            voiceStream = stream;
            voiceChunks = [];
            voiceMime = 'audio/webm';
            if (MediaRecorder.isTypeSupported('audio/webm;codecs=opus')) {
                voiceMime = 'audio/webm;codecs=opus';
            } else if (MediaRecorder.isTypeSupported('audio/ogg;codecs=opus')) {
                voiceMime = 'audio/ogg;codecs=opus';
            }
            voiceRecorder = new MediaRecorder(stream, { mimeType: voiceMime });
            voiceRecorder._sendAfterStop = false;
            voiceRecorder.ondataavailable = function (ev) {
                if (ev.data && ev.data.size) voiceChunks.push(ev.data);
            };
            voiceRecorder.onstop = function () {
                var shouldSend = !!(voiceRecorder && voiceRecorder._sendAfterStop);
                cleanupVoiceStream();
                stopVoiceTimer();
                setVoiceUi(false);
                if (shouldSend && voiceChunks.length) {
                    var baseMime = String(voiceMime || 'audio/webm').split(';')[0];
                    var ext = baseMime.indexOf('ogg') >= 0 ? '.ogg' : '.webm';
                    var blob = new Blob(voiceChunks, { type: baseMime });
                    if (blob.size < 800) {
                        window.alert('Rekaman terlalu pendek. Coba lagi.');
                    } else {
                        var file = new File([blob], 'voice-' + Date.now() + ext, { type: baseMime });
                        setPendingFile(file);
                        sendMessage();
                    }
                }
                voiceChunks = [];
                voiceRecorder = null;
                state.recordingVoice = false;
            };
            voiceRecorder.start(250);
            state.recordingVoice = true;
            voiceStartedMs = Date.now();
            setVoiceUi(true);
            if (voiceTimerEl) voiceTimerEl.textContent = '0:00';
            stopVoiceTimer();
            voiceTickTimer = setInterval(function () {
                if (voiceTimerEl) voiceTimerEl.textContent = formatVoiceDuration(Date.now() - voiceStartedMs);
            }, 400);
        }).catch(function () {
            cleanupVoiceStream();
            window.alert('Tidak bisa mengakses mikrofon. Izinkan akses mikrofon di pengaturan browser.');
        });
    }

    function syncInputHeight() {
        if (!inputEl) return;
        inputEl.style.height = 'auto';
        var next = Math.min(120, Math.max(40, inputEl.scrollHeight));
        inputEl.style.height = next + 'px';
    }

    function formatTime(value) {
        if (!value) return '';
        var s = String(value).trim().replace(' ', 'T');
        var dt = new Date(s);
        if (isNaN(dt.getTime())) return String(value).slice(11, 16);
        var hh = String(dt.getHours()).padStart(2, '0');
        var mm = String(dt.getMinutes()).padStart(2, '0');
        return hh + ':' + mm;
    }

    function parseChatDate(value) {
        if (!value) return null;
        var text = String(value).trim().replace(' ', 'T');
        var dt = new Date(text);
        return isNaN(dt.getTime()) ? null : dt;
    }

    function formatLastOnline(lastSeen, online) {
        if (online) return 'Online sekarang';
        var dt = parseChatDate(lastSeen);
        if (!dt) return 'Terakhir online tidak diketahui';
        var now = new Date();
        var pad = function (n) { return String(n).padStart(2, '0'); };
        var time = pad(dt.getHours()) + ':' + pad(dt.getMinutes());
        var startToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
        var startThat = new Date(dt.getFullYear(), dt.getMonth(), dt.getDate()).getTime();
        var dayDiff = Math.round((startToday - startThat) / 86400000);
        if (dayDiff === 0) return 'Terakhir online hari ini ' + time;
        if (dayDiff === 1) return 'Terakhir online kemarin ' + time;
        var months = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des'];
        return 'Terakhir online ' + pad(dt.getDate()) + ' ' + months[dt.getMonth()] + ' ' + dt.getFullYear() + ', ' + time;
    }

    function conversationById(userId) {
        var convs = state.conversations || [];
        for (var i = 0; i < convs.length; i++) {
            if (convs[i].user_id === userId) return convs[i];
        }
        return null;
    }

    function previewText(text) {
        var s = String(text || '').replace(/\s+/g, ' ').trim();
        if (s.length > 28) return s.slice(0, 28) + '…';
        return s;
    }

    function requestJson(url, options) {
        options = options || {};
        var headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/json'
        };
        var token = csrfToken();
        if (token) headers['X-CSRFToken'] = token;
        if (options.json) {
            headers['Content-Type'] = 'application/json';
        }
        return fetch(url, {
            method: options.method || 'GET',
            credentials: 'same-origin',
            headers: headers,
            body: options.body || null
        }).then(function (res) {
            return res.json().catch(function () {
                return { status: false, error: 'Gagal memuat chat' };
            });
        });
    }

    function isGroupPeer(peer) {
        return String(peer || '').indexOf('group:') === 0;
    }

    function isDirectPeer(peer) {
        var p = String(peer || '');
        return p && p !== 'team' && !isGroupPeer(p);
    }

    function resolveFoto(path) {
        var p = String(path || '').trim();
        if (!p) return defaultFoto;
        if (p.indexOf('http://') === 0 || p.indexOf('https://') === 0 || p.indexOf('/') === 0) return p;
        return '/' + p.replace(/^\/+/, '');
    }

    function messagePlainText(msg) {
        if (!msg) return '';
        if (msg.body) return String(msg.body);
        if (msg.file_name) return String(msg.file_name);
        return '';
    }

    function copyToClipboard(text) {
        var t = String(text || '');
        if (!t) return Promise.reject(new Error('empty'));
        if (navigator.clipboard && navigator.clipboard.writeText) {
            return navigator.clipboard.writeText(t);
        }
        return new Promise(function (resolve, reject) {
            try {
                var ta = document.createElement('textarea');
                ta.value = t;
                ta.setAttribute('readonly', '');
                ta.style.position = 'fixed';
                ta.style.left = '-9999px';
                document.body.appendChild(ta);
                ta.select();
                var ok = document.execCommand('copy');
                document.body.removeChild(ta);
                if (ok) resolve();
                else reject(new Error('copy'));
            } catch (e) {
                reject(e);
            }
        });
    }

    function applyPeerPrefs(json) {
        if (!json) return;
        if (json.pinned_message_id !== undefined) {
            state.pinnedMessageId = json.pinned_message_id || '';
        }
        if (json.pinned_message !== undefined) {
            state.pinnedMessage = json.pinned_message || null;
        }
        renderPinnedBar();
        refreshPinnedMarkers();
        if (headerPinBtn) {
            headerPinBtn.classList.toggle('is-active', !!state.pinnedMessageId);
        }
    }

    function pinnedPreviewText(msg) {
        if (!msg) return '';
        if (msg.body) return previewText(msg.body);
        if (msg.file_name) return 'Lampiran: ' + msg.file_name;
        return 'Pesan';
    }

    function renderPinnedBar() {
        if (!pinnedEl) return;
        while (pinnedEl.firstChild) pinnedEl.removeChild(pinnedEl.firstChild);
        var msg = state.pinnedMessage;
        if (!msg || !state.pinnedMessageId) {
            pinnedEl.hidden = true;
            return;
        }
        pinnedEl.hidden = false;
        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'hris-live-chat-pinned-inner';
        btn.innerHTML = '<i class="bi bi-pin-angle-fill"></i>';
        var text = document.createElement('span');
        text.className = 'hris-live-chat-pinned-text';
        text.textContent = pinnedPreviewText(msg);
        btn.appendChild(text);
        var unpin = document.createElement('span');
        unpin.className = 'hris-live-chat-pinned-unpin';
        unpin.title = 'Lepas pin';
        unpin.innerHTML = '&times;';
        unpin.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            setPinnedMessage(null);
        });
        btn.appendChild(unpin);
        btn.addEventListener('click', function () {
            scrollToMessage(state.pinnedMessageId);
        });
        pinnedEl.appendChild(btn);
    }

    function refreshPinnedMarkers() {
        if (!messagesEl) return;
        var pid = state.pinnedMessageId || '';
        var nodes = messagesEl.querySelectorAll('.hris-live-chat-bubble[data-message-id]');
        for (var i = 0; i < nodes.length; i++) {
            var mid = nodes[i].getAttribute('data-message-id') || '';
            nodes[i].classList.toggle('is-pinned-msg', !!pid && mid === pid);
        }
    }

    function setPinnedMessage(messageId) {
        if (!urls.pin) return;
        requestJson(urls.pin, {
            method: 'POST',
            json: true,
            body: JSON.stringify({ peer: state.peer, message_id: messageId || '' })
        }).then(function (json) {
            if (!json || !json.status) {
                window.alert((json && json.error) || 'Gagal menyimpan pin');
                return;
            }
            applyPeerPrefs(json);
        }).catch(function () {});
    }

    function deleteMessage(msg) {
        if (!msg || !msg.message_id || !urls.deleteMsg) return;
        if (!msg.mine) return;
        if (!window.confirm('Hapus pesan ini?')) return;
        requestJson(urls.deleteMsg, {
            method: 'POST',
            json: true,
            body: JSON.stringify({ message_id: msg.message_id })
        }).then(function (json) {
            if (!json || !json.status) {
                window.alert((json && json.error) || 'Gagal menghapus pesan');
                return;
            }
            var mid = msg.message_id;
            if (state.pinnedMessageId === mid) {
                state.pinnedMessageId = '';
                state.pinnedMessage = null;
                renderPinnedBar();
            }
            state.messages = (state.messages || []).filter(function (m) {
                return m.message_id !== mid;
            });
            delete state.seen[mid];
            if (messagesEl) {
                var node = messagesEl.querySelector('[data-message-id="' + mid + '"]');
                if (node) node.remove();
                if (!messagesEl.querySelector('.hris-live-chat-bubble')) {
                    var empty = document.createElement('div');
                    empty.className = 'hris-live-chat-empty';
                    empty.textContent = state.searchQuery
                        ? 'Tidak ada pesan yang cocok.'
                        : 'Belum ada pesan. Kirim yang pertama.';
                    messagesEl.appendChild(empty);
                }
            }
        }).catch(function () {});
    }

    function clearConversation() {
        if (!urls.clear) return;
        if (!window.confirm('Hapus riwayat chat ini dari tampilan Anda? Pesan lawan tidak terhapus.')) return;
        requestJson(urls.clear, {
            method: 'POST',
            json: true,
            body: JSON.stringify({ peer: state.peer })
        }).then(function (json) {
            if (!json || !json.status) {
                window.alert((json && json.error) || 'Gagal menghapus riwayat');
                return;
            }
            state.pinnedMessageId = '';
            state.pinnedMessage = null;
            renderPinnedBar();
            closeProfilePane();
            setSearchOpen(false);
            loadMessages(true);
        }).catch(function () {});
    }

    function formatProfileActivity(profile) {
        if (!profile) return '';
        if (profile.online) return 'Online sekarang';
        var t = formatLastOnline(profile.last_seen, false);
        if (t.indexOf('Terakhir online ') === 0) {
            return 'Aktif ' + t.slice('Terakhir online '.length);
        }
        return t;
    }

    function closeProfilePane() {
        state.profileOpen = false;
        state.profileUserId = '';
        if (profilePaneEl) {
            profilePaneEl.hidden = true;
            while (profilePaneEl.firstChild) profilePaneEl.removeChild(profilePaneEl.firstChild);
        }
        if (threadEl) threadEl.classList.remove('is-profile-open');
        if (panel) panel.classList.remove('has-chat-profile');
    }

    function renderProfilePane(profile) {
        if (!profilePaneEl || !profile) return;
        while (profilePaneEl.firstChild) profilePaneEl.removeChild(profilePaneEl.firstChild);
        profilePaneEl.hidden = false;

        var closeBtn = document.createElement('button');
        closeBtn.type = 'button';
        closeBtn.className = 'hris-live-chat-profile-close';
        closeBtn.setAttribute('aria-label', 'Tutup profil');
        closeBtn.innerHTML = '&times;';
        closeBtn.addEventListener('click', closeProfilePane);
        profilePaneEl.appendChild(closeBtn);

        var hero = document.createElement('div');
        hero.className = 'hris-live-chat-profile-hero';
        var img = document.createElement('img');
        img.className = 'hris-live-chat-profile-avatar';
        img.alt = profile.user_alias || 'User';
        img.src = resolveFoto(profile.user_foto);
        hero.appendChild(img);
        var name = document.createElement('div');
        name.className = 'hris-live-chat-profile-name';
        name.textContent = profile.user_alias || profile.user_name || 'User';
        hero.appendChild(name);
        if (profile.department) {
            var dept = document.createElement('div');
            dept.className = 'hris-live-chat-profile-dept';
            dept.textContent = profile.department;
            hero.appendChild(dept);
        }
        var act = document.createElement('div');
        act.className = 'hris-live-chat-profile-activity';
        act.textContent = formatProfileActivity(profile);
        hero.appendChild(act);
        profilePaneEl.appendChild(hero);

        function sectionTitle(label) {
            var h = document.createElement('div');
            h.className = 'hris-live-chat-profile-section';
            h.textContent = label;
            return h;
        }

        function contactCard(iconClass, label, value) {
            var card = document.createElement('div');
            card.className = 'hris-live-chat-profile-card';
            var ic = document.createElement('span');
            ic.className = 'hris-live-chat-profile-card-icon';
            ic.innerHTML = '<i class="' + iconClass + '"></i>';
            card.appendChild(ic);
            var meta = document.createElement('span');
            meta.className = 'hris-live-chat-profile-card-meta';
            var lab = document.createElement('small');
            lab.textContent = label;
            meta.appendChild(lab);
            var val = document.createElement('strong');
            val.textContent = value || '-';
            meta.appendChild(val);
            card.appendChild(meta);
            return card;
        }

        profilePaneEl.appendChild(sectionTitle('Kontak'));
        profilePaneEl.appendChild(contactCard('bi bi-envelope-fill', 'Email', profile.user_mail || '-'));
        profilePaneEl.appendChild(contactCard('bi bi-telephone-fill', 'Nomor telepon', profile.user_telp || '-'));
    }

    function openProfilePane() {
        if (!isDirectPeer(state.peer) || !urls.profile) return;
        if (state.profileOpen && state.profileUserId === state.peer) {
            closeProfilePane();
            return;
        }
        state.profileOpen = true;
        state.profileUserId = state.peer;
        if (threadEl) threadEl.classList.add('is-profile-open');
        if (panel) panel.classList.add('has-chat-profile');
        if (profilePaneEl) {
            profilePaneEl.hidden = false;
            profilePaneEl.innerHTML = '<div class="hris-live-chat-empty" style="font-size:0.78rem">Memuat profil...</div>';
        }
        requestJson(urls.profile + '?user_id=' + encodeURIComponent(state.peer)).then(function (json) {
            if (!state.profileOpen || state.profileUserId !== state.peer) return;
            if (!json || !json.status || !json.profile) {
                closeProfilePane();
                window.alert((json && json.error) || 'Gagal memuat profil');
                return;
            }
            renderProfilePane(json.profile);
        }).catch(function () {
            closeProfilePane();
        });
    }

    function setSearchOpen(open) {
        state.searchOpen = !!open;
        if (searchBarEl) searchBarEl.hidden = !state.searchOpen;
        if (headerSearchBtn) headerSearchBtn.classList.toggle('is-active', state.searchOpen);
        if (!state.searchOpen) {
            state.searchQuery = '';
            if (searchInputEl) searchInputEl.value = '';
            loadMessages(true);
        } else if (searchInputEl) {
            searchInputEl.focus();
        }
    }

    function runMessageSearch() {
        var q = searchInputEl ? String(searchInputEl.value || '').trim() : '';
        state.searchQuery = q;
        if (q.length < 2) {
            if (state.searchOpen) loadMessages(true);
            return;
        }
        if (!urls.messages) return;
        var url = urls.messages + '?peer=' + encodeURIComponent(state.peer) + '&q=' + encodeURIComponent(q);
        requestJson(url).then(function (json) {
            if (!json || !json.status) return;
            applyReceiptPayload(json);
            applyPeerPrefs(json);
            var rows = json.messages || [];
            state.messages = rows;
            state.lastId = '';
            state.seen = {};
            renderMessages(rows, true);
        }).catch(function () {});
    }

    function currentGroup() {
        var groups = state.groups || [];
        for (var i = 0; i < groups.length; i++) {
            if (groups[i].peer === state.peer) return groups[i];
        }
        return null;
    }

    function updateThreadHeader() {
        if (threadNameEl) threadNameEl.textContent = state.peerLabel || 'Percakapan';
        else if (threadTitle) threadTitle.textContent = state.peerLabel || 'Percakapan';
        var groupMode = isGroupPeer(state.peer);
        var directMode = isDirectPeer(state.peer);
        if (threadTitle) {
            if (groupMode || directMode) threadTitle.classList.add('is-clickable');
            else threadTitle.classList.remove('is-clickable');
        }
        if (directMode && state.profileOpen && state.profileUserId !== state.peer) {
            closeProfilePane();
        }
        if (!threadSubEl) return;
        if (groupMode) {
            var grp = currentGroup();
            var n = grp ? (parseInt(grp.member_count, 10) || 0) : 0;
            threadSubEl.textContent = n
                ? (n + ' anggota · ketuk untuk lihat')
                : 'Ketuk untuk lihat anggota';
            return;
        }
        if (directMode) {
            var conv = conversationById(state.peer);
            var statusLine = conv
                ? formatLastOnline(conv.last_seen, !!conv.online)
                : '';
            threadSubEl.textContent = statusLine
                ? (statusLine + ' · ketuk untuk profil')
                : 'Ketuk untuk profil';
            return;
        }
        threadSubEl.textContent = '';
    }

    function memberSubtitle(member) {
        var bits = [];
        if (member.is_me) bits.push('Anda');
        if (member.is_creator) bits.push('Pembuat grup');
        if (member.online) bits.push('Online');
        else if (!member.is_me) bits.push('Offline');
        return bits.join(' · ');
    }

    function openMembersSheet() {
        if (!isGroupPeer(state.peer) || !urls.members) return;
        openSheet(state.peerLabel || 'Anggota grup');
        var loading = document.createElement('div');
        loading.className = 'hris-live-chat-empty';
        loading.style.fontSize = '0.78rem';
        loading.textContent = 'Memuat anggota...';
        sheetBodyEl.appendChild(loading);
        requestJson(urls.members + '?peer=' + encodeURIComponent(state.peer)).then(function (json) {
            if (!sheetEl || !sheetEl.classList.contains('is-open')) return;
            while (sheetBodyEl.firstChild) sheetBodyEl.removeChild(sheetBodyEl.firstChild);
            if (!json || !json.status) {
                var err = document.createElement('div');
                err.className = 'hris-live-chat-empty';
                err.textContent = (json && json.error) || 'Gagal memuat anggota';
                sheetBodyEl.appendChild(err);
                return;
            }
            if (sheetTitleEl) {
                sheetTitleEl.textContent = (json.group_name || state.peerLabel || 'Grup') +
                    ' · ' + (json.member_count || 0) + ' anggota';
            }
            var groups = state.groups || [];
            for (var i = 0; i < groups.length; i++) {
                if (groups[i].peer === state.peer) {
                    groups[i].member_count = json.member_count || groups[i].member_count;
                }
            }
            updateThreadHeader();
            var members = json.members || [];
            if (!members.length) {
                var empty = document.createElement('div');
                empty.className = 'hris-live-chat-empty';
                empty.textContent = 'Belum ada anggota.';
                sheetBodyEl.appendChild(empty);
                return;
            }
            members.forEach(function (member) {
                var wrap = document.createElement('div');
                wrap.className = 'hris-live-chat-member';
                var dot = document.createElement('span');
                dot.className = 'hris-live-chat-dot' + (member.online ? ' is-online' : '');
                wrap.appendChild(dot);
                var meta = document.createElement('span');
                meta.style.minWidth = '0';
                meta.style.flex = '1';
                var name = document.createElement('div');
                name.className = 'hris-live-chat-pick-name';
                name.textContent = member.user_alias || member.user_name || 'User';
                meta.appendChild(name);
                var sub = document.createElement('div');
                sub.className = 'hris-live-chat-pick-sub';
                sub.textContent = memberSubtitle(member);
                meta.appendChild(sub);
                wrap.appendChild(meta);
                if (!member.is_me) {
                    wrap.classList.add('is-clickable');
                    wrap.title = 'Buka chat pribadi';
                    wrap.addEventListener('click', function () {
                        closeSheet();
                        selectPeer(member.user_id, member.user_alias || member.user_name || 'User', true);
                    });
                }
                sheetBodyEl.appendChild(wrap);
            });
        }).catch(function () {
            if (!sheetBodyEl) return;
            while (sheetBodyEl.firstChild) sheetBodyEl.removeChild(sheetBodyEl.firstChild);
            var err = document.createElement('div');
            err.className = 'hris-live-chat-empty';
            err.textContent = 'Gagal memuat anggota';
            sheetBodyEl.appendChild(err);
        });
    }

    function setOpen(open) {
        state.open = !!open;
        if (!panel) return;
        if (state.open) {
            panel.classList.add('is-open');
            markRead();
            loadMessages(true);
            if (inputEl) inputEl.focus();
        } else {
            panel.classList.remove('is-open');
            panel.classList.remove('is-thread');
            setEmojiOpen(false);
            closeSheet();
            clearReply();
        }
        scheduleTimers();
    }

    function selectPeer(peer, label, fromList) {
        state.peer = peer || 'team';
        state.peerLabel = label || (state.peer === 'team' ? 'Chat Tim' : 'Percakapan');
        state.lastId = '';
        state.messages = [];
        state.seen = {};
        state.pinnedMessageId = '';
        state.pinnedMessage = null;
        closeProfilePane();
        setSearchOpen(false);
        updateThreadHeader();
        if (panel && fromList) panel.classList.add('is-thread');
        state.peerReadAt = '';
        state.peerOnline = false;
        clearReply();
        renderListActive();
        renderPinnedBar();
        markRead();
        if (state.open) loadMessages(true);
    }

    function renderListActive() {
        if (!listEl) return;
        var items = listEl.querySelectorAll('[data-peer]');
        for (var i = 0; i < items.length; i++) {
            if (items[i].getAttribute('data-peer') === state.peer) {
                items[i].classList.add('is-active');
            } else {
                items[i].classList.remove('is-active');
            }
        }
    }

    function makeSection(title) {
        var el = document.createElement('div');
        el.className = 'hris-live-chat-section';
        el.textContent = title;
        return el;
    }

    function replyPreview(msg) {
        if (!msg) return '';
        if (msg.body) return previewText(msg.body);
        if (msg.file_name) return 'Lampiran: ' + msg.file_name;
        if (msg.reply_file_name) return 'Lampiran: ' + msg.reply_file_name;
        return 'Pesan';
    }

    function clearReply() {
        state.replyTo = null;
        if (replyBar) replyBar.classList.remove('is-on');
        if (replyNameEl) replyNameEl.textContent = '';
        if (replyPreviewEl) replyPreviewEl.textContent = '';
        if (inputEl) inputEl.placeholder = 'Tulis pesan...';
    }

    function setReply(msg) {
        if (!msg || !msg.message_id) return;
        state.replyTo = msg;
        if (replyBar) replyBar.classList.add('is-on');
        if (replyNameEl) replyNameEl.textContent = 'Membalas ' + (msg.from_alias || (msg.mine ? 'diri sendiri' : 'User'));
        if (replyPreviewEl) replyPreviewEl.textContent = replyPreview(msg);
        if (inputEl) {
            inputEl.placeholder = 'Tulis balasan...';
            inputEl.focus();
        }
    }

    function scrollToMessage(messageId) {
        if (!messagesEl || !messageId) return;
        var node = messagesEl.querySelector('[data-message-id="' + messageId + '"]');
        if (!node) return;
        node.scrollIntoView({ behavior: 'smooth', block: 'center' });
        node.classList.add('is-actions');
        setTimeout(function () { node.classList.remove('is-actions'); }, 1400);
    }

    function closeSheet() {
        if (sheetEl) sheetEl.classList.remove('is-open');
        if (sheetBodyEl) while (sheetBodyEl.firstChild) sheetBodyEl.removeChild(sheetBodyEl.firstChild);
        if (sheetFootEl) while (sheetFootEl.firstChild) sheetFootEl.removeChild(sheetFootEl.firstChild);
    }

    function openSheet(title) {
        if (!sheetEl) return;
        if (sheetTitleEl) sheetTitleEl.textContent = title || '';
        if (sheetBodyEl) while (sheetBodyEl.firstChild) sheetBodyEl.removeChild(sheetBodyEl.firstChild);
        if (sheetFootEl) while (sheetFootEl.firstChild) sheetFootEl.removeChild(sheetFootEl.firstChild);
        sheetEl.classList.add('is-open');
        setEmojiOpen(false);
    }

    function loadDirectory(cb) {
        if (state.directory && state.directory.users) {
            cb(state.directory);
            return;
        }
        if (!urls.directory) {
            cb({ users: [], groups: state.groups || [] });
            return;
        }
        requestJson(urls.directory).then(function (json) {
            if (json && json.status) {
                state.directory = json;
                cb(json);
                return;
            }
            cb({ users: [], groups: state.groups || [] });
        }).catch(function () {
            cb({ users: [], groups: state.groups || [] });
        });
    }

    function filterUsers(users, query) {
        var q = String(query || '').toLowerCase().trim();
        if (!q) return users || [];
        return (users || []).filter(function (u) {
            return String(u.user_alias || '').toLowerCase().indexOf(q) !== -1
                || String(u.user_name || '').toLowerCase().indexOf(q) !== -1
                || String(u.user_mail || '').toLowerCase().indexOf(q) !== -1;
        });
    }

    function openGroupSheet() {
        loadDirectory(function (dir) {
            openSheet('Grup baru');
            var nameInput = document.createElement('input');
            nameInput.type = 'text';
            nameInput.className = 'hris-live-chat-field';
            nameInput.placeholder = 'Nama grup';
            nameInput.maxLength = 80;
            sheetBodyEl.appendChild(nameInput);

            var search = document.createElement('input');
            search.type = 'search';
            search.className = 'hris-live-chat-field';
            search.placeholder = 'Cari anggota...';
            sheetBodyEl.appendChild(search);

            var list = document.createElement('div');
            sheetBodyEl.appendChild(list);

            var selected = {};
            function renderPicks() {
                while (list.firstChild) list.removeChild(list.firstChild);
                var rows = filterUsers(dir.users || [], search.value);
                if (!rows.length) {
                    var empty = document.createElement('div');
                    empty.className = 'hris-live-chat-empty';
                    empty.style.fontSize = '0.75rem';
                    empty.textContent = 'Tidak ada user.';
                    list.appendChild(empty);
                    return;
                }
                rows.forEach(function (user) {
                    var lab = document.createElement('label');
                    lab.className = 'hris-live-chat-check';
                    var cb = document.createElement('input');
                    cb.type = 'checkbox';
                    cb.checked = !!selected[user.user_id];
                    cb.addEventListener('change', function () {
                        if (cb.checked) selected[user.user_id] = true;
                        else delete selected[user.user_id];
                    });
                    var span = document.createElement('span');
                    span.textContent = (user.user_alias || user.user_name || 'User') + (user.online ? ' · online' : '');
                    lab.appendChild(cb);
                    lab.appendChild(span);
                    list.appendChild(lab);
                });
            }
            search.addEventListener('input', renderPicks);
            renderPicks();

            var cancel = document.createElement('button');
            cancel.type = 'button';
            cancel.className = 'hris-live-chat-ghost';
            cancel.textContent = 'Batal';
            cancel.addEventListener('click', closeSheet);
            var create = document.createElement('button');
            create.type = 'button';
            create.className = 'hris-live-chat-primary';
            create.textContent = 'Buat grup';
            create.addEventListener('click', function () {
                var members = Object.keys(selected);
                create.disabled = true;
                requestJson(urls.group, {
                    method: 'POST',
                    json: true,
                    body: JSON.stringify({ name: nameInput.value, members: members })
                }).then(function (json) {
                    if (!json || !json.status) {
                        window.alert((json && json.error) || 'Gagal membuat grup');
                        create.disabled = false;
                        return;
                    }
                    state.directory = null;
                    closeSheet();
                    var group = json.group || {};
                    selectPeer(group.peer, group.group_name || 'Grup', true);
                    heartbeat();
                }).catch(function () {
                    create.disabled = false;
                });
            });
            sheetFootEl.appendChild(cancel);
            sheetFootEl.appendChild(create);
            nameInput.focus();
        });
    }

    function addPick(parent, opts) {
        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'hris-live-chat-pick';
        var meta = document.createElement('span');
        var name = document.createElement('div');
        name.className = 'hris-live-chat-pick-name';
        name.textContent = opts.label;
        meta.appendChild(name);
        if (opts.sub) {
            var sub = document.createElement('div');
            sub.className = 'hris-live-chat-pick-sub';
            sub.textContent = opts.sub;
            meta.appendChild(sub);
        }
        btn.appendChild(meta);
        btn.addEventListener('click', opts.onClick);
        parent.appendChild(btn);
    }

    function openForwardSheet(msg) {
        if (!msg || !msg.message_id) return;
        loadDirectory(function (dir) {
            openSheet('Teruskan pesan');
            var search = document.createElement('input');
            search.type = 'search';
            search.className = 'hris-live-chat-field';
            search.placeholder = 'Cari tujuan...';
            sheetBodyEl.appendChild(search);

            var list = document.createElement('div');
            sheetBodyEl.appendChild(list);

            function sendForward(peer, label) {
                requestJson(urls.forward, {
                    method: 'POST',
                    json: true,
                    body: JSON.stringify({ message_id: msg.message_id, peer: peer })
                }).then(function (json) {
                    if (!json || !json.status) {
                        window.alert((json && json.error) || 'Gagal meneruskan pesan');
                        return;
                    }
                    closeSheet();
                    if (json.peer === state.peer && json.message) {
                        state.messages.push(json.message);
                        appendMessage(json.message, true);
                        if (json.message.message_id) state.lastId = json.message.message_id;
                    } else {
                        selectPeer(peer, label, true);
                    }
                }).catch(function () {});
            }

            function renderTargets() {
                while (list.firstChild) list.removeChild(list.firstChild);
                var q = String(search.value || '').toLowerCase().trim();
                function matches(text) {
                    return !q || String(text || '').toLowerCase().indexOf(q) !== -1;
                }
                if (matches('Chat Tim') || matches('tim')) {
                    addPick(list, {
                        label: 'Chat Tim',
                        sub: 'Semua user',
                        onClick: function () { sendForward('team', 'Chat Tim'); }
                    });
                }
                (state.groups || dir.groups || []).forEach(function (grp) {
                    var label = grp.group_name || 'Grup';
                    if (!matches(label)) return;
                    addPick(list, {
                        label: label,
                        sub: (grp.member_count || 0) + ' anggota',
                        onClick: function () { sendForward(grp.peer, label); }
                    });
                });
                var users = dir.users || [];
                var seen = {};
                users.forEach(function (user) {
                    var label = user.user_alias || user.user_name || 'User';
                    if (!matches(label) && !matches(user.user_mail)) return;
                    seen[user.user_id] = true;
                    addPick(list, {
                        label: label,
                        sub: formatLastOnline(user.last_seen, !!user.online),
                        onClick: function () { sendForward(user.user_id, label); }
                    });
                });
                (state.conversations || []).forEach(function (user) {
                    if (seen[user.user_id]) return;
                    var label = user.user_alias || user.user_name || 'User';
                    if (!matches(label)) return;
                    addPick(list, {
                        label: label,
                        sub: formatLastOnline(user.last_seen, !!user.online),
                        onClick: function () { sendForward(user.user_id, label); }
                    });
                });
            }
            search.addEventListener('input', renderTargets);
            renderTargets();
        });
    }

    function makeItem(opts) {
        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'hris-live-chat-item' + (opts.peer === state.peer ? ' is-active' : '');
        btn.setAttribute('data-peer', opts.peer);

        var dot = document.createElement('span');
        if (opts.kind === 'group') {
            dot.className = 'hris-live-chat-dot is-group';
            var icon = document.createElement('i');
            icon.className = 'bi bi-people-fill';
            dot.appendChild(icon);
        } else {
            dot.className = 'hris-live-chat-dot' + (opts.online ? ' is-online' : '');
        }
        btn.appendChild(dot);

        var meta = document.createElement('span');
        meta.style.minWidth = '0';
        meta.style.flex = '1';

        var name = document.createElement('span');
        name.className = 'hris-live-chat-item-name';
        name.textContent = opts.label;
        meta.appendChild(name);

        if (opts.sub) {
            var sub = document.createElement('span');
            sub.className = 'hris-live-chat-item-sub';
            sub.textContent = opts.sub;
            meta.appendChild(sub);
        }
        btn.appendChild(meta);

        if (opts.unread) {
            var badge = document.createElement('span');
            badge.className = 'hris-live-chat-item-badge';
            badge.textContent = opts.unread > 99 ? '99+' : String(opts.unread);
            btn.appendChild(badge);
        }

        btn.addEventListener('click', function () {
            selectPeer(opts.peer, opts.label, true);
        });
        return btn;
    }

    function renderSnapshot(payload) {
        if (!payload || !payload.status) return;
        if (payload.peer_read_at !== undefined) {
            state.peerReadAt = payload.peer_read_at || '';
            state.peerOnline = !!payload.peer_online;
            refreshReceipts();
        }
        var team = payload.team || {};
        var convs = payload.conversations || [];
        var groups = payload.groups || [];
        state.conversations = convs;
        state.groups = groups;
        var teamUnread = parseInt(team.unread, 10) || 0;
        if (state.open && !document.hidden && state.peer === 'team') teamUnread = 0;
        var unreadTotal = teamUnread;
        for (var c = 0; c < convs.length; c++) {
            var itemUnread = parseInt(convs[c].unread, 10) || 0;
            if (state.open && !document.hidden && convs[c].user_id === state.peer) itemUnread = 0;
            convs[c]._unread = itemUnread;
            unreadTotal += itemUnread;
        }
        for (var g = 0; g < groups.length; g++) {
            var groupUnread = parseInt(groups[g].unread, 10) || 0;
            if (state.open && !document.hidden && groups[g].peer === state.peer) groupUnread = 0;
            groups[g]._unread = groupUnread;
            unreadTotal += groupUnread;
        }
        setBadge(unreadTotal);
        var onlineCount = parseInt(payload.online_count, 10) || 0;
        if (statusEl) {
            statusEl.textContent = onlineCount > 0
                ? onlineCount + ' online'
                : 'Tidak ada user online';
        }
        if (!listEl) return;
        while (listEl.firstChild) listEl.removeChild(listEl.firstChild);

        var newGroupBtn = document.createElement('button');
        newGroupBtn.type = 'button';
        newGroupBtn.className = 'hris-live-chat-new-group';
        newGroupBtn.innerHTML = '<i class="bi bi-people"></i><span>Grup baru</span>';
        newGroupBtn.addEventListener('click', openGroupSheet);
        listEl.appendChild(newGroupBtn);

        var search = document.createElement('input');
        search.type = 'search';
        search.className = 'hris-live-chat-list-search';
        search.placeholder = 'Cari user atau grup...';
        search.setAttribute('autocomplete', 'off');
        search.addEventListener('input', function () {
            var q = String(search.value || '').toLowerCase().trim();
            var items = listEl.querySelectorAll('.hris-live-chat-item');
            for (var si = 0; si < items.length; si++) {
                var nameEl = items[si].querySelector('.hris-live-chat-item-name');
                var label = ((nameEl && nameEl.textContent) || '').toLowerCase();
                items[si].style.display = !q || label.indexOf(q) !== -1 ? '' : 'none';
            }
        });
        listEl.appendChild(search);

        var teamSub = previewText(team.last_body) || 'Percakapan semua user';
        listEl.appendChild(makeItem({
            peer: 'team',
            label: 'Chat Tim',
            sub: teamSub,
            online: true,
            unread: teamUnread
        }));

        if (groups.length) {
            listEl.appendChild(makeSection('Grup'));
            for (var gi = 0; gi < groups.length; gi++) {
                var grp = groups[gi];
                var gSub = grp.last_from
                    ? (grp.last_from + ': ' + (previewText(grp.last_body) || ''))
                    : (previewText(grp.last_body) || ((grp.member_count || 0) + ' anggota'));
                listEl.appendChild(makeItem({
                    peer: grp.peer,
                    label: grp.group_name || 'Grup',
                    sub: gSub,
                    kind: 'group',
                    unread: grp._unread || 0
                }));
            }
        }

        listEl.appendChild(makeSection('Pesan langsung'));
        if (!convs.length) {
            var empty = document.createElement('div');
            empty.className = 'hris-live-chat-empty';
            empty.style.fontSize = '0.72rem';
            empty.textContent = 'Tidak ada user lain.';
            listEl.appendChild(empty);
        }
        for (var i = 0; i < convs.length; i++) {
            var c = convs[i];
            listEl.appendChild(makeItem({
                peer: c.user_id,
                label: c.user_alias || c.user_name || 'User',
                sub: formatLastOnline(c.last_seen, !!c.online),
                online: !!c.online,
                unread: c._unread || 0
            }));
        }
        renderListActive();
        updateThreadHeader();
    }

    function appendMessage(msg, stickToBottom) {
        if (!messagesEl || !msg) return;
        var mid = msg.message_id || '';
        if (mid && state.seen[mid]) return;
        if (mid) state.seen[mid] = true;
        var empty = messagesEl.querySelector('.hris-live-chat-empty');
        if (empty) empty.remove();

        var bubble = document.createElement('div');
        bubble.className = 'hris-live-chat-bubble' + (msg.mine ? ' is-mine' : '');
        if (state.pinnedMessageId && msg.message_id === state.pinnedMessageId) {
            bubble.classList.add('is-pinned-msg');
        }
        bubble.setAttribute('data-message-id', msg.message_id || '');

        if (!msg.mine) {
            var name = document.createElement('div');
            name.className = 'hris-live-chat-bubble-name';
            name.textContent = msg.from_alias || 'User';
            bubble.appendChild(name);
        }

        if (msg.forwarded) {
            var fwd = document.createElement('div');
            fwd.className = 'hris-live-chat-fwd';
            fwd.textContent = 'Diteruskan' + (msg.forwarded_from_alias ? ' dari ' + msg.forwarded_from_alias : '');
            bubble.appendChild(fwd);
        }

        if (msg.reply_to_id) {
            var quote = document.createElement('div');
            quote.className = 'hris-live-chat-quote';
            quote.setAttribute('data-reply-to', msg.reply_to_id);
            var qName = document.createElement('strong');
            qName.textContent = msg.reply_from_alias || 'Pesan';
            quote.appendChild(qName);
            var qBody = document.createElement('span');
            qBody.textContent = previewText(msg.reply_body || (msg.reply_file_name ? ('Lampiran: ' + msg.reply_file_name) : 'Pesan'));
            quote.appendChild(qBody);
            quote.addEventListener('click', function (e) {
                e.preventDefault();
                scrollToMessage(msg.reply_to_id);
            });
            bubble.appendChild(quote);
        }

        if (msg.has_file && msg.file_url) {
            if (msg.file_is_audio) {
                var audioEl = document.createElement('audio');
                audioEl.className = 'hris-live-chat-audio';
                audioEl.controls = true;
                audioEl.preload = 'metadata';
                audioEl.src = msg.file_url;
                bubble.appendChild(audioEl);
            } else if (msg.file_is_image) {
                var imgLink = document.createElement('a');
                imgLink.href = msg.file_url;
                imgLink.target = '_blank';
                imgLink.rel = 'noopener';
                var img = document.createElement('img');
                img.className = 'hris-live-chat-thumb';
                img.alt = msg.file_name || 'Gambar';
                img.src = msg.file_url;
                imgLink.appendChild(img);
                bubble.appendChild(imgLink);
            }
            if (!msg.file_is_audio) {
                var fileLink = document.createElement('a');
                fileLink.className = 'hris-live-chat-file-link';
                fileLink.href = msg.file_url;
                fileLink.target = '_blank';
                fileLink.rel = 'noopener';
                fileLink.textContent = (msg.file_is_image ? '' : '📎 ') + (msg.file_name || 'Unduh lampiran');
                if (msg.file_size) fileLink.textContent += ' (' + formatFileSize(msg.file_size) + ')';
                bubble.appendChild(fileLink);
            }
        }

        if (msg.body) {
            var body = document.createElement('div');
            body.className = 'hris-live-chat-bubble-body';
            body.textContent = escapeText(msg.body);
            bubble.appendChild(body);
        }

        var time = document.createElement('span');
        time.className = 'hris-live-chat-bubble-time';
        var timeText = document.createElement('span');
        timeText.textContent = formatTime(msg.created_at);
        time.appendChild(timeText);
        if (msg.mine) {
            time.appendChild(buildTicks(msg.created_at));
        }
        bubble.appendChild(time);

        var actions = document.createElement('div');
        actions.className = 'hris-live-chat-bubble-actions';
        var copyBtn = document.createElement('button');
        copyBtn.type = 'button';
        copyBtn.className = 'hris-live-chat-action-copy';
        copyBtn.title = 'Salin';
        copyBtn.innerHTML = '<i class="bi bi-copy"></i>';
        copyBtn.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            var plain = messagePlainText(msg);
            if (!plain) return;
            copyToClipboard(plain).catch(function () {
                window.alert('Gagal menyalin teks');
            });
        });
        var replyBtn = document.createElement('button');
        replyBtn.type = 'button';
        replyBtn.title = 'Balas';
        replyBtn.innerHTML = '<i class="bi bi-reply"></i>';
        replyBtn.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            setReply(msg);
        });
        var pinBtn = document.createElement('button');
        pinBtn.type = 'button';
        pinBtn.title = (state.pinnedMessageId === msg.message_id) ? 'Lepas pin' : 'Pin pesan';
        pinBtn.innerHTML = '<i class="bi bi-pin-angle"></i>';
        pinBtn.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            if (state.pinnedMessageId === msg.message_id) setPinnedMessage(null);
            else setPinnedMessage(msg.message_id);
        });
        var fwdBtn = document.createElement('button');
        fwdBtn.type = 'button';
        fwdBtn.title = 'Teruskan';
        fwdBtn.innerHTML = '<i class="bi bi-forward"></i>';
        fwdBtn.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            openForwardSheet(msg);
        });
        actions.appendChild(copyBtn);
        actions.appendChild(replyBtn);
        actions.appendChild(pinBtn);
        actions.appendChild(fwdBtn);
        if (msg.mine) {
            var delBtn = document.createElement('button');
            delBtn.type = 'button';
            delBtn.className = 'is-danger';
            delBtn.title = 'Hapus';
            delBtn.innerHTML = '<i class="bi bi-trash"></i>';
            delBtn.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                deleteMessage(msg);
            });
            actions.appendChild(delBtn);
        }
        bubble.appendChild(actions);

        var nearBottom = messagesEl.scrollHeight - messagesEl.scrollTop - messagesEl.clientHeight < 80;
        messagesEl.appendChild(bubble);
        if (stickToBottom || nearBottom || msg.mine) {
            messagesEl.scrollTop = messagesEl.scrollHeight;
        }
    }

    function receiptStatus(createdAt) {
        var created = String(createdAt || '').replace('T', ' ').slice(0, 19);
        var readAt = String(state.peerReadAt || '').replace('T', ' ').slice(0, 19);
        if (readAt && created && created <= readAt) return 'read';
        if (state.peerOnline) return 'delivered';
        return 'sent';
    }

    function applyTickClass(el, status) {
        if (!el) return;
        el.classList.remove('is-sent', 'is-delivered', 'is-read');
        el.classList.add('is-' + status);
        var icon = el.querySelector('i');
        if (!icon) {
            icon = document.createElement('i');
            el.appendChild(icon);
        }
        if (status === 'sent') {
            icon.className = 'bi bi-check';
            el.title = 'Terkirim';
        } else if (status === 'delivered') {
            icon.className = 'bi bi-check-all';
            el.title = 'Diterima';
        } else {
            icon.className = 'bi bi-check-all';
            el.title = 'Dibaca';
        }
    }

    function buildTicks(createdAt) {
        var wrap = document.createElement('span');
        wrap.className = 'hris-live-chat-ticks';
        wrap.setAttribute('data-created-at', createdAt || '');
        wrap.appendChild(document.createElement('i'));
        applyTickClass(wrap, receiptStatus(createdAt));
        return wrap;
    }

    function refreshReceipts() {
        if (!messagesEl) return;
        var nodes = messagesEl.querySelectorAll('.hris-live-chat-ticks');
        for (var i = 0; i < nodes.length; i++) {
            applyTickClass(nodes[i], receiptStatus(nodes[i].getAttribute('data-created-at')));
        }
    }

    function applyReceiptPayload(json) {
        if (!json) return;
        if (json.peer_read_at !== undefined) state.peerReadAt = json.peer_read_at || '';
        if (json.peer_online !== undefined) state.peerOnline = !!json.peer_online;
        refreshReceipts();
    }

    function renderMessages(rows, reset) {
        if (!messagesEl) return;
        if (reset) {
            while (messagesEl.firstChild) messagesEl.removeChild(messagesEl.firstChild);
        }
        if (!rows || !rows.length) {
            if (reset) {
                var empty = document.createElement('div');
                empty.className = 'hris-live-chat-empty';
                empty.textContent = 'Belum ada pesan. Kirim yang pertama.';
                messagesEl.appendChild(empty);
            }
            return;
        }
        for (var i = 0; i < rows.length; i++) {
            appendMessage(rows[i], reset && i === rows.length - 1);
            if (rows[i].message_id) state.lastId = rows[i].message_id;
        }
    }

    function loadMessages(reset) {
        if (!urls.messages) return;
        if (state.searchOpen && state.searchQuery && state.searchQuery.length >= 2) {
            runMessageSearch();
            return;
        }
        var url = urls.messages + '?peer=' + encodeURIComponent(state.peer);
        if (!reset && state.lastId) {
            url += '&after=' + encodeURIComponent(state.lastId);
        }
        requestJson(url).then(function (json) {
            if (!json || !json.status) return;
            applyReceiptPayload(json);
            applyPeerPrefs(json);
            var rows = json.messages || [];
            if (reset) {
                state.messages = rows;
                state.lastId = '';
                state.seen = {};
                renderMessages(rows, true);
                markRead();
                return;
            }
            if (rows.length) {
                state.messages = state.messages.concat(rows);
                renderMessages(rows, false);
                markRead();
            }
        }).catch(function () {});
    }

    function markRead() {
        if (!urls.read) return;
        requestJson(urls.read, {
            method: 'POST',
            json: true,
            body: JSON.stringify({ peer: state.peer })
        }).then(function (json) {
            applyReceiptPayload(json);
        }).catch(function () {});
    }

    function heartbeat() {
        if (!urls.heartbeat) return;
        var viewing = (state.open && !document.hidden) ? state.peer : '';
        requestJson(urls.heartbeat, {
            method: 'POST',
            json: true,
            body: JSON.stringify({ viewing: viewing })
        }).then(function (json) {
            renderSnapshot(json);
        }).catch(function () {});
    }

    function sendMessage() {
        if (state.sending || !urls.send) return;
        var body = inputEl ? String(inputEl.value || '').trim() : '';
        if (!body && !state.pendingFile) return;
        state.sending = true;
        if (sendBtn) sendBtn.disabled = true;

        var req;
        if (state.pendingFile) {
            var form = new FormData();
            form.append('peer', state.peer);
            form.append('body', body);
            if (state.replyTo && state.replyTo.message_id) {
                form.append('reply_to', state.replyTo.message_id);
            }
            form.append('file', state.pendingFile);
            var token = csrfToken();
            if (token) form.append('csrfmiddlewaretoken', token);
            var headers = { 'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json' };
            if (token) headers['X-CSRFToken'] = token;
            req = fetch(urls.send, {
                method: 'POST',
                credentials: 'same-origin',
                headers: headers,
                body: form
            }).then(function (res) {
                return res.json().catch(function () {
                    return { status: false, error: 'Gagal mengirim lampiran' };
                });
            });
        } else {
            req = requestJson(urls.send, {
                method: 'POST',
                json: true,
                body: JSON.stringify({
                    peer: state.peer,
                    body: body,
                    reply_to: (state.replyTo && state.replyTo.message_id) || ''
                })
            });
        }

        req.then(function (json) {
            if (!json || !json.status) {
                if (json && json.error) window.alert(json.error);
                return;
            }
            if (inputEl) inputEl.value = '';
            setPendingFile(null);
            clearReply();
            if (json.message) {
                state.messages.push(json.message);
                appendMessage(json.message, true);
                if (json.message.message_id) state.lastId = json.message.message_id;
            }
        }).catch(function () {
        }).then(function () {
            state.sending = false;
            if (sendBtn) sendBtn.disabled = false;
            if (inputEl) inputEl.focus();
        });
    }

    function scheduleTimers() {
        if (state.heartbeatTimer) {
            clearInterval(state.heartbeatTimer);
            state.heartbeatTimer = null;
        }
        if (state.pollTimer) {
            clearInterval(state.pollTimer);
            state.pollTimer = null;
        }
        var hidden = document.hidden;
        var beatMs = hidden ? 25000 : 12000;
        var pollMs = state.open ? (hidden ? 6000 : 2500) : 0;
        state.heartbeatTimer = setInterval(heartbeat, beatMs);
        if (pollMs) {
            state.pollTimer = setInterval(function () {
                if (state.searchOpen && state.searchQuery && state.searchQuery.length >= 2) return;
                loadMessages(false);
            }, pollMs);
        }
    }

    if (fab) {
        fab.addEventListener('click', function () {
            unlockAudio();
            setOpen(!state.open);
        });
    }
    if (navToggle) {
        navToggle.addEventListener('click', function (e) {
            e.preventDefault();
            unlockAudio();
            setOpen(!state.open);
        });
    }
    if (closeBtn) {
        closeBtn.addEventListener('click', function () {
            setOpen(false);
        });
    }
    if (backBtn) {
        backBtn.addEventListener('click', function () {
            if (panel) panel.classList.remove('is-thread');
        });
    }
    if (sendBtn) {
        sendBtn.addEventListener('click', sendMessage);
    }
    if (attachBtn && fileInput) {
        attachBtn.addEventListener('click', function () {
            setEmojiOpen(false);
            fileInput.click();
        });
    }
    if (voiceBtn) {
        voiceBtn.addEventListener('click', function () {
            setEmojiOpen(false);
            if (state.recordingVoice) return;
            startVoiceRecording();
        });
    }
    if (voiceCancelBtn) {
        voiceCancelBtn.addEventListener('click', function () {
            cancelVoiceRecording();
        });
    }
    if (voiceSendBtn) {
        voiceSendBtn.addEventListener('click', function () {
            stopVoiceRecording(true);
        });
    }
    if (emojiBtn) {
        emojiBtn.addEventListener('click', function (e) {
            e.preventDefault();
            setEmojiOpen(!state.emojiOpen);
        });
    }
    if (emojiSearchEl) {
        emojiSearchEl.addEventListener('input', function () {
            var q = String(emojiSearchEl.value || '').trim();
            renderEmojiGrid(q ? '' : (state.emojiCat || 'smile'), q);
        });
    }
    if (fileInput) {
        fileInput.addEventListener('change', function () {
            var file = fileInput.files && fileInput.files[0];
            if (!file) {
                setPendingFile(null);
                return;
            }
            if (file.size > 10 * 1024 * 1024) {
                window.alert('Ukuran file maksimal 10MB');
                fileInput.value = '';
                return;
            }
            setPendingFile(file);
        });
    }
    if (fileClearBtn) {
        fileClearBtn.addEventListener('click', function () {
            setPendingFile(null);
        });
    }
    if (replyClearBtn) {
        replyClearBtn.addEventListener('click', clearReply);
    }
    if (sheetCloseBtn) {
        sheetCloseBtn.addEventListener('click', closeSheet);
    }
    if (threadTitle) {
        threadTitle.addEventListener('click', function () {
            if (isGroupPeer(state.peer)) openMembersSheet();
            else if (isDirectPeer(state.peer)) openProfilePane();
        });
    }
    if (headerPinBtn) {
        headerPinBtn.addEventListener('click', function () {
            if (state.pinnedMessageId) scrollToMessage(state.pinnedMessageId);
            else window.alert('Pin pesan dari menu aksi di bubble pesan (ikon pin).');
        });
    }
    if (headerClearBtn) {
        headerClearBtn.addEventListener('click', clearConversation);
    }
    if (headerSearchBtn) {
        headerSearchBtn.addEventListener('click', function () {
            setSearchOpen(!state.searchOpen);
        });
    }
    if (searchCloseBtn) {
        searchCloseBtn.addEventListener('click', function () {
            setSearchOpen(false);
        });
    }
    if (searchInputEl) {
        searchInputEl.addEventListener('input', function () {
            if (searchDebounceTimer) clearTimeout(searchDebounceTimer);
            searchDebounceTimer = setTimeout(runMessageSearch, 320);
        });
        searchInputEl.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') setSearchOpen(false);
        });
    }
    if (inputEl) {
        inputEl.addEventListener('input', syncInputHeight);
        syncInputHeight();
        inputEl.addEventListener('keydown', function (e) {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendMessage();
            }
        });
    }
    document.addEventListener('click', unlockAudio, { once: true });
    document.addEventListener('visibilitychange', scheduleTimers);

    heartbeat();
    scheduleTimers();
})();
