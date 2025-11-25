window.RDE = window.RDE || {};
if (window.RDE.initialized) {
  console.debug('[RDE] app.js already initialized');
} else {
  window.RDE.initialized = true;

  const significantFormatter = new Intl.NumberFormat('en-US', { maximumSignificantDigits: 4 });

  const fmt = (value) => {
    if (value == null || value === 'n/a') return 'n/a';
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) return 'n/a';
      return significantFormatter.format(value);
    }
    return String(value);
  };

  const fmtRange = (min, max, unit = '') => {
    if (typeof min !== 'number' || typeof max !== 'number') return 'n/a';
    const unitText = unit ? ` ${unit}` : '';
    return `${fmt(min)}–${fmt(max)}${unitText}`;
  };

  window.RDE.util = { fmt, fmtRange };

  function accepts(file, acceptList) {
    if (!file || !acceptList || !acceptList.length) return true;
    const name = (file.name || '').toLowerCase();
    const type = (file.type || '').toLowerCase();
    return acceptList.some((rule) => {
      if (!rule) return false;
      const value = rule.toLowerCase();
      if (value.startsWith('.')) {
        return name.endsWith(value);
      }
      return type.includes(value);
    });
  }

  function formatBytes(bytes) {
    if (!bytes || Number.isNaN(bytes)) return '';
    if (bytes < 1024) return `${bytes} B`;
    const kb = bytes / 1024;
    if (kb < 1024) return `${kb.toFixed(1)} KB`;
    const mb = kb / 1024;
    return `${mb.toFixed(1)} MB`;
  }

  function createFileInput(el, options) {
    let input = el.querySelector('input[type="file"]');
    if (input) return input;

    input = document.createElement('input');
    input.type = 'file';
    input.style.display = 'none';
    if (options && options.multiple) {
      input.multiple = true;
    }
    if (options && options.name) {
      input.name = options.name;
    }
    if (options && options.required) {
      input.required = true;
    }
    if (options && options.form) {
      input.setAttribute('form', options.form);
    }
    if (options && options.accept && options.accept.length) {
      const extensions = options.accept.filter((rule) => rule && rule.startsWith('.')).join(',');
      if (extensions) {
        input.setAttribute('accept', extensions);
      }
    }
    el.appendChild(input);
    return input;
  }

  function assignFilesToInput(input, files) {
    if (!input) return false;
    if (!files || !files.length) {
      input.value = '';
      return true;
    }
    let assigned = false;
    if (typeof DataTransfer !== 'undefined') {
      try {
        const dataTransfer = new DataTransfer();
        files.forEach((file) => dataTransfer.items.add(file));
        input.files = dataTransfer.files;
        assigned = true;
      } catch (error) {
        console.warn('[RDE] DataTransfer assignment failed:', error);
      }
    }
    if (!assigned) {
      try {
        input.files = files;
        assigned = true;
      } catch (error) {
        console.warn('[RDE] FileList assignment failed:', error);
      }
    }
    if (!assigned) {
      alert('Unable to use dropped files. Please use the browse button.');
    }
    return assigned;
  }

  function wireDropzone(el, acceptList, onFiles, options = {}) {
    if (!el || el.dataset.rdeDropzone === 'bound') return;
    el.dataset.rdeDropzone = 'bound';

    const labelEl = el.querySelector('[data-file-label]');
    const errorEl = el.querySelector('[data-error]');
    const defaultLabel = labelEl ? labelEl.textContent : '';
    const input = createFileInput(el, options);
    const maxSizeMb = typeof options.maxSizeMb === 'number'
      ? options.maxSizeMb
      : Number.parseFloat(el.dataset.maxSizeMb || '') || 0;
    const maxSizeBytes = maxSizeMb > 0 ? maxSizeMb * 1024 * 1024 : 0;

    const clearError = () => {
      if (errorEl) {
        errorEl.textContent = '';
        errorEl.classList.add('hidden');
      }
      el.classList.remove('has-error');
    };

    const showError = (message) => {
      if (errorEl) {
        errorEl.textContent = message || '';
        errorEl.classList.remove('hidden');
      }
      el.classList.add('has-error');
    };

    const updateLabel = (files) => {
      if (!labelEl) return;
      if (!files || !files.length) {
        labelEl.textContent = defaultLabel;
        el.classList.remove('has-file');
        return;
      }
      const first = files[0];
      const sizeLabel = formatBytes(first.size);
      labelEl.textContent = sizeLabel ? `${first.name} (${sizeLabel})` : first.name;
      el.classList.add('has-file');
    };

    const handleSelected = (files) => {
      if (!files || !files.length) {
        assignFilesToInput(input, files);
        updateLabel(files);
        clearError();
        onFiles([]);
        return;
      }
      const accepted = files.filter((file) => accepts(file, acceptList));
      if (!accepted.length) {
        assignFilesToInput(input, []);
        updateLabel([]);
        showError('Unsupported file type.');
        onFiles([]);
        return;
      }
      if (maxSizeBytes && accepted.some((file) => file.size > maxSizeBytes)) {
        assignFilesToInput(input, []);
        updateLabel([]);
        showError(`File must be smaller than ${fmt(maxSizeMb)} MB.`);
        onFiles([]);
        return;
      }
      clearError();
      const selected = accepted.slice(0, options.multiple ? accepted.length : 1);
      assignFilesToInput(input, selected);
      updateLabel(selected);
      onFiles(selected);
    };

    const prevent = (event) => {
      event.preventDefault();
      event.stopPropagation();
    };

    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach((eventName) => {
      el.addEventListener(eventName, prevent, { passive: false });
    });

    el.addEventListener('dragenter', () => {
      el.classList.add('hover');
    });

    el.addEventListener('dragover', () => {
      el.classList.add('hover');
    });

    el.addEventListener('dragleave', () => {
      el.classList.remove('hover');
    });

    el.addEventListener('drop', (event) => {
      el.classList.remove('hover');
      const dt = event.dataTransfer;
      if (!dt || !dt.files || !dt.files.length) {
        return;
      }
      handleSelected(Array.from(dt.files));
    });

    const browse = el.querySelector('[data-action="browse"]');
    if (browse) {
      browse.addEventListener('click', (event) => {
        event.preventDefault();
        input.click();
      });
    }

    input.addEventListener('change', () => {
      handleSelected(Array.from(input.files || []));
    });

    updateLabel(Array.from(input.files || []));
  }

  function initDropzones() {
    const zones = [
      { sel: '#drop-pems', key: 'pems', accept: ['.csv', 'text/csv'], name: 'pems_file', required: true },
      {
        sel: '#drop-gps',
        key: 'gps',
        accept: ['.csv', '.nmea', '.gpx', '.txt', 'text/csv', 'text/plain', 'application/xml'],
        name: 'gps_file',
        required: true,
      },
      {
        sel: '#drop-ecu',
        key: 'ecu',
        accept: ['.csv', '.mf4', '.mdf', 'text/csv', 'application/octet-stream'],
        name: 'ecu_file',
        required: true,
      },
    ];

    zones.forEach((zone) => {
      const el = document.querySelector(zone.sel);
      if (!el) {
        console.warn('[RDE] Dropzone not found:', zone.sel);
        return;
      }
      const maxSizeAttr = Number.parseFloat(el.dataset.maxSizeMb || '') || 0;
      wireDropzone(el, zone.accept, (files) => handleFiles(zone.key, files), {
        name: zone.name,
        required: zone.required,
        maxSizeMb: maxSizeAttr,
      });
    });
  }

  window.initDropzones = initDropzones;

  async function handleFiles(kind, files) {
    console.debug('[RDE] Files dropped:', kind, files.map((file) => file.name));
    window.RDE.uploads = window.RDE.uploads || {};
    window.RDE.uploads[kind] = files;
  }

  (function RDE_APP_BOOTSTRAP() {
    'use strict';

    // Patch once
    if (!window.__RDE_SAFE_MAP_PATCHED__) {
      window.__RDE_SAFE_MAP_PATCHED__ = true;

      if (typeof window.safeInitMap !== 'function') {
        window.safeInitMap = function () { return true; };
      }
      var __base = window.safeInitMap;

      window.safeInitMap = function safeInitMap(payload, el) { // eslint-disable-line no-global-assign
        if (!el || !payload || !payload.visual || !payload.visual.map) return false;
        try {
          if (typeof L === 'undefined') return __base(payload, el);

          // Ensure height if CSS collapsed
          var h = el.clientHeight || 0;
          if (h < 60) { el.style.height = '320px'; }

          // Reuse map if already initialized
          if (el.__leafletMap) return true;

          var center = payload.visual.map.center || { lat: 48.2082, lon: 16.3738, zoom: 9 };
          var map = L.map(el, { preferCanvas: true });
          el.__leafletMap = map;

          L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap',
            maxZoom: 19
          }).addTo(map);

          map.setView([center.lat, center.lon], center.zoom || 9);

          var pts = (payload.visual.map.latlngs || []).filter(Boolean);
          if (pts.length) {
            var latlngs = pts.map(function (p) { return [p.lat, p.lon]; });
            L.polyline(latlngs, { weight: 3, opacity: 0.8 }).addTo(map);
            map.fitBounds(latlngs);
          }
          return true;
        } catch (e) {
          console.warn('Map render failed:', e);
          return false;
        }
      };
    }

    function safeInitCharts(payload, el) {
      if (!el || !payload || !payload.visual || !payload.visual.chart) return false;
      try {
        el.setAttribute('data-chart-ready', '1');

        if (el.__rdeChartDrawn) return true;
        el.__rdeChartDrawn = true;

        var series = (payload.visual.chart.series || [])[0];
        var data = (series && series.data) || [];

        // Visible “ready” text if no data
        if (!data.length) {
          var note = document.createElement('div');
          note.style.opacity = '0.7';
          note.style.fontSize = '12px';
          note.textContent = 'Chart ready';
          el.appendChild(note);
          return true;
        }

        var w = el.clientWidth || 600;
        var h = 200;
        var svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', w);
        svg.setAttribute('height', h);
        svg.setAttribute('viewBox', '0 0 ' + w + ' ' + h);
        svg.style.color = 'rgba(148,163,184,0.9)';

        var ys = data.map(function (v) { return +v || 0; });
        var min = Math.min.apply(null, ys);
        var max = Math.max.apply(null, ys);
        var span = (max - min) || 1;

        var d = 'M 0 ' + (h - ((ys[0] - min) / span) * h);
        for (var i = 1; i < ys.length; i++) {
          var x = (i / (ys.length - 1)) * (w - 2);
          var y = h - ((ys[i] - min) / span) * (h - 2);
          d += ' L ' + x.toFixed(1) + ' ' + y.toFixed(1);
        }

        var path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', 'none');
        path.setAttribute('stroke', 'currentColor');
        path.setAttribute('stroke-width', '2');

        svg.appendChild(path);
        el.appendChild(svg);
        return true;
      } catch (e) {
        console.warn('Chart render failed:', e);
        return false;
      }
    }

    window.safeInitCharts = safeInitCharts;

    window.safeInjectKpis = function safeInjectKpis(payload, el) {
      const kpis = payload && (payload.kpi_numbers || payload.kpis);
      if (!el || !kpis) return false;
      try {
        el.setAttribute('data-kpis-present', '1');
        return true;
      } catch (error) {
        console.warn('KPI injection failed:', error);
        return false;
      }
    };

    async function downloadCurrentPdf() {
      const post = async (url) => fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ results_payload: getResultsPayload() })
      });

      try {
        // First try: normal path (CI wants this to 503 if WeasyPrint missing)
        var url = '/export_pdf';
        var res = await post(url);

        // If WeasyPrint is missing in dev, retry with fallback=1
        if (res.status === 503) {
          url = '/export_pdf?dev_fallback=1';
          res = await post(url);
        }

        var ct = (res.headers.get('content-type') || '').toLowerCase();
        if (!res.ok || !ct.includes('application/pdf')) {
          var text = await res.text().catch(function () { return ''; });
          throw new Error(text || ('PDF request failed (' + res.status + ')'));
        }

        var blob = await res.blob();
        var href = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = href;
        a.download = 'report_eu7_ld.pdf';
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(href);
      } catch (err) {
        console.error('PDF export failed:', err);
        alert('PDF export failed.\n' + (err && err.message ? err.message : ''));
      }
    }

    window.downloadCurrentPdf = downloadCurrentPdf;

    function getResultsPayload() {
      return window.results_payload || window.__RDE_RESULT__ || {};
    }

    window.getResultsPayload = getResultsPayload;

    function filterBySection(list, name) {
      return (Array.isArray(list) ? list : []).filter((item) => item && item.section === name);
    }

    const numberFormatter = new Intl.NumberFormat('en-US', { maximumFractionDigits: 3 });

    const pass = (b) => (b === true ? 'pass' : (b === false ? 'fail' : 'na'));

    const resultLabel = (state) => {
      if (state === 'pass') return 'PASS';
      if (state === 'fail') return 'FAIL';
      return 'n/a'.toUpperCase();
    };

    function buildKpis(payload) {
      const el = document.getElementById('kpis');
      if (!el) return false;
      const fc = (payload && payload.final_conformity) || {};
      const emissions = (payload && payload.emissions) || {};
      const trip = emissions.trip || {};
      const maw = (payload && payload.maw_coverage) || {};
      const gps = (payload && payload.gps) || {};
      const elevation = (payload && payload.elevation) || {};
      const tiles = [
        {
          label: 'NOx Final',
          value: fc.NOx_mg_km && fc.NOx_mg_km.value,
          unit: 'mg/km',
          pass: fc.NOx_mg_km && fc.NOx_mg_km.pass,
          goto: '#final-conformity',
        },
        {
          label: 'PN10 Final',
          value: fc.PN10_hash_km && fc.PN10_hash_km.value,
          unit: '#/km',
          pass: fc.PN10_hash_km && fc.PN10_hash_km.pass,
          goto: '#final-conformity',
        },
        {
          label: 'CO Trip',
          value: trip.CO_mg_km,
          unit: 'mg/km',
          goto: '#emissions-summary',
        },
        {
          label: 'CO Urban',
          value: (emissions.urban && emissions.urban.CO_mg_km) || null,
          unit: 'mg/km',
          goto: '#emissions-summary',
        },
        {
          label: 'Duration',
          value: payload && payload.trip_shares ? payload.trip_shares.duration_min : null,
          unit: 'min',
          goto: '#trip-shares',
        },
        {
          label: 'GPS gaps',
          value: gps.total_gaps_s,
          unit: 's',
          goto: '#gps',
        },
        {
          label: 'Δh',
          value: elevation.start_end_abs_m,
          unit: 'm',
          goto: '#elevation',
        },
        {
          label: 'MAW Low/High',
          value: maw.low_pct != null && maw.high_pct != null ? `${fmt(maw.low_pct)}% / ${fmt(maw.high_pct)}%` : 'n/a',
          unit: '',
          goto: '#maw-coverage',
        },
      ];

      el.innerHTML = tiles
        .map((t) => {
          const state = pass(t.pass);
          const stateClass = state ? ` result ${state}` : '';
          const valueText = typeof t.value === 'number' ? fmt(t.value) : (t.value || 'n/a');
          return `
    <div class="tile${stateClass}" data-goto="${t.goto || ''}">
      <div class="label">${t.label}</div>
      <div class="value">${valueText} <span class="unit">${t.unit || ''}</span></div>
    </div>`;
        })
        .join('');

      Array.from(el.querySelectorAll('.tile')).forEach((tile) => {
        const target = tile.dataset.goto;
        if (target) {
          tile.addEventListener('click', () => {
            const section = document.querySelector(target);
            if (section) {
              section.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
          });
        }
      });
      return true;
    }

    function renderRows(tbodyId, rows) {
      const tbody = document.getElementById(tbodyId);
      if (!tbody) return;
      const safeRows = Array.isArray(rows) ? rows : [];
      tbody.innerHTML = safeRows
        .map((row) => {
          const [ref, criterion, condition, value, unit, result] = row;
          const displayValue = typeof value === 'number' ? fmt(value) : (value || 'n/a');
          const displayUnit = unit || '';
          const resultStateValue = typeof result === 'string' ? result : pass(result);
          return `
        <tr>
          <td>${ref || '—'}</td>
          <td>${criterion || '—'}</td>
          <td>${condition || '—'}</td>
          <td>${displayValue}</td>
          <td>${displayUnit}</td>
          <td class="result ${resultStateValue}">${resultLabel(resultStateValue)}</td>
        </tr>`;
        })
        .join('');
    }

    function rowsPreconSoak(payload) {
      const block = (payload && payload.precon_soak) || {};
      const drive = block.drive_10min_each || {};
      const durationRange = block.soak_duration_range || [6, 72];
      const tempRange = block.soak_temperature_range || [-7, 38];
      const durationText = [fmt(drive.urban_min_s), fmt(drive.rural_min_s), fmt(drive.motorway_min_s)].join(' / ');
      const soakDuration = block.soak_duration_h;
      const minDuration = Array.isArray(durationRange) ? durationRange[0] : null;
      const maxDuration = Array.isArray(durationRange) ? durationRange[1] : null;
      return [
        ['§8.3', '≥10 min each operation', '≥ 600 s (urban/rural/motorway)', durationText, 's', pass(drive.ok)],
        [
          '§10.6',
          'Soak duration',
          `${fmt(minDuration)}–${fmt(maxDuration)} h`,
          soakDuration,
          'h',
          typeof soakDuration === 'number' && minDuration != null && maxDuration != null
            ? soakDuration >= minDuration && soakDuration <= maxDuration
            : null,
        ],
        [
          '§10.6',
          'Soak temperature range',
          `${fmt(tempRange[0])}…${fmt(tempRange[1])} °C`,
          `${fmt(block.soak_temp_min_c)}–${fmt(block.soak_temp_max_c)}`,
          '°C',
          'na',
        ],
        [
          '§10.6',
          'Extended temp multiplier',
          'Applied when last 3h in range',
          block.extended_temp_flag ? `applied (${fmt(block.extended_last_temp_c)} °C)` : 'not applied',
          '',
          block.extended_temp_flag ? 'pass' : 'na',
        ],
      ];
    }

    function rowsColdStart(payload) {
      const block = (payload && payload.cold_start) || {};
      const moveLimit = typeof block.duration_limit_s === 'number' ? block.duration_limit_s : 120;
      const stopLimit = 180;
      return [
        ['§8.3', 'Start/End logged', 'Documentation available', block.start_end_logged ? 'logged' : 'missing', '', pass(block.start_end_logged)],
        ['§8.3', 'Vehicle moves within limit', `≤ ${fmt(moveLimit)} s`, block.move_within_s, 's', typeof block.move_within_s === 'number' ? block.move_within_s <= moveLimit : null],
        ['§8.3', 'Total stop time', `≤ ${fmt(stopLimit)} s`, block.stop_total_s, 's', typeof block.stop_total_s === 'number' ? block.stop_total_s <= stopLimit : null],
        ['§8.3', 'Average / max speed', '≤ 40 / ≤ 60 km/h', `${fmt(block.avg_speed_kmh)} / ${fmt(block.max_speed_kmh)}`, 'km/h', 'na'],
        ['§10.6', 'Multiplier applied', '1.6 when ambient in range', block.multiplier_applied ? `applied (${fmt(block.ambient_temp_c)} °C)` : 'not applied', '', block.multiplier_applied ? 'pass' : 'na'],
      ];
    }

    function rowsTripShares(payload) {
      const block = (payload && payload.trip_shares) || {};
      const distanceMin = block.distance_min_km || {};
      const shareRanges = block.share_ranges || {};
      const durationRange = block.duration_range || [];
      const orderExpected = Array.isArray(block.order_expected) ? block.order_expected.join(' → ') : 'n/a';
      const orderActual = Array.isArray(block.order) && block.order.length ? block.order.join(' → ') : 'n/a';
      const urbanRange = Array.isArray(shareRanges.urban) ? shareRanges.urban : [];
      const motorwayRange = Array.isArray(shareRanges.motorway) ? shareRanges.motorway : [];
      const durationLabel = durationRange.length >= 2 ? fmtRange(durationRange[0], durationRange[1], 'min') : 'n/a';
      const urbanLabel = urbanRange.length >= 2 ? fmtRange(urbanRange[0], urbanRange[1], '%') : 'n/a';
      const motorwayLabel = motorwayRange.length >= 2 ? fmtRange(motorwayRange[0], motorwayRange[1], '%') : 'n/a';
      return [
        ['§7.1', 'Urban distance', `≥ ${fmt(distanceMin.urban)} km`, block.urban_km, 'km', typeof block.urban_km === 'number' ? block.urban_km >= (distanceMin.urban || 0) : null],
        ['§7.1', 'Motorway distance', `≥ ${fmt(distanceMin.motorway)} km`, block.motorway_km, 'km', typeof block.motorway_km === 'number' ? block.motorway_km >= (distanceMin.motorway || 0) : null],
        ['§7.1', 'Trip duration', durationLabel, block.duration_min, 'min', typeof block.duration_min === 'number' && durationRange.length >= 2 ? block.duration_min >= durationRange[0] && block.duration_min <= durationRange[1] : null],
        ['§7.1', 'Urban share', urbanLabel, `${fmt(block.urban_share_pct)}%`, '%', typeof block.urban_share_pct === 'number' && urbanRange.length >= 2 ? block.urban_share_pct >= urbanRange[0] && block.urban_share_pct <= urbanRange[1] : null],
        ['§7.1', 'Motorway share', motorwayLabel, `${fmt(block.motorway_share_pct)}%`, '%', typeof block.motorway_share_pct === 'number' && motorwayRange.length >= 2 ? block.motorway_share_pct >= motorwayRange[0] && block.motorway_share_pct <= motorwayRange[1] : null],
        ['§7.1', 'Phase order', orderExpected, orderActual, '', Array.isArray(block.order) && Array.isArray(block.order_expected) ? block.order.join('|') === block.order_expected.join('|') : null],
      ];
    }

    function rowsElevation(payload) {
      const block = (payload && payload.elevation) || {};
      const startLimit = block.start_end_limit_m;
      const tripLimit = block.trip_limit_m_per_100km;
      const urbanLimit = block.urban_limit_m_per_100km;
      const delta = block.start_end_delta_m;
      return [
        ['§7.2', 'Start/end elevation delta', `≤ ${fmt(startLimit)} m`, delta, 'm', typeof delta === 'number' && typeof startLimit === 'number' ? Math.abs(delta) <= startLimit : null],
        ['§7.2', 'Trip cumulative elevation', `≤ ${fmt(tripLimit)} m/100km`, block.trip_cumulative_m_per_100km, 'm/100km', typeof block.trip_cumulative_m_per_100km === 'number' && typeof tripLimit === 'number' ? block.trip_cumulative_m_per_100km <= tripLimit : null],
        ['§7.2', 'Urban cumulative elevation', `≤ ${fmt(urbanLimit)} m/100km`, block.urban_cumulative_m_per_100km, 'm/100km', typeof block.urban_cumulative_m_per_100km === 'number' && typeof urbanLimit === 'number' ? block.urban_cumulative_m_per_100km <= urbanLimit : null],
        ['§7.2', 'Extended conditions active', 'Declared when thresholds exceeded', block.extended_active ? 'extended' : 'normal', '', block.extended_active ? 'pass' : 'na'],
        ['§7.2', 'Extended emissions valid', 'If extended, emissions must pass', block.extended_emissions_valid ? 'valid' : 'invalid', '', pass(block.extended_emissions_valid)],
      ];
    }

    function rowsGps(payload) {
      const block = (payload && payload.gps) || {};
      return [
        ['§7.3', 'Distance delta', `≤ ${fmt(block.delta_limit_pct)} %`, `${fmt(block.distance_delta_pct)} %`, '%', typeof block.distance_delta_pct === 'number' && typeof block.delta_limit_pct === 'number' ? Math.abs(block.distance_delta_pct) <= block.delta_limit_pct : null],
        ['§7.3', 'Max gap', `≤ ${fmt(block.max_gap_limit_s)} s`, block.max_gap_s, 's', typeof block.max_gap_s === 'number' && typeof block.max_gap_limit_s === 'number' ? block.max_gap_s <= block.max_gap_limit_s : null],
        ['§7.3', 'Total gaps', `≤ ${fmt(block.total_gaps_limit_s)} s`, block.total_gaps_s, 's', typeof block.total_gaps_s === 'number' && typeof block.total_gaps_limit_s === 'number' ? block.total_gaps_s <= block.total_gaps_limit_s : null],
        ['§7.3', 'Gap events', 'Descriptive', (block.gaps || []).length ? `${(block.gaps || []).length} gap(s)` : 'no gaps', '', (block.gaps || []).length ? 'na' : 'pass'],
      ];
    }

    function rowsSpanZero(payload) {
      const block = (payload && payload.span_zero) || {};
      const zero = block.zero || {};
      const span = block.span || {};
      const coverage = block.coverage || {};
      const limits = block.limits || {};
      return [
        ['§6.1', 'CO₂ zero drift', `≤ ${fmt(limits.co2_zero_ppm)} ppm`, zero.co2_ppm, 'ppm', typeof zero.co2_ppm === 'number' && typeof limits.co2_zero_ppm === 'number' ? Math.abs(zero.co2_ppm) <= limits.co2_zero_ppm : null],
        ['§6.1', 'CO zero drift', `≤ ${fmt(limits.co_zero_ppm)} ppm`, zero.co_ppm, 'ppm', typeof zero.co_ppm === 'number' && typeof limits.co_zero_ppm === 'number' ? Math.abs(zero.co_ppm) <= limits.co_zero_ppm : null],
        ['§6.1', 'NOx zero drift', `≤ ${fmt(limits.nox_zero_ppm)} ppm`, zero.nox_ppm, 'ppm', typeof zero.nox_ppm === 'number' && typeof limits.nox_zero_ppm === 'number' ? Math.abs(zero.nox_ppm) <= limits.nox_zero_ppm : null],
        ['§6.1', 'PN zero', `≤ ${fmt(limits.pn_zero_hash_cm3)} #/cm³`, zero.pn_hash_cm3, '#/cm³', typeof zero.pn_hash_cm3 === 'number' && typeof limits.pn_zero_hash_cm3 === 'number' ? zero.pn_hash_cm3 <= limits.pn_zero_hash_cm3 : null],
        ['§6.3', 'CO₂ span drift', `≤ ${fmt(limits.co2_span_ppm)} ppm`, span.co2_ppm, 'ppm', typeof span.co2_ppm === 'number' && typeof limits.co2_span_ppm === 'number' ? Math.abs(span.co2_ppm) <= limits.co2_span_ppm : null],
        ['§6.3', 'CO span drift', `≤ ${fmt(limits.co_span_ppm)} ppm`, span.co_ppm, 'ppm', typeof span.co_ppm === 'number' && typeof limits.co_span_ppm === 'number' ? Math.abs(span.co_ppm) <= limits.co_span_ppm : null],
        ['§6.3', 'NOx span drift', `≤ ${fmt(limits.nox_span_ppm)} ppm`, span.nox_ppm, 'ppm', typeof span.nox_ppm === 'number' && typeof limits.nox_span_ppm === 'number' ? Math.abs(span.nox_ppm) <= limits.nox_span_ppm : null],
        ['§6.3', 'CO₂ span coverage', `≥ ${fmt(limits.coverage_min_pct)} %`, `${fmt(coverage.co2_pct)} %`, '%', typeof coverage.co2_pct === 'number' && typeof limits.coverage_min_pct === 'number' ? coverage.co2_pct >= limits.coverage_min_pct : null],
        ['§6.3', 'CO span coverage', `≥ ${fmt(limits.coverage_min_pct)} %`, `${fmt(coverage.co_pct)} %`, '%', typeof coverage.co_pct === 'number' && typeof limits.coverage_min_pct === 'number' ? coverage.co_pct >= limits.coverage_min_pct : null],
        ['§6.3', 'NOx span coverage', `≥ ${fmt(limits.coverage_min_pct)} %`, `${fmt(coverage.nox_pct)} %`, '%', typeof coverage.nox_pct === 'number' && typeof limits.coverage_min_pct === 'number' ? coverage.nox_pct >= limits.coverage_min_pct : null],
        ['§6.3', 'CO₂ between span and 2×span', `≤ ${fmt(limits.two_x_pct)} %`, `${fmt(coverage.co2_mid_pct)} %`, '%', typeof coverage.co2_mid_pct === 'number' && typeof limits.two_x_pct === 'number' ? coverage.co2_mid_pct <= limits.two_x_pct : null],
        ['§6.3', 'CO₂ >2×span events', `≤ ${fmt(limits.exceed_count)}`, coverage.co2_over_limit, 'count', typeof coverage.co2_over_limit === 'number' && typeof limits.exceed_count === 'number' ? coverage.co2_over_limit <= limits.exceed_count : null],
      ];
    }

    function rowsDevices(payload) {
      const block = (payload && payload.devices) || {};
      const limits = block.limits || {};
      return [
        ['§4.6', 'Gas PEMS', 'Identifier', block.gas_pems || 'n/a', '', 'na'],
        ['§4.6', 'PN PEMS', 'Identifier', block.pn_pems || 'n/a', '', 'na'],
        ['§4.6', 'EFM', 'Identifier', block.efm || 'n/a', '', 'na'],
        ['§4.6', 'Gas PEMS leak rate', `≤ ${fmt(limits.leak_rate_pct)} %`, block.leak_rate_pct, '%', typeof block.leak_rate_pct === 'number' && typeof limits.leak_rate_pct === 'number' ? block.leak_rate_pct <= limits.leak_rate_pct : null],
        ['§4.6', 'PN dilute pressure rise', `≤ ${fmt(limits.pn_dilute_pressure_mbar)} mbar`, block.pn_dilute_pressure_mbar, 'mbar', typeof block.pn_dilute_pressure_mbar === 'number' && typeof limits.pn_dilute_pressure_mbar === 'number' ? block.pn_dilute_pressure_mbar <= limits.pn_dilute_pressure_mbar : null],
        ['§4.6', 'PN sample pressure rise', `≤ ${fmt(limits.pn_sample_pressure_mbar)} mbar`, block.pn_sample_pressure_mbar, 'mbar', typeof block.pn_sample_pressure_mbar === 'number' && typeof limits.pn_sample_pressure_mbar === 'number' ? block.pn_sample_pressure_mbar <= limits.pn_sample_pressure_mbar : null],
        ['§4.6', 'Device errors', `≤ ${fmt(limits.device_errors)}`, block.device_errors, 'count', typeof block.device_errors === 'number' && typeof limits.device_errors === 'number' ? block.device_errors <= limits.device_errors : null],
      ];
    }

    function rowsDynamics(payload) {
      const block = (payload && payload.dynamics) || {};
      const urban = block.urban || {};
      const motorway = block.motorway || {};
      const limits = block.limits || {};
      const speeds = block.avg_speeds_kmh || {};
      const vaLimits = limits.va_pos95 || {};
      const rpaLimits = limits.rpa_min || {};
      return [
        ['§7.4', '<span class="swatch" style="background:var(--urban);"></span>Urban v̄', 'Descriptive', fmt(speeds.urban), 'km/h', 'na'],
        ['§7.4', '<span class="swatch" style="background:var(--urban);"></span>Urban a⁺95', `≤ ${fmt(vaLimits.urban)} m²/s³`, urban.va_pos95, 'm²/s³', typeof urban.va_pos95 === 'number' && typeof vaLimits.urban === 'number' ? urban.va_pos95 <= vaLimits.urban : null],
        ['§7.4', '<span class="swatch" style="background:var(--urban);"></span>Urban RPA', `≥ ${fmt(rpaLimits.urban)} m/s²`, urban.rpa, 'm/s²', typeof urban.rpa === 'number' && typeof rpaLimits.urban === 'number' ? urban.rpa >= rpaLimits.urban : null],
        ['§7.4', '<span class="swatch" style="background:var(--motorway);"></span>Motorway v̄', 'Descriptive', fmt(speeds.motorway), 'km/h', 'na'],
        ['§7.4', '<span class="swatch" style="background:var(--motorway);"></span>Motorway a⁺95', `≤ ${fmt(vaLimits.motorway)} m²/s³`, motorway.va_pos95, 'm²/s³', typeof motorway.va_pos95 === 'number' && typeof vaLimits.motorway === 'number' ? motorway.va_pos95 <= vaLimits.motorway : null],
        ['§7.4', '<span class="swatch" style="background:var(--motorway);"></span>Motorway RPA', `≥ ${fmt(rpaLimits.motorway)} m/s²`, motorway.rpa, 'm/s²', typeof motorway.rpa === 'number' && typeof rpaLimits.motorway === 'number' ? motorway.rpa >= rpaLimits.motorway : null],
        ['§7.4', 'Acceleration points (urban / motorway)', 'Descriptive', `${fmt(urban.accel_points)} / ${fmt(motorway.accel_points)}`, '', 'na'],
      ];
    }

    function rowsMawCoverage(payload) {
      const block = (payload && payload.maw_coverage) || {};
      return [
        ['§7.5', 'Low speed coverage', `≥ ${fmt(block.low_limit_pct)} %`, `${fmt(block.low_pct)} %`, '%', typeof block.low_pct === 'number' && typeof block.low_limit_pct === 'number' ? block.low_pct >= block.low_limit_pct : null],
        ['§7.5', 'High speed coverage', `≥ ${fmt(block.high_limit_pct)} %`, `${fmt(block.high_pct)} %`, '%', typeof block.high_pct === 'number' && typeof block.high_limit_pct === 'number' ? block.high_pct >= block.high_limit_pct : null],
        ['§7.5', 'Windows analysed', 'Descriptive', Array.isArray(block.windows) && block.windows.length ? `${block.windows.length} windows` : 'n/a', '', 'na'],
      ];
    }

    function rowsEmissionsSummary(payload) {
      const block = (payload && payload.emissions_summary) || {};
      const phases = block.phases || {};
      const rows = [];
      Object.keys(phases).forEach((key) => {
        const data = phases[key] || {};
        const label = data.label || key;
        const final = (payload && payload.final_conformity) || {};
        let tripState = 'na';
        if (key === 'trip') {
          const checks = [];
          if (final.NOx_mg_km && typeof final.NOx_mg_km.pass === 'boolean') checks.push(final.NOx_mg_km.pass);
          if (final.PN10_hash_km && typeof final.PN10_hash_km.pass === 'boolean') checks.push(final.PN10_hash_km.pass);
          if (checks.length) {
            tripState = pass(checks.every(Boolean));
          }
        }
        rows.push([
          label,
          data.NOx_mg_km != null ? `${fmt(data.NOx_mg_km)} mg/km` : 'n/a',
          data.PN10_hash_km != null ? `${fmt(data.PN10_hash_km)} #/km` : 'n/a',
          data.CO_mg_km != null ? `${fmt(data.CO_mg_km)} mg/km` : 'n/a',
          data.CO2_g_km != null ? `${fmt(data.CO2_g_km)} g/km` : 'n/a',
          key === 'trip' ? tripState : 'na',
        ]);
      });
      return rows;
    }

    function rowsFinalConformity(payload) {
      const fc = (payload && payload.final_conformity) || {};
      const limits = (payload && payload.limits) || {};
      const entries = [];
      if (fc.NOx_mg_km) {
        entries.push(['NOx', `≤ ${fmt(fc.NOx_mg_km.limit || limits.NOx_mg_km_RDE)} mg/km`, fc.NOx_mg_km.value, 'mg/km', fc.NOx_mg_km.pass]);
      }
      if (fc.PN10_hash_km) {
        const limit = fc.PN10_hash_km.limit || limits.PN10_hash_km_RDE || limits.PN_hash_km_RDE;
        entries.push(['PN10', `≤ ${fmt(limit)} #/km`, fc.PN10_hash_km.value, '#/km', fc.PN10_hash_km.pass]);
      }
      if (fc.CO_mg_km) {
        entries.push(['CO', 'Informative', fc.CO_mg_km.value, 'mg/km', fc.CO_mg_km.pass]);
      }
      if (!entries.length) {
        entries.push(['n/a', 'Limits unavailable', 'n/a', '', 'na']);
      }
      return entries;
    }

    function renderModernTables(payload) {
      renderRows('tbl-precon-soak', rowsPreconSoak(payload));
      renderRows('tbl-cold-start', rowsColdStart(payload));
      renderRows('tbl-trip-shares', rowsTripShares(payload));
      renderRows('tbl-elevation', rowsElevation(payload));
      renderRows('tbl-gps', rowsGps(payload));
      renderRows('tbl-span-zero', rowsSpanZero(payload));
      renderRows('tbl-devices', rowsDevices(payload));
      renderRows('tbl-dynamics', rowsDynamics(payload));
      renderRows('tbl-maw-coverage', rowsMawCoverage(payload));
      renderRows('tbl-emissions-summary', rowsEmissionsSummary(payload));
      renderRows('tbl-final-conformity', rowsFinalConformity(payload));
    }

    function setupCanvas(canvas) {
      if (!canvas) return null;
      const ctx = canvas.getContext('2d');
      if (!ctx) return null;
      const dpr = window.devicePixelRatio || 1;
      const width = canvas.clientWidth || 340;
      const height = canvas.clientHeight || 240;
      if (canvas.width !== width * dpr || canvas.height !== height * dpr) {
        canvas.width = width * dpr;
        canvas.height = height * dpr;
      }
      if (ctx.resetTransform) {
        ctx.resetTransform();
      } else {
        ctx.setTransform(1, 0, 0, 1, 0, 0);
      }
      ctx.scale(dpr, dpr);
      ctx.clearRect(0, 0, width, height);
      return { ctx, width, height };
    }

    function renderOverviewChart(payload) {
      const canvas = document.getElementById('chart-overview');
      if (!canvas) return;
      if (canvas.dataset.rendered) return;
      const visual = (payload && payload.visual && payload.visual.chart) || {};
      const series = Array.isArray(visual.series) ? visual.series : [];
      const datasets = series.map((item, idx) => ({
        name: item && item.name ? item.name : `Series ${idx + 1}`,
        values: (item && item.values) || [],
        color: idx === 0 ? '#38bdf8' : '#fbbf24',
      }));
      const setup = setupCanvas(canvas);
      if (!setup) return;
      const { ctx, width, height } = setup;
      ctx.fillStyle = 'rgba(15,23,42,0.7)';
      ctx.fillRect(0, 0, width, height);
      datasets.forEach((dataset, idx) => {
        const values = dataset.values.filter((v) => typeof v === 'number');
        if (!values.length) return;
        const min = Math.min.apply(null, values);
        const max = Math.max.apply(null, values);
        const span = max - min || 1;
        ctx.beginPath();
        ctx.strokeStyle = dataset.color;
        ctx.lineWidth = 2;
        values.forEach((value, i) => {
          const x = (i / Math.max(1, values.length - 1)) * (width - 20) + 10;
          const norm = (value - min) / span;
          const y = height - 20 - norm * (height - 40);
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.stroke();
      });
      ctx.fillStyle = 'rgba(255,255,255,0.7)';
      ctx.font = '12px Inter, sans-serif';
      datasets.forEach((dataset, idx) => {
        ctx.fillText(dataset.name, 12, 18 + idx * 14);
      });
      canvas.dataset.rendered = '1';
    }

    function renderEmissionsChart(payload) {
      const canvas = document.getElementById('chart-emissions');
      if (!canvas) return;
      if (canvas.dataset.rendered) return;
      const block = (payload && payload.emissions_summary && payload.emissions_summary.phases) || {};
      const phases = ['urban', 'rural', 'motorway', 'trip'];
      const pollutants = [
        { key: 'NOx_mg_km', label: 'NOx', color: '#38bdf8' },
        { key: 'PN10_hash_km', label: 'PN10', color: '#f97316' },
        { key: 'CO_mg_km', label: 'CO', color: '#22c55e' },
      ];
      const values = pollutants.map((pollutant) => (
        phases.map((phase) => {
          const data = block[phase] || {};
          const raw = data[pollutant.key];
          return typeof raw === 'number' ? raw : null;
        })
      ));
      const maxima = values.map((list) => {
        const numeric = list.filter((v) => typeof v === 'number');
        if (!numeric.length) return 0;
        return Math.max.apply(null, numeric);
      });
      const setup = setupCanvas(canvas);
      if (!setup) return;
      const { ctx, width, height } = setup;
      ctx.fillStyle = 'rgba(15,23,42,0.7)';
      ctx.fillRect(0, 0, width, height);
      const chartHeight = height - 50;
      const groupWidth = (width - 40) / phases.length;
      phases.forEach((phase, idx) => {
        pollutants.forEach((pollutant, pIdx) => {
          const value = values[pIdx][idx];
          if (typeof value !== 'number' || maxima[pIdx] <= 0) return;
          const ratio = value / maxima[pIdx];
          const barHeight = ratio * chartHeight;
          const barWidth = (groupWidth / pollutants.length) * 0.7;
          const x = 20 + idx * groupWidth + pIdx * (groupWidth / pollutants.length);
          const y = height - 30 - barHeight;
          ctx.fillStyle = pollutant.color;
          ctx.fillRect(x, y, barWidth, barHeight);
        });
        ctx.fillStyle = 'rgba(255,255,255,0.7)';
        ctx.font = '12px Inter, sans-serif';
        ctx.fillText(phase.toUpperCase(), 20 + idx * groupWidth, height - 10);
      });
      canvas.dataset.rendered = '1';
    }

    function renderDynamicsChart(payload) {
      const canvas = document.getElementById('chart-dynamics');
      if (!canvas) return;
      if (canvas.dataset.rendered) return;
      const block = (payload && payload.dynamics) || {};
      const urban = block.urban || {};
      const rural = block.rural || {};
      const motorway = block.motorway || {};
      const points = [
        { label: 'Urban', data: urban, color: '#00b3b3' },
        { label: 'Rural', data: rural, color: '#5865f2' },
        { label: 'Motorway', data: motorway, color: '#9b5de5' },
      ];
      const setup = setupCanvas(canvas);
      if (!setup) return;
      const { ctx, width, height } = setup;
      ctx.fillStyle = 'rgba(15,23,42,0.7)';
      ctx.fillRect(0, 0, width, height);
      const mid = height / 2;

      const drawScatter = (values, originX, originY, title) => {
        const widthSpan = width / 2 - 20;
        const heightSpan = height / 2 - 40;
        ctx.strokeStyle = 'rgba(255,255,255,0.15)';
        ctx.strokeRect(originX, originY, widthSpan, heightSpan);
        ctx.fillStyle = 'rgba(255,255,255,0.6)';
        ctx.font = '12px Inter, sans-serif';
        ctx.fillText(title, originX, originY - 6);
        const maxV = Math.max.apply(null, values.map((item) => (item && typeof item.v === 'number' ? item.v : 0)).concat([1]));
        const maxY = Math.max.apply(null, values.map((item) => (item && typeof item.y === 'number' ? item.y : 0)).concat([1]));
        values.forEach((item) => {
          if (!item || typeof item.v !== 'number' || typeof item.y !== 'number') return;
          const x = originX + (item.v / maxV) * (widthSpan - 20) + 10;
          const y = originY + heightSpan - (item.y / maxY) * (heightSpan - 20) - 10;
          ctx.fillStyle = item.color;
          ctx.beginPath();
          ctx.arc(x, y, 4, 0, Math.PI * 2);
          ctx.fill();
        });
      };

      const vaValues = points.map((item) => ({
        v: typeof item.data.avg_speed_kmh === 'number' ? item.data.avg_speed_kmh : 0,
        y: typeof item.data.va_pos95 === 'number' ? item.data.va_pos95 : 0,
        color: item.color,
      }));
      const rpaValues = points.map((item) => ({
        v: typeof item.data.avg_speed_kmh === 'number' ? item.data.avg_speed_kmh : 0,
        y: typeof item.data.rpa === 'number' ? item.data.rpa : 0,
        color: item.color,
      }));

      drawScatter(vaValues, 10, mid - 30, 'va⁺95 vs v̄');
      drawScatter(rpaValues, mid + 10, mid - 30, 'RPA vs v̄');
      canvas.dataset.rendered = '1';
    }

    function renderMawChart(payload) {
      const canvas = document.getElementById('chart-maw');
      if (!canvas) return;
      if (canvas.dataset.rendered) return;
      const block = (payload && payload.maw_coverage) || {};
      const setup = setupCanvas(canvas);
      if (!setup) return;
      const { ctx, width, height } = setup;
      ctx.fillStyle = 'rgba(15,23,42,0.7)';
      ctx.fillRect(0, 0, width, height);
      const values = [
        { label: 'Low speed', value: block.low_pct, limit: block.low_limit_pct, color: '#38bdf8' },
        { label: 'High speed', value: block.high_pct, limit: block.high_limit_pct, color: '#f97316' },
      ];
      const maxValue = Math.max.apply(null, values.map((item) => (typeof item.value === 'number' ? item.value : 0)).concat([100]));
      const barWidth = (width - 60) / values.length;
      values.forEach((item, idx) => {
        if (typeof item.value !== 'number' || maxValue <= 0) return;
        const barHeight = (item.value / maxValue) * (height - 60);
        const x = 30 + idx * barWidth;
        const y = height - 30 - barHeight;
        ctx.fillStyle = item.color;
        ctx.fillRect(x, y, barWidth * 0.6, barHeight);
        if (typeof item.limit === 'number') {
          const limitHeight = (item.limit / maxValue) * (height - 60);
          const limitY = height - 30 - limitHeight;
          ctx.strokeStyle = 'rgba(248,113,113,0.6)';
          ctx.beginPath();
          ctx.moveTo(x - 4, limitY);
          ctx.lineTo(x + barWidth * 0.6 + 4, limitY);
          ctx.stroke();
        }
        ctx.fillStyle = 'rgba(255,255,255,0.8)';
        ctx.font = '12px Inter, sans-serif';
        ctx.fillText(`${item.label}: ${fmt(item.value)}%`, x, height - 8);
      });
      canvas.dataset.rendered = '1';
    }

    function renderAmbientChart(payload) {
      const canvas = document.getElementById('chart-ambient');
      if (!canvas) return;
      if (canvas.dataset.rendered) return;
      const setup = setupCanvas(canvas);
      if (!setup) return;
      const { ctx, width, height } = setup;
      ctx.fillStyle = 'rgba(15,23,42,0.7)';
      ctx.fillRect(0, 0, width, height);
      ctx.fillStyle = 'rgba(255,255,255,0.6)';
      ctx.font = '14px Inter, sans-serif';
      ctx.fillText('Ambient / QA data unavailable', 20, height / 2);
      canvas.dataset.rendered = '1';
    }

    function renderRegressionCharts(payload) {
      const container = document.getElementById('qa-regressions');
      if (!container) return;
      if (container.dataset.rendered) return;
      const regressions = (payload && payload.regressions) || {};
      const efmCanvas = document.getElementById('chart-efm');
      const fuelCanvas = document.getElementById('chart-fuel');

      const drawMessage = (canvas, label) => {
        if (!canvas) return;
        const setup = setupCanvas(canvas);
        if (!setup) return;
        const { ctx, width, height } = setup;
        ctx.fillStyle = 'rgba(15,23,42,0.7)';
        ctx.fillRect(0, 0, width, height);
        ctx.fillStyle = 'rgba(255,255,255,0.6)';
        ctx.font = '13px Inter, sans-serif';
        ctx.fillText(`${label}: no data`, 20, height / 2);
      };

      if (regressions.efm && Array.isArray(regressions.efm.points)) {
        const setup = setupCanvas(efmCanvas);
        if (setup) {
          const { ctx, width, height } = setup;
          ctx.fillStyle = 'rgba(15,23,42,0.7)';
          ctx.fillRect(0, 0, width, height);
          ctx.fillStyle = '#38bdf8';
          regressions.efm.points.slice(0, 100).forEach((point) => {
            if (!point) return;
            const x = typeof point.x === 'number' ? point.x : 0;
            const y = typeof point.y === 'number' ? point.y : 0;
            ctx.beginPath();
            ctx.arc(20 + (x % (width - 40)), height - 20 - (y % (height - 40)), 3, 0, Math.PI * 2);
            ctx.fill();
          });
        }
      } else {
        drawMessage(efmCanvas, 'EFM regression');
      }

      if (regressions.fuel && Array.isArray(regressions.fuel.points)) {
        const setup = setupCanvas(fuelCanvas);
        if (setup) {
          const { ctx, width, height } = setup;
          ctx.fillStyle = 'rgba(15,23,42,0.7)';
          ctx.fillRect(0, 0, width, height);
          ctx.fillStyle = '#f97316';
          regressions.fuel.points.slice(0, 100).forEach((point) => {
            if (!point) return;
            const x = typeof point.x === 'number' ? point.x : 0;
            const y = typeof point.y === 'number' ? point.y : 0;
            ctx.beginPath();
            ctx.arc(20 + (x % (width - 40)), height - 20 - (y % (height - 40)), 3, 0, Math.PI * 2);
            ctx.fill();
          });
        }
      } else {
        drawMessage(fuelCanvas, 'Fuel regression');
      }

      container.dataset.rendered = '1';
    }

    function initVizTabs(payload) {
      const container = document.getElementById('viz');
      if (!container || container.dataset.bound) return;
      const buttons = Array.from(container.querySelectorAll('nav button'));
      const overview = document.getElementById('chart-overview');
      const emissions = document.getElementById('chart-emissions');
      const dynamics = document.getElementById('chart-dynamics');
      const maw = document.getElementById('chart-maw');
      const map = document.getElementById('map');
      const ambient = document.getElementById('chart-ambient');
      const regressions = document.getElementById('qa-regressions');

      const hideAll = () => {
        [overview, emissions, dynamics, maw, map, ambient].forEach((el) => {
          if (el) el.hidden = true;
        });
        if (regressions) regressions.hidden = true;
      };

      buttons.forEach((button) => {
        button.addEventListener('click', () => {
          buttons.forEach((btn) => btn.classList.remove('active'));
          button.classList.add('active');
          const target = button.dataset.tab;
          hideAll();
          if (target === 'overview' && overview) {
            overview.hidden = false;
            renderOverviewChart(payload);
          } else if (target === 'emissions' && emissions) {
            emissions.hidden = false;
            renderEmissionsChart(payload);
          } else if (target === 'dynamics' && dynamics) {
            dynamics.hidden = false;
            renderDynamicsChart(payload);
          } else if (target === 'maw' && maw) {
            maw.hidden = false;
            renderMawChart(payload);
          } else if (target === 'map' && map) {
            map.hidden = false;
            safeInitMap(payload, map);
          } else if (target === 'ambient' && ambient) {
            ambient.hidden = false;
            renderAmbientChart(payload);
            if (regressions) {
              regressions.hidden = false;
              renderRegressionCharts(payload);
            }
          }
        });
      });

      hideAll();
      if (overview) {
        overview.hidden = false;
        renderOverviewChart(payload);
      }
      if (buttons[0]) {
        buttons[0].classList.add('active');
      }
      container.dataset.bound = '1';
    }

    function renderModernCharts(payload) {
      renderOverviewChart(payload);
      renderEmissionsChart(payload);
      renderDynamicsChart(payload);
      renderMawChart(payload);
      renderAmbientChart(payload);
      renderRegressionCharts(payload);
      initVizTabs(payload);
    }

    function bindModernActions(payload) {
      const runBtn = document.getElementById('run-analysis');
      if (runBtn && !runBtn.dataset.bound) {
        runBtn.dataset.bound = '1';
        runBtn.addEventListener('click', () => {
          window.location.href = '/analyze?demo=1';
        });
      }
      const pdfBtn = document.getElementById('download-pdf');
      if (pdfBtn && !pdfBtn.dataset.bound) {
        pdfBtn.dataset.bound = '1';
        pdfBtn.addEventListener('click', (event) => {
          event.preventDefault();
          downloadCurrentPdf();
        });
      }
      const jsonBtn = document.getElementById('download-json');
      if (jsonBtn && !jsonBtn.dataset.bound) {
        jsonBtn.dataset.bound = '1';
        jsonBtn.addEventListener('click', () => {
          try {
            const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'eu7_report.json';
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(url);
          } catch (error) {
            console.warn('JSON download failed:', error);
          }
        });
      }
      buildKpis(payload);
    }

    function renderModernReport(payload) {
      const container = document.getElementById('kpis');
      if (!container) return false;
      renderModernTables(payload);
      renderModernCharts(payload);
      bindModernActions(payload);
      return true;
    }

    function normaliseValue(value) {
      if (value === null || typeof value === 'undefined') {
        return null;
      }
      if (typeof value === 'number') {
        return Number.isFinite(value) ? value : null;
      }
      if (typeof value === 'string') {
        const trimmed = value.trim();
        if (!trimmed) {
          return null;
        }
        if (/^[+-]?\d+(?:\.\d+)?(?:e[+-]?\d+)?$/i.test(trimmed)) {
          const parsed = Number(trimmed);
          return Number.isFinite(parsed) ? parsed : trimmed;
        }
        return trimmed;
      }
      return value;
    }

    function resultState(value) {
      if (value === true || String(value).toLowerCase() === 'pass') {
        return 'pass';
      }
      if (value === false || String(value).toLowerCase() === 'fail') {
        return 'fail';
      }
      return 'pending';
    }

    function createStatusBadge(result) {
      const badge = document.createElement('span');
      badge.className = 'ml-2 px-2 py-0.5 rounded';
      const state = resultState(result);
      if (state === 'pass') {
        badge.className += ' bg-emerald-600/20 text-emerald-300';
        badge.textContent = 'Pass';
      } else if (state === 'fail') {
        badge.className += ' bg-rose-600/20 text-rose-300';
        badge.textContent = 'Fail';
      } else {
        badge.className += ' bg-slate-700/40 text-slate-200';
        badge.textContent = 'n/a';
      }
      return badge;
    }

    function readValue(criterion) {
      if (!criterion || typeof criterion !== 'object') {
        return 'n/a';
      }
      if (criterion.value !== undefined && criterion.value !== null) {
        return criterion.value;
      }
      if (criterion.measured !== undefined && criterion.measured !== null) {
        return criterion.measured;
      }
      return 'n/a';
    }

    function formatValue(value) {
      const normalised = normaliseValue(value);
      if (normalised === null || typeof normalised === 'undefined') {
        return 'n/a';
      }
      if (typeof normalised === 'number') {
        return numberFormatter.format(normalised);
      }
      return String(normalised);
    }

    const pass = (b) => (b === true ? 'pass' : (b === false ? 'fail' : 'na'));

    const resultLabel = (state) => {
      if (state === 'pass') return 'PASS';
      if (state === 'fail') return 'FAIL';
      return 'n/a'.toUpperCase();
    };

    function setOverallStatus(payload) {
      const badge = document.getElementById('overall-result-badge');
      if (!badge) return;
      const finalBlock = (payload && payload.final) || {};
      const state = resultState(typeof finalBlock.pass === 'boolean' ? finalBlock.pass : null);
      const baseClasses = 'rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-wide';
      if (state === 'pass') {
        badge.className = baseClasses + ' bg-emerald-500/20 text-emerald-200';
        badge.textContent = 'Pass';
      } else if (state === 'fail') {
        badge.className = baseClasses + ' bg-rose-500/20 text-rose-200';
        badge.textContent = 'Fail';
      } else {
        badge.className = baseClasses + ' bg-amber-500/20 text-amber-700 dark:text-amber-200';
        badge.textContent = 'Pending limits';
      }

      const meta = payload && payload.meta ? payload.meta : {};
      const legislationBadge = document.getElementById('legislation-badge');
      if (legislationBadge && meta.legislation) {
        legislationBadge.textContent = meta.legislation;
      }
    }

    function renderQuickVerdicts(payload) {
      const container = document.getElementById('quick-verdicts');
      if (!container) return;
      container.innerHTML = '';
      const criteria = Array.isArray(payload && payload.criteria) ? payload.criteria : [];
      const sections = [
        'Pre/Post Checks (Zero/Span)',
        'Trip Composition & Timing',
        'Dynamics & MAW',
        'GPS Validity',
        'Emissions Summary',
      ];
      let total = 0;
      let passes = 0;

      sections.forEach((name) => {
        filterBySection(criteria, name)
          .slice(0, 2)
          .forEach((item) => {
            total += 1;
            if (resultState(item && item.result) === 'pass') {
              passes += 1;
            }
            const card = document.createElement('div');
            card.className = 'flex items-center justify-between rounded border border-slate-800/60 bg-slate-800/30 px-2 py-1';
            const label = document.createElement('span');
            label.className = 'text-slate-300 truncate';
            const title = (item && (item.description || item.criterion || item.id)) || 'Criterion';
            label.title = title;
            label.textContent = title;
            const badge = createStatusBadge(item && item.result);
            card.appendChild(label);
            card.appendChild(badge);
            container.appendChild(card);
          });
      });

      if (!container.children.length) {
        const placeholder = document.createElement('div');
        placeholder.className = 'rounded border border-slate-800/60 bg-slate-800/30 px-2 py-2 text-slate-400';
        placeholder.textContent = 'No quick verdict data.';
        container.appendChild(placeholder);
      }

      const summary = document.getElementById('quick-verdict-summary');
      if (summary) {
        if (!total) {
          summary.textContent = 'Summary pending';
          summary.className = 'px-2 py-0.5 rounded text-xs bg-slate-800/40 text-slate-300';
        } else {
          summary.textContent = `Quick verdicts: ${passes}/${total} pass`;
          summary.className = passes === total
            ? 'px-2 py-0.5 rounded text-xs bg-emerald-600/20 text-emerald-200'
            : 'px-2 py-0.5 rounded text-xs bg-amber-500/20 text-amber-200';
        }
      }
    }

    function renderSectionTable(selector, rows) {
      const target = typeof selector === 'string' ? document.querySelector(selector) : selector;
      if (!target) return;
      const criteria = Array.isArray(rows) ? rows : [];
      if (!criteria.length) {
        target.innerHTML = '<p class="text-sm text-slate-500 dark:text-slate-400">No criteria available for this section.</p>';
        return;
      }

      const wrapper = document.createElement('div');
      wrapper.className = 'overflow-x-auto';
      const table = document.createElement('table');
      table.className = 'min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-700';

      const thead = document.createElement('thead');
      thead.className = 'bg-slate-100 text-xs font-semibold uppercase tracking-wide text-slate-600 dark:bg-slate-800/80 dark:text-slate-300';
      thead.innerHTML = '<tr>'
        + '<th scope="col" class="px-4 py-3 text-left">Ref</th>'
        + '<th scope="col" class="px-4 py-3 text-left">Criterion</th>'
        + '<th scope="col" class="px-4 py-3 text-left">Condition</th>'
        + '<th scope="col" class="px-4 py-3 text-left">Value</th>'
        + '<th scope="col" class="px-4 py-3 text-left">Unit</th>'
        + '<th scope="col" class="px-4 py-3 text-left">Result</th>'
        + '</tr>';

      const tbody = document.createElement('tbody');
      tbody.className = 'divide-y divide-slate-100 dark:divide-slate-800';

      criteria.forEach((item) => {
        const row = document.createElement('tr');
        row.className = 'bg-white/60 text-slate-700 dark:bg-slate-900/60 dark:text-slate-200';

        const refCell = document.createElement('td');
        refCell.className = 'px-4 py-3 font-semibold text-slate-500 dark:text-slate-400';
        refCell.textContent = (item && (item.clause || item.ref || item.id)) || '—';

        const critCell = document.createElement('td');
        critCell.className = 'px-4 py-3 font-medium text-slate-900 dark:text-white';
        critCell.textContent = (item && (item.description || item.criterion || item.id)) || '—';

        const condCell = document.createElement('td');
        condCell.className = 'px-4 py-3 text-slate-600 dark:text-slate-300';
        condCell.textContent = (item && (item.limit || item.condition)) || '—';

        const valueCell = document.createElement('td');
        valueCell.className = 'px-4 py-3';
        const rawValue = readValue(item);
        valueCell.textContent = formatValue(rawValue);

        const unitCell = document.createElement('td');
        unitCell.className = 'px-4 py-3';
        unitCell.textContent = (item && item.unit) || '';

        const resultCell = document.createElement('td');
        resultCell.className = 'px-4 py-3';
        resultCell.appendChild(createStatusBadge(item && item.result));

        row.appendChild(refCell);
        row.appendChild(critCell);
        row.appendChild(condCell);
        row.appendChild(valueCell);
        row.appendChild(unitCell);
        row.appendChild(resultCell);
        tbody.appendChild(row);
      });

      table.appendChild(thead);
      table.appendChild(tbody);
      wrapper.appendChild(table);
      target.innerHTML = '';
      target.appendChild(wrapper);
    }

    function renderEmissionsSummary(selector, emissions, limits) {
      const target = typeof selector === 'string' ? document.querySelector(selector) : selector;
      if (!target) return;
      const block = emissions || {};
      const trip = block.trip || {};
      const urban = block.urban || {};
      const finalLimits = limits || {};

      const rows = [
        {
          label: 'Trip NOx (mg/km)',
          value: trip.NOx_mg_km,
          limit: finalLimits.NOx_mg_km_RDE,
          unit: 'mg/km',
        },
        {
          label: 'Trip PN (#/km)',
          value: trip.PN_hash_km,
          limit: finalLimits.PN_hash_km_RDE,
          unit: '#/km',
        },
        {
          label: 'Trip CO (mg/km)',
          value: trip.CO_mg_km,
          limit: finalLimits.CO_mg_km_WLTP,
          unit: 'mg/km',
        },
        {
          label: 'Urban NOx (mg/km)',
          value: urban.NOx_mg_km,
          limit: null,
          unit: 'mg/km',
        },
        {
          label: 'Urban PN (#/km)',
          value: urban.PN_hash_km,
          limit: null,
          unit: '#/km',
        },
        {
          label: 'Urban CO (mg/km)',
          value: urban.CO_mg_km,
          limit: null,
          unit: 'mg/km',
        },
      ].filter((item) => typeof item.value !== 'undefined' && item.value !== null);

      if (!rows.length) {
        target.innerHTML = '<p class="text-sm text-slate-500 dark:text-slate-400">Emission metrics unavailable.</p>';
        return;
      }

      const table = document.createElement('table');
      table.className = 'min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-700';
      const thead = document.createElement('thead');
      thead.className = 'bg-slate-100 text-xs font-semibold uppercase tracking-wide text-slate-600 dark:bg-slate-800/80 dark:text-slate-300';
      thead.innerHTML = '<tr>'
        + '<th class="px-4 py-3 text-left">Metric</th>'
        + '<th class="px-4 py-3 text-left">Value</th>'
        + '<th class="px-4 py-3 text-left">Limit</th>'
        + '<th class="px-4 py-3 text-left">Result</th>'
        + '</tr>';

      const tbody = document.createElement('tbody');
      tbody.className = 'divide-y divide-slate-100 dark:divide-slate-800';

      rows.forEach((row) => {
        const tr = document.createElement('tr');
        tr.className = 'bg-white/60 text-slate-700 dark:bg-slate-900/60 dark:text-slate-200';

        const labelCell = document.createElement('td');
        labelCell.className = 'px-4 py-3 font-medium text-slate-900 dark:text-white';
        labelCell.textContent = row.label;

        const valueCell = document.createElement('td');
        valueCell.className = 'px-4 py-3';
        const emissionValue = normaliseValue(row.value);
        if (typeof emissionValue === 'number' && row.unit && row.unit.includes('#')) {
          valueCell.textContent = `${emissionValue.toExponential(3)} ${row.unit}`;
        } else {
          valueCell.textContent = `${formatValue(emissionValue)} ${row.unit || ''}`.trim();
        }

        const limitCell = document.createElement('td');
        limitCell.className = 'px-4 py-3';
        if (typeof row.limit === 'number') {
          if (row.unit && row.unit.includes('#')) {
            limitCell.textContent = `≤ ${row.limit.toExponential(3)} ${row.unit}`;
          } else {
            limitCell.textContent = `≤ ${formatValue(row.limit)} ${row.unit || ''}`.trim();
          }
        } else {
          limitCell.textContent = '—';
        }

        const resultCell = document.createElement('td');
        resultCell.className = 'px-4 py-3';
        let state = 'pending';
        if (typeof row.limit === 'number' && typeof row.value === 'number') {
          state = row.value <= row.limit ? 'pass' : 'fail';
        }
        resultCell.appendChild(createStatusBadge(state));

        tr.appendChild(labelCell);
        tr.appendChild(valueCell);
        tr.appendChild(limitCell);
        tr.appendChild(resultCell);
        tbody.appendChild(tr);
      });

      table.appendChild(thead);
      table.appendChild(tbody);
      target.innerHTML = '';
      target.appendChild(table);
    }

    function renderFinalConformity(selector, payload) {
      const target = typeof selector === 'string' ? document.querySelector(selector) : selector;
      if (!target) return;
      const finalBlock = (payload && payload.final) || {};
      const pollutants = Array.isArray(finalBlock.pollutants)
        ? finalBlock.pollutants
        : filterBySection(payload && payload.criteria, 'Final Conformity');

      if (!pollutants || !pollutants.length) {
        target.innerHTML = '<p class="text-sm text-slate-500 dark:text-slate-400">Emission limits not fully configured; final verdict pending.</p>';
        return;
      }

      const table = document.createElement('table');
      table.className = 'min-w-full divide-y divide-slate-200 text-sm dark:divide-slate-700';
      const thead = document.createElement('thead');
      thead.className = 'bg-slate-100 text-xs font-semibold uppercase tracking-wide text-slate-600 dark:bg-slate-800/80 dark:text-slate-300';
      thead.innerHTML = '<tr>'
        + '<th class="px-4 py-3 text-left">Pollutant</th>'
        + '<th class="px-4 py-3 text-left">Condition</th>'
        + '<th class="px-4 py-3 text-left">Value</th>'
        + '<th class="px-4 py-3 text-left">Unit</th>'
        + '<th class="px-4 py-3 text-left">Result</th>'
        + '</tr>';

      const tbody = document.createElement('tbody');
      tbody.className = 'divide-y divide-slate-100 dark:divide-slate-800';

      pollutants.forEach((item) => {
        const row = document.createElement('tr');
        row.className = 'bg-white/60 text-slate-700 dark:bg-slate-900/60 dark:text-slate-200';

        const critCell = document.createElement('td');
        critCell.className = 'px-4 py-3 font-medium text-slate-900 dark:text-white';
        critCell.textContent = (item && (item.criterion || item.description || item.id)) || '—';

        const condCell = document.createElement('td');
        condCell.className = 'px-4 py-3 text-slate-600 dark:text-slate-300';
        condCell.textContent = (item && (item.condition || item.limit)) || '—';

        const valueCell = document.createElement('td');
        valueCell.className = 'px-4 py-3';
        const rawValue = readValue(item);
        valueCell.textContent = formatValue(rawValue);

        const unitCell = document.createElement('td');
        unitCell.className = 'px-4 py-3';
        unitCell.textContent = (item && item.unit) || '';

        const resultCell = document.createElement('td');
        resultCell.className = 'px-4 py-3';
        resultCell.appendChild(createStatusBadge(item && item.result));

        row.appendChild(critCell);
        row.appendChild(condCell);
        row.appendChild(valueCell);
        row.appendChild(unitCell);
        row.appendChild(resultCell);
        tbody.appendChild(row);
      });

      table.appendChild(thead);
      table.appendChild(tbody);
      target.innerHTML = '';
      target.appendChild(table);
    }

    window.filterBySection = filterBySection;
    window.renderQuickVerdicts = renderQuickVerdicts;
    window.renderSectionTable = renderSectionTable;
    window.renderEmissionsSummary = renderEmissionsSummary;
    window.renderFinalConformity = renderFinalConformity;

    function renderLegacyReport(payload) {
      try {
        setOverallStatus(payload);
        renderQuickVerdicts(payload);
        renderSectionTable('#section-zero-span', filterBySection(payload.criteria, 'Pre/Post Checks (Zero/Span)'));
        renderSectionTable('#section-trip-comp', filterBySection(payload.criteria, 'Trip Composition & Timing'));
        renderSectionTable('#section-dynamics', filterBySection(payload.criteria, 'Dynamics & MAW'));
        renderSectionTable('#section-gps', filterBySection(payload.criteria, 'GPS Validity'));
        renderEmissionsSummary('#section-emissions', payload.emissions, payload.limits);
        renderFinalConformity('#section-final', payload);

        const container = document.querySelector('#analysis-summary-content');
        if (typeof window.renderSummary === 'function' && container) {
          window.renderSummary(container);
        }

        const btn = document.getElementById('btn-export-pdf');
        if (btn && !btn._rdePdfBound) {
          btn._rdePdfBound = true;
          btn.addEventListener('click', (event) => {
            event.preventDefault();
            downloadCurrentPdf();
          });
        }
      } catch (error) {
        console.warn('Payload render failed:', error);
        return false;
      }
      return true;
    }

    // ==== RDE UI: required ready hook (must match test literal) ====
    document.addEventListener("rde:payload-ready", () => {
      let payload;
      try {
        payload = getResultsPayload();
      } catch (error) {
        console.warn('Payload retrieval failed:', error);
        return false;
      }

      try {
        const handled = renderModernReport(payload);
        if (!handled) {
          renderLegacyReport(payload);
        }
      } catch (error) {
        console.warn('Payload render failed:', error);
      }

      try {
        populateExportForms(payload);
      } catch (error) {
        console.warn('Export form population failed:', error);
      }

      return true;
    });

    function renderSummaryMarkdown(container, target) {
      const dataEl = container.querySelector('#summary-data');
      if (!dataEl) return;

      let markdown = '';
      try {
        markdown = JSON.parse(dataEl.textContent || '""');
      } catch (error) {
        markdown = dataEl.textContent || '';
      }

      if (!target) return;

      if (markdown && window.marked && typeof window.marked.parse === 'function') {
        target.innerHTML = window.marked.parse(markdown);
      } else {
        target.textContent = markdown;
      }
    }

    window.renderSummaryMarkdown = renderSummaryMarkdown;

    function renderSummary(container) {
      const target = container.querySelector('#analysis-summary-content') || container;
      try {
        const payload = getResultsPayload();
        safeInitMap(payload, document.getElementById('drive-map'));
        safeInitCharts(payload, document.getElementById('chart-speed'));
        safeInjectKpis(payload, target);
      } catch (error) {
        console.warn('Map render failed:', error);
        return false;
      }
      renderSummaryMarkdown(container, target);
      return true;
    }

    window.renderSummary = renderSummary;

    function populateExportForms(payload) {
      let serialised = '';
      if (payload) {
        try {
          serialised = JSON.stringify(payload);
        } catch (error) {
          serialised = '';
        }
      }
      const zipField = document.getElementById('exportZipPayload');
      if (zipField) {
        zipField.value = serialised;
      }
      const pdfField = document.getElementById('exportPdfPayload');
      if (pdfField) {
        pdfField.value = serialised;
      }
    }

    window.populateExportForms = populateExportForms;

    document.addEventListener('DOMContentLoaded', () => {
      initDropzones();
      populateExportForms(getResultsPayload());
    });
  })();
}

function rehydrateUI() {
  // Each called function should internally avoid double-binding
  if (typeof initDropzones === "function") initDropzones();
  if (typeof bindTabsAndCharts === "function") bindTabsAndCharts();
  if (typeof wireKpiDeepLinks === "function") wireKpiDeepLinks();
  if (typeof getResultsPayload === "function" && typeof populateExportForms === "function") {
    populateExportForms(getResultsPayload());
  }
}

document.addEventListener("rde:payload-ready", () => {
  rehydrateUI();
});

// DO NOT MODIFY THIS LINE: CI asserts this exact substring exists
document.addEventListener("htmx:afterSwap", (event) => {
  try { rehydrateUI(); } catch (e) { console.error("[RDE] rehydrate afterSwap failed", e); }
});
