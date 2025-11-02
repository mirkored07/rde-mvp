if (window.__RDE_APP_JS_LOADED__) {
  // already initialized
} else {
  window.__RDE_APP_JS_LOADED__ = true;
  (function RDE_APP_BOOTSTRAP() {
    'use strict';

    function initDropzones(root) {
      const context = root || document;
      try {
        const zones = Array.from(context.querySelectorAll('[data-dropzone]'));
        if (!zones.length) {
          return false;
        }

        zones.forEach((zone) => {
          if (!zone || zone._dropzoneBound) {
            return;
          }
          zone._dropzoneBound = true;

          const input = zone.querySelector('input[type="file"]');
          const feedback = zone.querySelector('[data-file-label]');
          const errorEl = zone.querySelector('[data-error]');

          const defaultFeedback = feedback ? feedback.textContent : '';
          if (feedback && !feedback.dataset.defaultLabel) {
            feedback.dataset.defaultLabel = defaultFeedback;
          }

          const allowedRaw = (input && input.dataset && input.dataset.allowed) || '';
          const allowed = allowedRaw
            .split(',')
            .map((value) => value.trim().toLowerCase())
            .filter(Boolean);

          let maxSizeMb = 0;
          if (input && input.dataset && input.dataset.maxSizeMb) {
            const parsed = Number.parseFloat(input.dataset.maxSizeMb);
            if (!Number.isNaN(parsed) && parsed > 0) {
              maxSizeMb = parsed;
            }
          }
          const maxSizeBytes = maxSizeMb ? maxSizeMb * 1024 * 1024 : 0;

          const setFeedback = (message) => {
            if (feedback) {
              feedback.textContent = message || feedback.dataset.defaultLabel || '';
            }
          };

          const showError = (message) => {
            if (zone) {
              zone.classList.add('has-error');
              zone.classList.remove('has-file');
            }
            if (errorEl) {
              errorEl.textContent = message || '';
              errorEl.classList.remove('hidden');
            }
            setFeedback(feedback && feedback.dataset ? feedback.dataset.defaultLabel : defaultFeedback);
          };

          const clearError = () => {
            if (zone) {
              zone.classList.remove('has-error');
            }
            if (errorEl) {
              errorEl.textContent = '';
              errorEl.classList.add('hidden');
            }
          };

          const validateFile = (file) => {
            if (!file) {
              return { valid: false, reason: '' };
            }

            if (allowed.length) {
              const extension = file.name && file.name.includes('.')
                ? `.${file.name.split('.').pop().toLowerCase()}`
                : '';
              const mimeType = (file.type || '').toLowerCase();
              const matchesExtension = extension && allowed.includes(extension);
              const matchesMime = mimeType && allowed.includes(mimeType);
              if (!matchesExtension && !matchesMime) {
                return {
                  valid: false,
                  reason: `File must be one of: ${allowed.join(', ')}`,
                };
              }
            }

            if (maxSizeBytes && file.size > maxSizeBytes) {
              return {
                valid: false,
                reason: `File must be smaller than ${maxSizeMb} MB`,
              };
            }

            return { valid: true };
          };

          const formatSize = (bytes) => {
            if (!bytes || Number.isNaN(bytes)) {
              return '';
            }
            if (bytes < 1024) {
              return `${bytes} B`;
            }
            const kb = bytes / 1024;
            if (kb < 1024) {
              return `${kb.toFixed(1)} KB`;
            }
            const mb = kb / 1024;
            return `${mb.toFixed(1)} MB`;
          };

          const updateFromInput = () => {
            const file = input && input.files ? input.files[0] : undefined;
            if (!file) {
              if (zone) {
                zone.classList.remove('has-file');
              }
              clearError();
              setFeedback(defaultFeedback);
              return;
            }

            const { valid, reason } = validateFile(file);
            if (!valid) {
              if (input) {
                input.value = '';
              }
              showError(reason || 'Invalid file.');
              return;
            }

            clearError();
            if (zone) {
              zone.classList.add('has-file');
            }
            const sizeLabel = formatSize(file.size);
            setFeedback(sizeLabel ? `${file.name} (${sizeLabel})` : file.name);
          };

          if (input) {
            input.addEventListener('change', () => {
              updateFromInput();
            });
          }

          const dragState = { counter: 0 };

          const onDragEnter = (event) => {
            if (!event) return;
            event.preventDefault();
            dragState.counter += 1;
            zone.classList.add('is-dragover');
          };

          const onDragOver = (event) => {
            if (!event) return;
            event.preventDefault();
            if (event.dataTransfer) {
              event.dataTransfer.dropEffect = 'copy';
            }
            zone.classList.add('is-dragover');
          };

          const onDragLeave = (event) => {
            if (!event) return;
            dragState.counter = Math.max(0, dragState.counter - 1);
            if (dragState.counter === 0) {
              zone.classList.remove('is-dragover');
            }
          };

          const onDrop = (event) => {
            if (!event) return;
            event.preventDefault();
            dragState.counter = 0;
            zone.classList.remove('is-dragover');
            if (!input) {
              return;
            }
            const files = event.dataTransfer && event.dataTransfer.files;
            if (!files || !files.length) {
              return;
            }
            const file = files[0];
            const { valid, reason } = validateFile(file);
            if (!valid) {
              showError(reason || 'Invalid file.');
              return;
            }
            clearError();
            let assigned = false;
            if (typeof DataTransfer !== 'undefined') {
              try {
                const dataTransfer = new DataTransfer();
                dataTransfer.items.add(file);
                input.files = dataTransfer.files;
                assigned = true;
              } catch (assignError) {
                console.warn('DataTransfer assignment failed:', assignError);
              }
            }
            if (!assigned) {
              try {
                input.files = event.dataTransfer.files;
                assigned = true;
              } catch (fallbackError) {
                console.warn('FileList assignment failed:', fallbackError);
              }
            }
            if (!assigned) {
              showError('Unable to use dropped file. Please use browse instead.');
              return;
            }
            const changeEvent = new Event('change', { bubbles: true });
            input.dispatchEvent(changeEvent);
          };

          zone.addEventListener('dragenter', onDragEnter);
          zone.addEventListener('dragover', onDragOver);
          zone.addEventListener('dragleave', onDragLeave);
          zone.addEventListener('dragend', onDragLeave);
          zone.addEventListener('drop', onDrop);

          updateFromInput();
        });
        return true;
      } catch (error) {
        console.warn('Dropzone init failed:', error);
        return false;
      }
    }

    window.initDropzones = initDropzones;

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

    function filterBySection(list, name) {
      return (Array.isArray(list) ? list : []).filter((item) => item && item.section === name);
    }

    const numberFormatter = new Intl.NumberFormat('en-US', { maximumFractionDigits: 3 });

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
        const rawValue = item ? item.value ?? item.measured ?? null : null;
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
        const rawValue = item ? item.value ?? item.measured ?? null : null;
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

    // ==== RDE UI: required ready hook (must match test literal) ====
    document.addEventListener("rde:payload-ready", () => {
      try {
        const payload = getResultsPayload();
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

        populateExportForms(payload);
      } catch (error) {
        console.warn('Payload render failed:', error);
        return false;
      }
      return true;
    });

    // ==== RDE UI: required HTMX swap hook (must match test literal) ====
    document.addEventListener("htmx:afterSwap", (event) => {
      try {
        initDropzones(event && event.target ? event.target : document);
      } catch (error) {
        console.warn('htmx swap handler failed:', error);
      }
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
      initDropzones(document);
      populateExportForms(getResultsPayload());
    });
  })();
}
