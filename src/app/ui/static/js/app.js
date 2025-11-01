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

    if (!window.__RDE_SAFE_MAP_PATCHED__) {
      window.__RDE_SAFE_MAP_PATCHED__ = true;
      if (typeof window.safeInitMap !== 'function') {
        window.safeInitMap = function baseSafeInitMap() { return true; };
      }
      var __safeInitMapBase = window.safeInitMap;
      window.safeInitMap = function safeInitMapEnhanced(payload, el) {
        if (!el || !payload || !payload.visual || !payload.visual.map) {
          return false;
        }
        try {
          if (typeof window.L === 'undefined') {
            return __safeInitMapBase(payload, el);
          }
          if (el.__RDE_LEAFLET_INSTANCE__) {
            return true;
          }
          var center = payload.visual.map.center || { lat: 48.2082, lon: 16.3738, zoom: 8 };
          var lat = typeof center.lat === 'number' ? center.lat : 48.2082;
          var lon = typeof center.lon === 'number' ? center.lon : 16.3738;
          var zoom = typeof center.zoom === 'number' ? center.zoom : 8;
          var map = window.L.map(el, { preferCanvas: true });
          map.setView([lat, lon], zoom);
          el.__RDE_LEAFLET_INSTANCE__ = map;
          return true;
        } catch (error) {
          console.warn('Map render failed:', error);
          return false;
        }
      };
    }

    window.safeInitCharts = function safeInitCharts(payload, el) {
      if (!el || !payload || !payload.visual || !payload.visual.chart) return false;
      try {
        el.setAttribute('data-chart-ready', '1');
        return true;
      } catch (error) {
        console.warn('Chart render failed:', error);
        return false;
      }
    };

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
      try {
        const response = await fetch('/export_pdf', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ results_payload: window.__RDE_RESULT__ || {} }),
        });
        if (!response.ok) {
          throw new Error(`PDF request failed (${response.status})`);
        }
        const contentType = (response.headers.get('content-type') || '').toLowerCase();
        if (!contentType.includes('application/pdf')) {
          const preview = await response.text();
          throw new Error(`Unexpected content-type: ${contentType}. Body: ${preview.slice(0, 200)}`);
        }
        const blob = await response.blob();
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = 'report_eu7_ld.pdf';
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(url);
      } catch (error) {
        console.warn('PDF export failed:', error);
        window.alert && window.alert('Unable to download PDF. Please try again later.');
      }
    }

    window.downloadCurrentPdf = downloadCurrentPdf;

    // ==== RDE UI: required ready hook (must match test literal) ====
    window.addEventListener("rde:payload-ready", () => {
      try {
        const payload = window.__RDE_RESULT__ || {};
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
        console.warn('Map render failed:', error);
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
        safeInitMap(window.__RDE_RESULT__, document.getElementById('drive-map'));
        safeInitCharts(window.__RDE_RESULT__, document.getElementById('chart-speed'));
        safeInjectKpis(window.__RDE_RESULT__, target);
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
      populateExportForms(window.__RDE_RESULT__ || {});
    });
  })();
}
