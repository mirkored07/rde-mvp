// Singleton slots – do NOT use `let/const` twice across reloads
window.__leafletMap = window.__leafletMap || null;
window.__leafletTile = window.__leafletTile || null;
window.__rdeMapInitScheduled = window.__rdeMapInitScheduled || false;

(function () {
  const THEME_KEY = "rde-theme";
  const MAPPING_STORAGE_KEY = "rde-mapping-state";
  function applyTheme(theme) {
    const root = document.documentElement;
    if (theme === "dark") {
      root.classList.add("dark");
    } else {
      root.classList.remove("dark");
    }
    root.setAttribute("data-theme", theme);
  }

  function updateToggle(theme) {
    document.querySelectorAll("[data-theme-icon]").forEach((icon) => {
      const target = icon.getAttribute("data-theme-icon");
      if (target === theme) {
        icon.classList.remove("hidden");
      } else {
        icon.classList.add("hidden");
      }
    });
    const label = document.querySelector("[data-theme-toggle-label]");
    if (label) {
      label.textContent = theme === "dark" ? "Light mode" : "Dark mode";
    }
  }

  function preferredTheme() {
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }

  function initTheme() {
    const stored = window.localStorage.getItem(THEME_KEY);
    const theme = stored === "dark" || stored === "light" ? stored : preferredTheme();
    applyTheme(theme);
    updateToggle(theme);
  }

  function toggleTheme() {
    const current = document.documentElement.classList.contains("dark") ? "dark" : "light";
    const next = current === "dark" ? "light" : "dark";
    window.localStorage.setItem(THEME_KEY, next);
    applyTheme(next);
    updateToggle(next);
    document.dispatchEvent(new CustomEvent("themechange", { detail: { theme: next } }));
  }

  function initThemeToggle() {
    const trigger = document.querySelector("[data-theme-toggle]");
    if (!trigger) return;
    trigger.addEventListener("click", toggleTheme);
  }


  function parseJSONScript(id) {
  const element = document.getElementById(id);
  if (!element) return null;
  try {
    return JSON.parse(element.textContent || "{}");
  } catch (error) {
    return null;
  }
}

  function sanitizeMappingState(raw, datasets) {
  const state = {};
  if (!raw || typeof raw !== "object") return state;
  datasets.forEach((schema) => {
    if (!schema || typeof schema !== "object") return;
    const key = schema.key;
    const entry = raw[key];
    if (!entry || typeof entry !== "object") return;
    const allowed = new Set([...(schema.required || []), ...(schema.optional || [])]);
    const sanitized = { columns: {}, units: {} };
    if (entry.columns && typeof entry.columns === "object") {
      Object.entries(entry.columns).forEach(([canonical, columnName]) => {
        if (!allowed.has(canonical)) return;
        if (typeof columnName !== "string") return;
        const trimmed = columnName.trim();
        if (trimmed) {
          sanitized.columns[canonical] = trimmed;
        }
      });
    }
    if (entry.units && typeof entry.units === "object") {
      Object.entries(entry.units).forEach(([canonical, unit]) => {
        if (!allowed.has(canonical)) return;
        if (typeof unit !== "string") return;
        const trimmed = unit.trim();
        if (trimmed) {
          sanitized.units[canonical] = trimmed;
        }
      });
    }
    if (Object.keys(sanitized.columns).length || Object.keys(sanitized.units).length) {
      state[key] = sanitized;
    }
  });
  return state;
}

  function serialiseMappingState(state) {
  const payload = {};
  Object.entries(state).forEach(([dataset, entry]) => {
    if (!entry || typeof entry !== "object") return;
    const result = {};
    if (entry.columns && typeof entry.columns === "object" && Object.keys(entry.columns).length) {
      result.columns = { ...entry.columns };
    }
    if (entry.units && typeof entry.units === "object" && Object.keys(entry.units).length) {
      result.units = { ...entry.units };
    }
    if (Object.keys(result).length) {
      payload[dataset] = result;
    }
  });
  return payload;
}

  function initMappingAssistant() {
  const container = document.querySelector('[data-mapping-assistant]');
  if (!container) return;

  const canonicalData = parseJSONScript('canonical-schema');
  const unitHintsData = parseJSONScript('unit-hints') || {};
  const datasets = Array.isArray(canonicalData?.datasets) ? canonicalData.datasets : [];
  if (!datasets.length) {
    const placeholder = container.querySelector('[data-mapping-placeholder]');
    if (placeholder) {
      placeholder.textContent = 'Mapping assistant is unavailable. Refresh to try again.';
    }
    return;
  }

  const hiddenInput = document.querySelector('[data-mapping-payload]');
  if (!hiddenInput) {
    return;
  }

  const tabs = container.querySelector('[data-mapping-tabs]');
  const panels = container.querySelector('[data-mapping-panels]');
  if (!tabs || !panels) return;

  function loadStoredState() {
    try {
      const raw = window.localStorage.getItem(MAPPING_STORAGE_KEY);
      if (!raw) return {};
      const parsed = JSON.parse(raw);
      return typeof parsed === 'object' && parsed ? parsed : {};
    } catch (error) {
      return {};
    }
  }

  let mappingState = sanitizeMappingState(loadStoredState(), datasets);

  function serialisedState() {
    return serialiseMappingState(mappingState);
  }

  function updateHiddenField() {
    hiddenInput.value = JSON.stringify(serialisedState());
  }

  function showFeedback(message, tone = 'info') {
    const feedback = container.querySelector('[data-mapping-feedback]');
    if (!feedback) return;
    feedback.classList.remove('hidden', 'text-emerald-600', 'text-rose-500');
    feedback.classList.add('text-slate-500');
    if (tone === 'success') {
      feedback.classList.add('text-emerald-600');
    } else if (tone === 'error') {
      feedback.classList.add('text-rose-500');
    }
    feedback.textContent = message;
  }

  function clearFeedback() {
    const feedback = container.querySelector('[data-mapping-feedback]');
    if (!feedback) return;
    feedback.classList.add('hidden');
    feedback.textContent = '';
  }

  function updateEntry(dataset, column, value, kind) {
    const current = mappingState[dataset] || { columns: {}, units: {} };
    if (kind === 'unit') {
      if (value) {
        current.units[column] = value;
      } else {
        delete current.units[column];
      }
    } else if (value) {
      current.columns[column] = value;
    } else {
      delete current.columns[column];
    }
    if (Object.keys(current.columns).length || Object.keys(current.units).length) {
      mappingState[dataset] = current;
    } else {
      delete mappingState[dataset];
    }
    updateHiddenField();
  }

  function applyStateToInputs() {
    const serialised = serialisedState();
    container.querySelectorAll('[data-mapping-input="column"]').forEach((input) => {
      const dataset = input.getAttribute('data-dataset');
      const column = input.getAttribute('data-column');
      const value = serialised[dataset]?.columns?.[column] || '';
      input.value = value;
    });
    container.querySelectorAll('[data-mapping-input="unit"]').forEach((input) => {
      const dataset = input.getAttribute('data-dataset');
      const column = input.getAttribute('data-column');
      const value = serialised[dataset]?.units?.[column] || '';
      input.value = value;
    });
  }

  tabs.innerHTML = '';
  panels.innerHTML = '';

  const tabButtons = [];
  const panelElements = [];

  function createRow(schemaKey, column, required) {
    const row = document.createElement('div');
    row.className = 'rounded-2xl border border-slate-200 bg-white/70 p-4 shadow-sm dark:border-slate-700 dark:bg-slate-900/40';

    const labelRow = document.createElement('div');
    labelRow.className = 'flex items-center justify-between gap-2';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'text-sm font-semibold text-slate-900 dark:text-white';
    nameSpan.textContent = column;
    labelRow.appendChild(nameSpan);
    if (required) {
      const badge = document.createElement('span');
      badge.className = 'inline-flex items-center rounded-full bg-sky-500/10 px-2 py-0.5 text-xs font-semibold text-sky-600 dark:bg-indigo-500/20 dark:text-indigo-200';
      badge.textContent = 'Required';
      labelRow.appendChild(badge);
    }
    row.appendChild(labelRow);

    const inputWrapper = document.createElement('div');
    const hasUnit = Object.prototype.hasOwnProperty.call(unitHintsData, column);
    inputWrapper.className = hasUnit
      ? 'mt-3 grid gap-3 sm:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]'
      : 'mt-3 grid gap-3';

    const columnInput = document.createElement('input');
    columnInput.type = 'text';
    columnInput.className = 'w-full rounded-full border border-slate-200 bg-white px-4 py-2 text-sm text-slate-600 shadow-sm focus:border-sky-500 focus:outline-none focus:ring-2 focus:ring-sky-500 focus:ring-offset-2 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200';
    columnInput.placeholder = 'CSV column name';
    columnInput.autocomplete = 'off';
    columnInput.setAttribute('data-mapping-input', 'column');
    columnInput.setAttribute('data-dataset', schemaKey);
    columnInput.setAttribute('data-column', column);
    columnInput.addEventListener('input', (event) => {
      clearFeedback();
      updateEntry(schemaKey, column, event.target.value.trim(), 'column');
    });
    inputWrapper.appendChild(columnInput);

    if (hasUnit) {
      const unitInput = document.createElement('input');
      unitInput.type = 'text';
      unitInput.className = 'w-full rounded-full border border-slate-200 bg-white px-4 py-2 text-sm text-slate-600 shadow-sm focus:border-sky-500 focus:outline-none focus:ring-2 focus:ring-sky-500 focus:ring-offset-2 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200';
      unitInput.placeholder = unitHintsData[column] || 'Units';
      unitInput.autocomplete = 'off';
      unitInput.setAttribute('data-mapping-input', 'unit');
      unitInput.setAttribute('data-dataset', schemaKey);
      unitInput.setAttribute('data-column', column);
      unitInput.addEventListener('input', (event) => {
        clearFeedback();
        updateEntry(schemaKey, column, event.target.value.trim(), 'unit');
      });
      inputWrapper.appendChild(unitInput);
    }

    row.appendChild(inputWrapper);
    return row;
  }

  function buildPanel(schema) {
    const panel = document.createElement('div');
    panel.className = 'mt-4 space-y-4';
    panel.setAttribute('data-mapping-panel', schema.key);

    const requiredGroup = document.createElement('div');
    requiredGroup.className = 'space-y-3';
    if (schema.required && schema.required.length) {
      const heading = document.createElement('h4');
      heading.className = 'text-xs font-semibold uppercase tracking-[0.3em] text-slate-400 dark:text-slate-500';
      heading.textContent = 'Required columns';
      requiredGroup.appendChild(heading);
      schema.required.forEach((column) => {
        requiredGroup.appendChild(createRow(schema.key, column, true));
      });
      panel.appendChild(requiredGroup);
    }

    if (schema.optional && schema.optional.length) {
      const optionalGroup = document.createElement('div');
      optionalGroup.className = 'space-y-3';
      const heading = document.createElement('h4');
      heading.className = 'text-xs font-semibold uppercase tracking-[0.3em] text-slate-400 dark:text-slate-500';
      heading.textContent = 'Optional columns';
      optionalGroup.appendChild(heading);
      schema.optional.forEach((column) => {
        optionalGroup.appendChild(createRow(schema.key, column, false));
      });
      panel.appendChild(optionalGroup);
    }

    if (!panel.children.length) {
      const empty = document.createElement('p');
      empty.className = 'text-sm text-slate-500 dark:text-slate-400';
      empty.textContent = 'No canonical columns defined for this dataset yet.';
      panel.appendChild(empty);
    }

    return panel;
  }

  datasets.forEach((schema, index) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-600 shadow-sm transition hover:border-sky-400 hover:text-sky-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 focus-visible:ring-offset-2 dark:border-slate-700 dark:bg-slate-950/40 dark:text-slate-200 dark:hover:border-indigo-400 dark:hover:text-indigo-200';
    button.setAttribute('data-dataset', schema.key);
    button.textContent = schema.label || schema.key.toUpperCase();
    tabs.appendChild(button);
    tabButtons.push(button);

    const panel = buildPanel(schema);
    if (index !== 0) {
      panel.classList.add('hidden');
    } else {
      button.classList.add('bg-sky-500/10', 'border-sky-400', 'text-sky-600', 'dark:border-indigo-400', 'dark:text-indigo-200');
    }
    panels.appendChild(panel);
    panelElements.push(panel);
  });

  function activate(datasetKey) {
    tabButtons.forEach((button) => {
      const isActive = button.getAttribute('data-dataset') === datasetKey;
      button.classList.toggle('bg-sky-500/10', isActive);
      button.classList.toggle('border-sky-400', isActive);
      button.classList.toggle('text-sky-600', isActive);
      button.classList.toggle('dark:border-indigo-400', isActive);
      button.classList.toggle('dark:text-indigo-200', isActive);
    });
    panelElements.forEach((panel) => {
      const match = panel.getAttribute('data-mapping-panel') === datasetKey;
      panel.classList.toggle('hidden', !match);
    });
  }

  tabButtons.forEach((button) => {
    button.addEventListener('click', () => {
      activate(button.getAttribute('data-dataset'));
    });
  });

  if (tabButtons.length) {
    activate(tabButtons[0].getAttribute('data-dataset'));
  }

  applyStateToInputs();
  updateHiddenField();

  async function refreshServerProfiles(preserve) {
    const select = container.querySelector('[data-mapping-profile]');
    if (!select) return;
    const previous = preserve || select.value;
    try {
      const response = await fetch('/mapping_profiles');
      if (!response.ok) {
        throw new Error('Failed to fetch profiles');
      }
      const data = await response.json();
      const profiles = Array.isArray(data?.profiles) ? data.profiles : [];
      select.innerHTML = '';
      const defaultOption = document.createElement('option');
      defaultOption.value = '';
      defaultOption.textContent = 'Choose a saved profile…';
      select.appendChild(defaultOption);
      profiles.forEach((profile) => {
        if (!profile || typeof profile.slug !== 'string') return;
        const option = document.createElement('option');
        option.value = profile.slug;
        option.textContent = profile.name || profile.slug;
        if (profile.slug === previous) {
          option.selected = true;
        }
        select.appendChild(option);
      });
    } catch (error) {
      showFeedback('Unable to load server profiles right now.', 'error');
    }
  }

  const saveLocalButton = container.querySelector('[data-mapping-save]');
  if (saveLocalButton) {
    saveLocalButton.addEventListener('click', () => {
      try {
        window.localStorage.setItem(MAPPING_STORAGE_KEY, JSON.stringify(serialisedState()));
        showFeedback('Mapping saved to this browser.', 'success');
      } catch (error) {
        showFeedback('Unable to save mapping locally.', 'error');
      }
    });
  }

  const resetButton = container.querySelector('[data-mapping-reset]');
  if (resetButton) {
    resetButton.addEventListener('click', () => {
      mappingState = {};
      container.querySelectorAll('[data-mapping-input="column"], [data-mapping-input="unit"]').forEach((input) => {
        input.value = '';
      });
      window.localStorage.removeItem(MAPPING_STORAGE_KEY);
      updateHiddenField();
      showFeedback('Mapping cleared.', 'success');
    });
  }

  const loadButton = container.querySelector('[data-mapping-load]');
  if (loadButton) {
    loadButton.addEventListener('click', async () => {
      const select = container.querySelector('[data-mapping-profile]');
      const slug = select ? select.value : '';
      if (!slug) {
        showFeedback('Select a profile to load.', 'error');
        return;
      }
      try {
        clearFeedback();
        const response = await fetch(`/mapping_profiles/${encodeURIComponent(slug)}`);
        if (!response.ok) {
          throw new Error('Failed to load profile');
        }
        const data = await response.json();
        mappingState = sanitizeMappingState(data.mapping || {}, datasets);
        applyStateToInputs();
        updateHiddenField();
        showFeedback(`Loaded mapping profile "${data.name || slug}".`, 'success');
      } catch (error) {
        showFeedback('Unable to load mapping profile.', 'error');
      }
    });
  }

  const saveServerButton = container.querySelector('[data-mapping-save-server]');
  if (saveServerButton) {
    saveServerButton.addEventListener('click', async () => {
      clearFeedback();
      const payload = serialisedState();
      if (!Object.keys(payload).length) {
        showFeedback('Map at least one column before saving.', 'error');
        return;
      }
      const name = window.prompt('Name this mapping profile:');
      if (!name) {
        showFeedback('Profile name is required to save.', 'error');
        return;
      }
      try {
        const response = await fetch('/mapping_profiles', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, mapping: payload }),
        });
        if (!response.ok) {
          const detail = await response.json().catch(() => ({}));
          const message = typeof detail.detail === 'string' ? detail.detail : 'Unable to save mapping profile.';
          showFeedback(message, 'error');
          return;
        }
        const data = await response.json();
        showFeedback(`Saved mapping profile "${data.name}".`, 'success');
        refreshServerProfiles(data.slug);
      } catch (error) {
        showFeedback('Unable to save mapping profile.', 'error');
      }
    });
  }

  refreshServerProfiles();
  applyStateToInputs();
  updateHiddenField();
}

  function allowedExtensions(input) {
    return (input.dataset.allowed || "")
      .split(",")
      .map((value) => value.trim().toLowerCase())
      .filter(Boolean);
  }

  function displayError(zone, errorEl, message) {
    zone.classList.add("has-error");
    zone.classList.remove("has-file");
    if (errorEl) {
      errorEl.textContent = message;
      errorEl.classList.remove("hidden");
    }
  }

  function clearError(zone, errorEl) {
    zone.classList.remove("has-error");
    if (errorEl) {
      errorEl.textContent = "";
      errorEl.classList.add("hidden");
    }
  }

  function validateInput(input) {
    const zone = input.closest("[data-dropzone]");
    if (!zone) return true;
    const errorEl = zone.querySelector("[data-error]");
    const labelEl = zone.querySelector("[data-file-label]");

    clearError(zone, errorEl);

    const files = input.files;
    if (!files || !files.length) {
      if (labelEl) {
        labelEl.textContent = "No file selected";
      }
      zone.classList.remove("has-file");
      return false;
    }

    const file = files[0];
    const allowed = allowedExtensions(input);
    const fileExt = file.name.includes(".") ? `.${file.name.split(".").pop().toLowerCase()}` : "";

    if (allowed.length && !allowed.includes(fileExt)) {
      displayError(zone, errorEl, `Unsupported file type. Choose ${allowed.join(", ")}.`);
      input.value = "";
      if (labelEl) {
        labelEl.textContent = "No file selected";
      }
      return false;
    }

    const maxSize = parseInt(input.dataset.maxSizeMb || "0", 10);
    if (maxSize && file.size > maxSize * 1024 * 1024) {
      displayError(zone, errorEl, `File is larger than ${maxSize} MB.`);
      input.value = "";
      if (labelEl) {
        labelEl.textContent = "No file selected";
      }
      return false;
    }

    if (labelEl) {
      labelEl.textContent = file.name;
    }
    zone.classList.add("has-file");
    return true;
  }

  function initDropzones(root) {
    root.querySelectorAll("[data-dropzone]").forEach((zone) => {
      const input = zone.querySelector("input[type=file]");
      if (!input) return;

      ["dragenter", "dragover"].forEach((eventName) => {
        zone.addEventListener(eventName, (event) => {
          event.preventDefault();
          zone.classList.add("is-dragover");
        });
      });

      ["dragleave", "dragend", "drop"].forEach((eventName) => {
        zone.addEventListener(eventName, () => {
          zone.classList.remove("is-dragover");
        });
      });

      zone.addEventListener("drop", (event) => {
        event.preventDefault();
        if (event.dataTransfer && event.dataTransfer.files.length) {
          input.files = event.dataTransfer.files;
          input.dispatchEvent(new Event("change", { bubbles: true }));
        }
      });

      input.addEventListener("change", () => {
        validateInput(input);
      });
    });
  }

  function initFormValidation(form) {
    form.addEventListener("submit", (event) => {
      const inputs = form.querySelectorAll("input[type=file][required]");
      let valid = true;
      inputs.forEach((input) => {
        if (!validateInput(input)) {
          valid = false;
        }
      });
      if (!valid) {
        event.preventDefault();
        event.stopPropagation();
      }
    });
  }

  function renderSummary(container) {
    const target = container.querySelector("#analysis-summary");
    const dataEl = container.querySelector("#summary-data");
    if (!target || !dataEl) return;

    let markdown = "";
    try {
      markdown = JSON.parse(dataEl.textContent || '""');
    } catch (error) {
      markdown = dataEl.textContent || "";
    }

    if (markdown && window.marked && typeof window.marked.parse === "function") {
      target.innerHTML = window.marked.parse(markdown);
    } else {
      target.textContent = markdown;
    }
  }
  function populateExportForms(payload) {
    let serialised = "";
    if (payload) {
      try {
        serialised = JSON.stringify(payload);
      } catch (error) {
        serialised = "";
      }
    }
    const zipField = document.getElementById("exportZipPayload");
    if (zipField) {
      zipField.value = serialised;
    }
    const pdfField = document.getElementById("exportPdfPayload");
    if (pdfField) {
      pdfField.value = serialised;
    }
  }

  function initializeResults(root) {
    const container = root.querySelector("[data-component='analysis-results']");
    if (!container) return;
    renderSummary(container);
  }

  function safeInit() {
    const payload = getResultPayload();
    populateExportForms(payload);
    if (!payload) {
      console.info("RDE: analysis payload unavailable; skipping charts and map.");
      return;
    }

    const chartsOk = renderChartsFromPayload(payload);
    const kpiOk = injectKpisFromPayload(payload);

    if (!chartsOk) {
      console.warn("RDE: chart render skipped or partial");
    }

    if (!kpiOk) {
      console.info("RDE: KPI injection skipped; nothing to update.");
    }

    safeInitMap(payload);
  }

  function handleDomReady() {
    initTheme();
    initThemeToggle();
    initDropzones(document);
    initMappingAssistant();
    const form = document.getElementById("telemetry-form");
    if (form) {
      initFormValidation(form);
    }
    initializeResults(document);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", handleDomReady);
  } else {
    handleDomReady();
  }

  document.addEventListener("DOMContentLoaded", safeInit);
  if (document.readyState !== "loading") {
    safeInit();
  }

  window.addEventListener("rde:payload-ready", safeInit);

  document.addEventListener("htmx:afterSwap", (event) => {
    if (event.target && event.target.id === "analysis-results") {
      initializeResults(event.target);
      safeInit();
    }
  });
})();

function getResultPayload() {
  const payload = window.__RDE_RESULT__;
  if (!payload || typeof payload !== "object") {
    return null;
  }
  return payload;
}

function parseNumeric(value) {
  if (value == null) return null;
  const match = String(value).match(/[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function findPollutantSeries(payload, key) {
  const chart = (payload.analysis && payload.analysis.chart) || payload.chart || {};
  const pollutants = Array.isArray(chart.pollutants) ? chart.pollutants : [];
  const entry = pollutants.find((item) => item && item.key === key);
  if (!entry || typeof entry !== "object") {
    return { values: [], times: null };
  }
  const values = Array.isArray(entry.y)
    ? entry.y
    : Array.isArray(entry.values)
    ? entry.values
    : [];
  const times = Array.isArray(entry.t) ? entry.t : chart.times || null;
  return { values, times };
}

function getSpeedSeries(payload) {
  const chart = (payload.analysis && payload.analysis.chart) || payload.chart || {};
  const speed = chart.speed || {};
  const values = Array.isArray(speed.values)
    ? speed.values
    : Array.isArray(speed.y)
    ? speed.y
    : [];
  const times = Array.isArray(chart.times) ? chart.times : Array.isArray(speed.t) ? speed.t : null;
  return { values, times };
}

function formatKpiValue(value, unit) {
  if (!Number.isFinite(value)) return "n/a";
  const abs = Math.abs(value);
  const formatted = abs >= 1000 ? value.toExponential(3) : value.toFixed(3);
  return unit ? `${formatted} ${unit}` : formatted;
}

function setKpiText(key, scope, text) {
  if (!key || typeof text !== "string") return false;
  let selector = `[data-kpi="${key}"]`;
  if (scope === null) {
    selector = `${selector}:not([data-kpi-scope])`;
  } else if (typeof scope === "string") {
    selector = `${selector}[data-kpi-scope="${scope}"]`;
  }

  const nodes = document.querySelectorAll(selector);
  if (!nodes.length) return false;
  nodes.forEach((node) => {
    node.textContent = text;
  });
  return true;
}

function injectKpisFromPayload(payload) {
  if (!payload || typeof payload !== "object") return false;
  const analysis = payload.analysis;
  if (!analysis || typeof analysis !== "object") return false;

  const kpis = analysis.kpis && typeof analysis.kpis === "object" ? analysis.kpis : {};
  const bins = Array.isArray(analysis.bins) ? analysis.bins : [];
  const unitCache = new Map();

  const getUnit = (key) => {
    if (unitCache.has(key)) return unitCache.get(key);
    const entry = kpis[key];
    const unit = entry && typeof entry === "object" ? entry.unit : null;
    unitCache.set(key, unit || null);
    return unit || null;
  };

  let updated = false;

  Object.entries(kpis).forEach(([key, entry]) => {
    if (!entry || typeof entry !== "object") return;
    const unit = getUnit(key);
    const totalBlock = entry.total && typeof entry.total === "object" ? entry.total : null;
    const directTotal = totalBlock && totalBlock.value != null ? parseNumeric(totalBlock.value) : null;
    const fallbackTotal = entry.value != null ? parseNumeric(entry.value) : null;
    const totalValue = directTotal ?? fallbackTotal;
    if (totalValue != null) {
      const text = formatKpiValue(totalValue, unit);
      if (setKpiText(key, null, text)) updated = true;
      if (setKpiText(key, "total", text)) updated = true;
    }

    Object.entries(entry).forEach(([scope, scopeEntry]) => {
      if (scope === "label" || scope === "unit" || scope === "total" || scope === "value") return;
      if (!scopeEntry || typeof scopeEntry !== "object") return;
      const numeric = scopeEntry.value != null ? parseNumeric(scopeEntry.value) : null;
      if (numeric == null) return;
      const text = formatKpiValue(numeric, unit);
      if (setKpiText(key, scope, text)) updated = true;
    });
  });

  bins.forEach((bin) => {
    if (!bin || typeof bin !== "object") return;
    const scope = bin.name;
    if (!scope) return;
    const binKpis = bin.kpis && typeof bin.kpis === "object" ? bin.kpis : {};
    Object.entries(binKpis).forEach(([key, rawValue]) => {
      const numeric = parseNumeric(rawValue);
      if (numeric == null) return;
      const unit = getUnit(key);
      const text = formatKpiValue(numeric, unit);
      if (setKpiText(key, scope, text)) updated = true;
    });
  });

  return updated;
}

function renderChartsFromPayload(payload) {
  if (!payload || typeof payload !== "object") return false;
  if (typeof Plotly === "undefined" || typeof Plotly.newPlot !== "function") {
    console.warn("RDE: Plotly unavailable; charts skipped.");
    return false;
  }

  const { values: speedValuesRaw, times: speedTimes } = getSpeedSeries(payload);
  const speedEl = document.getElementById("chart-speed");

  const sanitizeSeries = (values) => {
    if (!Array.isArray(values)) return [];
    return values.map((value) => {
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : null;
    });
  };

  const alignTimes = (candidate, fallback, length) => {
    if (Array.isArray(candidate) && candidate.length === length) {
      return candidate;
    }
    if (Array.isArray(fallback) && fallback.length === length) {
      return fallback;
    }
    return Array.from({ length }, (_, index) => index);
  };

  let plotted = false;

  const speedSeries = sanitizeSeries(speedValuesRaw);
  if (speedEl && speedSeries.length && speedSeries.some((value) => value != null)) {
    const x = alignTimes(speedTimes, null, speedSeries.length);
    Plotly.newPlot(
      speedEl,
      [
        {
          x,
          y: speedSeries,
          name: "Vehicle speed",
          mode: "lines",
          line: { color: "#38bdf8" },
        },
      ],
      {
        margin: { t: 24, r: 16, b: 48, l: 64 },
        xaxis: { title: "Time" },
        yaxis: { title: "m/s" },
        paper_bgcolor: "transparent",
        plot_bgcolor: "transparent",
      },
      { displaylogo: false, responsive: true },
    );
    plotted = true;
  }

  const pollutantCharts = [
    { key: "NOx", elementId: "chart-nox", label: "NOx emission rate", unit: "mg/s", color: "#f97316" },
    { key: "PN", elementId: "chart-pn", label: "PN emission rate", unit: "1/s", color: "#8b5cf6" },
    { key: "PM", elementId: "chart-pm", label: "PM emission rate", unit: "mg/s", color: "#14b8a6" },
  ];

  pollutantCharts.forEach(({ key, elementId, label, unit, color }) => {
    const target = document.getElementById(elementId);
    if (!target) return;
    const series = findPollutantSeries(payload, key);
    const values = sanitizeSeries(series.values);
    if (!values.length || !values.some((value) => value != null)) return;
    const x = alignTimes(series.times, speedTimes, values.length);
    Plotly.newPlot(
      target,
      [
        {
          x,
          y: values,
          name: label,
          mode: "lines",
          line: { color },
        },
      ],
      {
        margin: { t: 24, r: 16, b: 48, l: 64 },
        xaxis: { title: "Time" },
        yaxis: { title: unit },
        paper_bgcolor: "transparent",
        plot_bgcolor: "transparent",
      },
      { displaylogo: false, responsive: true },
    );
    plotted = true;
  });

  return plotted;
}

// --- Map lifecycle guards ---
function getPayload() {
  return typeof window !== "undefined" &&
    window.__RDE_RESULT__ &&
    window.__RDE_RESULT__.visual &&
    window.__RDE_RESULT__.visual.map
    ? window.__RDE_RESULT__
    : null;
}

function getMapContainer() {
  return document.getElementById("drive-map") || null;
}

function containerIsVisible(el) {
  if (!el) return false;
  const r = el.getBoundingClientRect();
  return r.width > 0 && r.height > 0;
}

function destroyLeaflet() {
  if (window.__leafletMap && typeof window.__leafletMap.remove === "function") {
    try {
      window.__leafletMap.remove();
    } catch (_) {}
  }
  window.__leafletMap = null;
  window.__leafletTile = null;
}

function renderLeafletFromPayload(payload) {
  if (typeof window === "undefined" || typeof window.L === "undefined") {
    throw new Error("Leaflet is not available");
  }

  const mapData = payload?.visual?.map;
  const coords = Array.isArray(mapData?.coords)
    ? mapData.coords
        .map((pair) => (Array.isArray(pair) && pair.length >= 2 ? [Number(pair[0]), Number(pair[1])] : null))
        .filter((pair) => pair && Number.isFinite(pair[0]) && Number.isFinite(pair[1]))
    : [];

  const el = getMapContainer();
  if (!el) {
    throw new Error("Map container not found");
  }

  if (!window.__leafletMap) {
    window.__leafletMap = window.L.map(el, { zoomControl: true });
    window.__leafletTile = window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap",
    }).addTo(window.__leafletMap);
  } else if (window.__leafletMap._container !== el) {
    destroyLeaflet();
    window.__leafletMap = window.L.map(el, { zoomControl: true });
    window.__leafletTile = window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap",
    }).addTo(window.__leafletMap);
  }

  if (window.__leafletMap && typeof window.__leafletMap.eachLayer === "function") {
    window.__leafletMap.eachLayer((layer) => {
      if (layer && layer !== window.__leafletTile) {
        try {
          window.__leafletMap.removeLayer(layer);
        } catch (_) {}
      }
    });
  }

  if (coords.length >= 2) {
    const latlngs = coords.map(([lat, lon]) => window.L.latLng(lat, lon));
    window.L.polyline(latlngs, { weight: 3, opacity: 0.8 }).addTo(window.__leafletMap);
    window.__leafletMap.fitBounds(window.L.latLngBounds(latlngs), { padding: [20, 20] });
  } else if (coords.length === 1) {
    window.__leafletMap.setView([coords[0][0], coords[0][1]], 14);
  } else {
    window.__leafletMap.setView([47.0707, 15.4395], 11);
  }

  setTimeout(() => {
    try {
      window.__leafletMap.invalidateSize();
    } catch (_) {}
  }, 0);
}

function renderMapFromPayload(payload) {
  try {
    renderLeafletFromPayload(payload);
    return true;
  } catch (err) {
    console.warn("Map render failed:", err);
    return false;
  }
}

function tryInitMapOnceReady() {
  const el = getMapContainer();
  const payload = getPayload();

  if (!el || !containerIsVisible(el) || !payload) {
    if (!window.__rdeMapInitScheduled) {
      window.__rdeMapInitScheduled = true;
      requestAnimationFrame(() => {
        window.__rdeMapInitScheduled = false;
        tryInitMapOnceReady();
      });
    }
    return;
  }

  try {
    renderLeafletFromPayload(payload);
  } catch (err) {
    console.warn("Map render failed:", err);
  }
}

function safeInitMap() {
  tryInitMapOnceReady();
}

document.addEventListener("DOMContentLoaded", () => {
  safeInitMap(window.__RDE_RESULT__);
});

window.addEventListener("rde:payload-ready", () => {
  safeInitMap(window.__RDE_RESULT__);
});

document.addEventListener("htmx:afterSwap", (event) => {
  safeInitMap(window.__RDE_RESULT__);
});

function bindHtmxLifecycle() {
  if (!document.body) {
    return;
  }
  document.body.addEventListener("htmx:beforeSwap", () => {
    destroyLeaflet();
  });
  document.body.addEventListener("htmx:afterSwap", () => {
    tryInitMapOnceReady();
  });
}

if (document.body) {
  bindHtmxLifecycle();
} else {
  document.addEventListener("DOMContentLoaded", bindHtmxLifecycle, { once: true });
}

