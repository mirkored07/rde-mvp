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

  function applyChartTheme() {
    const chart = document.getElementById("analysis-chart");
    if (!chart || chart.dataset.chartReady !== "true" || !window.Plotly) {
      return;
    }
    const isDark = document.documentElement.classList.contains("dark");
    const color = isDark ? "#e2e8f0" : "#1e293b";
    window.Plotly.relayout(chart, {
      "font.color": color,
      "xaxis.color": color,
      "yaxis.color": color,
      "yaxis2.color": color,
      "paper_bgcolor": "rgba(0,0,0,0)",
      "plot_bgcolor": "rgba(0,0,0,0)",
    });
  }

  function renderChart(container) {
    const chart = container.querySelector("#analysis-chart");
    const empty = container.querySelector("[data-chart-empty]");
    const dataEl = container.querySelector("#chart-data");
    if (!chart || !dataEl) return;

    let config;
    try {
      config = JSON.parse(dataEl.textContent || "{}");
    } catch (error) {
      config = null;
    }

    if (!config || !config.traces || !config.traces.length) {
      if (empty) empty.classList.remove("hidden");
      chart.classList.add("hidden");
      chart.dataset.chartReady = "false";
      return;
    }

    if (empty) empty.classList.add("hidden");
    chart.classList.remove("hidden");

    if (window.Plotly) {
      window.Plotly.purge(chart);
      window.Plotly.newPlot(chart, config.traces, config.layout, {
        displayModeBar: false,
        responsive: true,
      });
      chart.dataset.chartReady = "true";
      applyChartTheme();
    }
  }

  function renderMap(container) {
    const mapEl = container.querySelector("#analysis-map");
    const empty = container.querySelector("[data-map-empty]");
    const dataEl = container.querySelector("#map-data");
    if (!mapEl || !dataEl) return;

    let payload;
    try {
      payload = JSON.parse(dataEl.textContent || "{}");
    } catch (error) {
      payload = null;
    }

    if (!payload || !payload.points || !payload.points.length || !window.L) {
      if (empty) empty.classList.remove("hidden");
      mapEl.classList.add("hidden");
      mapEl.dataset.mapReady = "false";
      if (mapEl._leafletMap) {
        mapEl._leafletMap.remove();
        mapEl._leafletMap = undefined;
      }
      return;
    }

    if (empty) empty.classList.add("hidden");
    mapEl.classList.remove("hidden");

    if (mapEl._leafletMap) {
      mapEl._leafletMap.remove();
    }

    const map = window.L.map(mapEl, { scrollWheelZoom: false });
    window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors",
    }).addTo(map);

    const latlngs = payload.points.map((point) => [point.lat, point.lon]);
    const polyline = window.L.polyline(latlngs, {
      color: "#2563eb",
      weight: 4,
      opacity: 0.85,
    }).addTo(map);

    if (latlngs.length >= 1) {
      window.L.circleMarker(latlngs[0], {
        radius: 5,
        color: "#10b981",
        fillColor: "#10b981",
        fillOpacity: 0.9,
      }).addTo(map);
    }

    if (latlngs.length >= 2) {
      window.L.circleMarker(latlngs[latlngs.length - 1], {
        radius: 5,
        color: "#f97316",
        fillColor: "#f97316",
        fillOpacity: 0.9,
      }).addTo(map);
    }

    if (payload.bounds && payload.bounds.length === 2) {
      const bounds = window.L.latLngBounds(payload.bounds);
      map.fitBounds(bounds, { padding: [24, 24] });
    } else {
      map.fitBounds(polyline.getBounds(), { padding: [24, 24] });
    }

    mapEl._leafletMap = map;
    mapEl.dataset.mapReady = "true";
  }

  function setupTabs(container) {
    const triggers = Array.from(container.querySelectorAll("[data-tab-target]"));
    const panels = Array.from(container.querySelectorAll("[data-tab-panel]"));
    if (!triggers.length) return;

    function activate(target) {
      triggers.forEach((trigger) => {
        const isActive = trigger.getAttribute("data-tab-target") === target;
        trigger.classList.toggle("is-active", isActive);
        trigger.setAttribute("aria-selected", isActive ? "true" : "false");
      });
      panels.forEach((panel) => {
        const match = panel.getAttribute("data-tab-panel") === target;
        if (match) {
          panel.removeAttribute("hidden");
        } else {
          panel.setAttribute("hidden", "true");
        }
      });
      if (target === "charts") {
        applyChartTheme();
        const chart = container.querySelector("#analysis-chart");
        if (chart && chart.dataset.chartReady === "true" && window.Plotly) {
          window.Plotly.Plots.resize(chart);
        }
      }
      if (target === "map") {
        const mapEl = container.querySelector("#analysis-map");
        if (mapEl && mapEl.dataset.mapReady === "true" && mapEl._leafletMap) {
          setTimeout(() => {
            mapEl._leafletMap.invalidateSize();
          }, 150);
        }
      }
    }

    triggers.forEach((trigger) => {
      trigger.addEventListener("click", () => {
        activate(trigger.getAttribute("data-tab-target"));
      });
    });

    const initial = triggers.find((trigger) => trigger.classList.contains("is-active"))
      || triggers[0];
    if (initial) {
      activate(initial.getAttribute("data-tab-target"));
    }
  }

  function setupExportButtons(container) {
    const pdfButton = container.querySelector("[data-download-pdf]");
    const zipButton = container.querySelector("[data-download-zip]");
    const payloadEl = container.querySelector("[data-report-payload]");
    const errorEl = container.querySelector("[data-export-error]");

    const showError = (message) => {
      if (!errorEl) return;
      if (message) {
        errorEl.textContent = message;
        errorEl.classList.remove("hidden");
      } else {
        errorEl.textContent = "";
        errorEl.classList.add("hidden");
      }
    };

    const buttons = [pdfButton, zipButton].filter(Boolean);
    if (!buttons.length) {
      if (errorEl) {
        errorEl.classList.add("hidden");
      }
      return;
    }

    if (!payloadEl) {
      buttons.forEach((btn) => {
        btn.disabled = true;
        btn.setAttribute("aria-disabled", "true");
      });
      showError("Report export is unavailable for this result.");
      return;
    }

    const parsePayload = () => {
      try {
        return JSON.parse(payloadEl.textContent || "{}");
      } catch (error) {
        showError("Unable to prepare export payload.");
        return null;
      }
    };

    const download = async (button, endpoint, filename, failureMessage) => {
      if (!button) return;
      showError("");
      const payload = parsePayload();
      if (!payload) return;

      button.disabled = true;
      button.setAttribute("aria-busy", "true");

      try {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ results: payload }),
        });

        if (!response.ok) {
          let message = failureMessage;
          try {
            const data = await response.json();
            if (data && typeof data.detail === "string") {
              message = data.detail;
            }
          } catch (parseError) {
            const fallback = await response.text();
            if (fallback) {
              message = fallback;
            }
          }
          showError(message);
          return;
        }

        const blob = await response.blob();
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
      } catch (error) {
        showError(failureMessage);
      } finally {
        button.disabled = false;
        button.removeAttribute("aria-busy");
      }
    };

    if (pdfButton) {
      pdfButton.addEventListener("click", () => {
        download(pdfButton, "/export_pdf", "analysis-report.pdf", "PDF download failed. Please try again.");
      });
    }

    if (zipButton) {
      zipButton.addEventListener("click", () => {
        download(zipButton, "/export_zip", "analysis-report.zip", "ZIP download failed. Please try again.");
      });
    }
  }

  function initializeResults(root) {
    const container = root.querySelector("[data-component='analysis-results']");
    if (!container) return;
    renderSummary(container);
    renderChart(container);
    renderMap(container);
    setupTabs(container);
    setupExportButtons(container);
  }

  document.addEventListener("DOMContentLoaded", () => {
    initTheme();
    initThemeToggle();
    initDropzones(document);
    initMappingAssistant();
    const form = document.getElementById("telemetry-form");
    if (form) {
      initFormValidation(form);
    }
    initializeResults(document);
  });

  document.addEventListener("htmx:afterSwap", (event) => {
    if (event.target && event.target.id === "analysis-results") {
      initializeResults(event.target);
    }
  });

  document.addEventListener("themechange", () => {
    applyChartTheme();
  });

  window.addEventListener("resize", () => {
    const chart = document.getElementById("analysis-chart");
    if (chart && chart.dataset.chartReady === "true" && window.Plotly) {
      window.Plotly.Plots.resize(chart);
    }
  });
})();
