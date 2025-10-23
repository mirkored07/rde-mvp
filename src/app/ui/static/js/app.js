(function () {
  const THEME_KEY = "rde-theme";

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
    const button = container.querySelector("[data-download-pdf]");
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

    if (!button) {
      if (errorEl) {
        errorEl.classList.add("hidden");
      }
      return;
    }

    if (!payloadEl) {
      button.disabled = true;
      showError("PDF export is unavailable for this result.");
      return;
    }

    button.addEventListener("click", async () => {
      showError("");
      let payload;
      try {
        payload = JSON.parse(payloadEl.textContent || "{}");
      } catch (error) {
        showError("Unable to prepare PDF payload.");
        return;
      }

      button.disabled = true;
      button.setAttribute("aria-busy", "true");

      try {
        const response = await fetch("/export_pdf", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ results: payload }),
        });

        if (!response.ok) {
          let message = "Unable to generate PDF right now.";
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
        link.download = "analysis-report.pdf";
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
      } catch (error) {
        showError("PDF download failed. Please try again.");
      } finally {
        button.disabled = false;
        button.removeAttribute("aria-busy");
      }
    });
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
