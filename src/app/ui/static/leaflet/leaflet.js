/* Lightweight Leaflet-compatible map bundle for local development.
 * Provides a subset of the Leaflet 1.9.4 API sufficient for the RDE MVP UI.
 * Supports tile layers, polylines, circle markers, and fitBounds/setView helpers.
 */
(function (global) {
  "use strict";

  const TILE_SIZE = 256;
  const MIN_ZOOM = 1;
  const MAX_ZOOM = 19;
  const DEFAULT_ZOOM = 12;
  const DEFAULT_CENTER = { lat: 0, lng: 0 };
  const SUBDOMAIN_FALLBACK = ["a", "b", "c"];

  function clampZoom(z) {
    return Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, z));
  }

  function toRad(deg) {
    return (deg * Math.PI) / 180;
  }

  function toDeg(rad) {
    return (rad * 180) / Math.PI;
  }

  function project(latlng, zoom) {
    const sin = Math.sin(toRad(latlng.lat));
    const scale = TILE_SIZE * Math.pow(2, zoom);
    const x = scale * (latlng.lng + 180) / 360;
    const y = scale * (1 - Math.log((1 + sin) / (1 - sin)) / Math.PI) / 2;
    return { x, y };
  }

  function unproject(point, zoom) {
    const scale = TILE_SIZE * Math.pow(2, zoom);
    const lng = (point.x / scale) * 360 - 180;
    const n = Math.PI - (2 * Math.PI * point.y) / scale;
    const lat = toDeg(Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))));
    return { lat, lng };
  }

  function latLngBounds(input) {
    if (!input || input.length !== 2) {
      throw new Error("latLngBounds expects [[south, west], [north, east]]");
    }
    const sw = Array.isArray(input[0]) ? { lat: input[0][0], lng: input[0][1] } : input[0];
    const ne = Array.isArray(input[1]) ? { lat: input[1][0], lng: input[1][1] } : input[1];
    return new LatLngBounds(sw, ne);
  }

  class LatLngBounds {
    constructor(sw, ne) {
      this._southWest = { lat: sw.lat, lng: sw.lng };
      this._northEast = { lat: ne.lat, lng: ne.lng };
    }

    getSouthWest() {
      return { lat: this._southWest.lat, lng: this._southWest.lng };
    }

    getNorthEast() {
      return { lat: this._northEast.lat, lng: this._northEast.lng };
    }

    getWest() {
      return this._southWest.lng;
    }

    getEast() {
      return this._northEast.lng;
    }

    getSouth() {
      return this._southWest.lat;
    }

    getNorth() {
      return this._northEast.lat;
    }

    extend(latlng) {
      const point = Array.isArray(latlng) ? { lat: latlng[0], lng: latlng[1] } : latlng;
      this._southWest.lat = Math.min(this._southWest.lat, point.lat);
      this._southWest.lng = Math.min(this._southWest.lng, point.lng);
      this._northEast.lat = Math.max(this._northEast.lat, point.lat);
      this._northEast.lng = Math.max(this._northEast.lng, point.lng);
      return this;
    }
  }

  class TileLayer {
    constructor(urlTemplate, options = {}) {
      this._template = urlTemplate;
      this._options = {
        subdomains: SUBDOMAIN_FALLBACK,
        minZoom: MIN_ZOOM,
        maxZoom: MAX_ZOOM,
        attribution: "",
        ...options,
      };
      if (typeof this._options.subdomains === "string") {
        this._options.subdomains = this._options.subdomains.split("");
      }
      this._container = null;
      this._map = null;
      this._tiles = new Map();
    }

    addTo(map) {
      map.addLayer(this);
      return this;
    }

    onAdd(map) {
      this._map = map;
      this._container = document.createElement("div");
      this._container.className = "leaflet-tile-container";
      map._tilePane.appendChild(this._container);
      this._update();
    }

    onRemove() {
      if (this._container && this._container.parentNode) {
        this._container.parentNode.removeChild(this._container);
      }
      this._tiles.clear();
      this._map = null;
      this._container = null;
    }

    _update() {
      if (!this._map || !this._container) return;
      const zoom = clampZoom(Math.round(this._map._zoom));
      const tileZoom = Math.max(this._options.minZoom, Math.min(this._options.maxZoom, zoom));
      const centerPoint = project(this._map._center, tileZoom);
      const { width, height } = this._map._size;
      const halfWidth = width / 2;
      const halfHeight = height / 2;
      const topLeftPoint = { x: centerPoint.x - halfWidth, y: centerPoint.y - halfHeight };
      const bottomRightPoint = { x: centerPoint.x + halfWidth, y: centerPoint.y + halfHeight };

      const tileSize = TILE_SIZE;
      const tileBounds = {
        minX: Math.floor(topLeftPoint.x / tileSize),
        maxX: Math.floor(bottomRightPoint.x / tileSize),
        minY: Math.floor(topLeftPoint.y / tileSize),
        maxY: Math.floor(bottomRightPoint.y / tileSize),
      };

      const usedTiles = new Set();

      for (let x = tileBounds.minX; x <= tileBounds.maxX; x++) {
        for (let y = tileBounds.minY; y <= tileBounds.maxY; y++) {
          const key = `${tileZoom}:${x}:${y}`;
          usedTiles.add(key);
          if (this._tiles.has(key)) continue;
          const img = document.createElement("img");
          img.className = "leaflet-tile";
          img.draggable = false;
          img.alt = "";
          const tileUrl = this._buildTileUrl(x, y, tileZoom);
          img.src = tileUrl;
          img.style.left = `${x * tileSize}px`;
          img.style.top = `${y * tileSize}px`;
          this._container.appendChild(img);
          this._tiles.set(key, img);
        }
      }

      for (const [key, img] of this._tiles.entries()) {
        if (!usedTiles.has(key)) {
          img.remove();
          this._tiles.delete(key);
        }
      }

      const offsetX = -topLeftPoint.x;
      const offsetY = -topLeftPoint.y;
      this._container.style.transform = `translate(${offsetX}px, ${offsetY}px)`;
    }

    _buildTileUrl(x, y, zoom) {
      let url = this._template;
      const parts = this._options.subdomains || SUBDOMAIN_FALLBACK;
      const subdomain = parts[(x + y) % parts.length];
      url = url.replace("{s}", subdomain);
      url = url.replace("{z}", zoom);
      url = url.replace("{x}", x);
      url = url.replace("{y}", y);
      return url;
    }
  }

  class PolylineLayer {
    constructor(latlngs = [], options = {}) {
      this._latlngs = latlngs.map((pt) => (Array.isArray(pt) ? { lat: pt[0], lng: pt[1] } : pt));
      this._options = {
        color: "#2563eb",
        weight: 3,
        opacity: 0.85,
        lineCap: "round",
        lineJoin: "round",
        ...options,
      };
      this._path = null;
      this._map = null;
    }

    addTo(map) {
      map.addLayer(this);
      return this;
    }

    getBounds() {
      if (!this._latlngs.length) return null;
      const bounds = new LatLngBounds(this._latlngs[0], this._latlngs[0]);
      this._latlngs.forEach((pt) => bounds.extend(pt));
      return bounds;
    }

    onAdd(map) {
      this._map = map;
      if (!map._overlaySvg) {
        map._overlaySvg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        map._overlaySvg.setAttribute("class", "leaflet-overlay-svg");
        map._overlayPane.appendChild(map._overlaySvg);
      }
      this._path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      this._applyStyles();
      map._overlaySvg.appendChild(this._path);
      this._update();
    }

    onRemove() {
      if (this._path && this._path.parentNode) {
        this._path.parentNode.removeChild(this._path);
      }
      this._path = null;
      this._map = null;
    }

    _applyStyles() {
      if (!this._path) return;
      this._path.setAttribute("fill", "none");
      this._path.setAttribute("stroke", this._options.color);
      this._path.setAttribute("stroke-width", this._options.weight);
      this._path.setAttribute("stroke-opacity", this._options.opacity);
      this._path.setAttribute("stroke-linecap", this._options.lineCap);
      this._path.setAttribute("stroke-linejoin", this._options.lineJoin);
    }

    _update() {
      if (!this._map || !this._path || !this._latlngs.length) return;
      const zoom = this._map._zoom;
      const centerPoint = project(this._map._center, zoom);
      const points = this._latlngs.map((latlng) => project(latlng, zoom));
      const transformed = points.map((pt) => ({
        x: pt.x - centerPoint.x + this._map._size.width / 2,
        y: pt.y - centerPoint.y + this._map._size.height / 2,
      }));
      const d = transformed.map((pt, idx) => `${idx === 0 ? "M" : "L"}${pt.x.toFixed(2)},${pt.y.toFixed(2)}`).join(" ");
      this._path.setAttribute("d", d);
    }
  }

  class CircleMarkerLayer {
    constructor(latlng, options = {}) {
      this._latlng = Array.isArray(latlng) ? { lat: latlng[0], lng: latlng[1] } : latlng;
      this._options = {
        radius: 4,
        color: "#0ea5e9",
        fillColor: null,
        fillOpacity: 0.9,
        opacity: 1,
        ...options,
      };
      this._circle = null;
      this._map = null;
    }

    addTo(map) {
      map.addLayer(this);
      return this;
    }

    onAdd(map) {
      this._map = map;
      if (!map._overlaySvg) {
        map._overlaySvg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        map._overlaySvg.setAttribute("class", "leaflet-overlay-svg");
        map._overlayPane.appendChild(map._overlaySvg);
      }
      this._circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      this._circle.setAttribute("r", this._options.radius);
      this._circle.setAttribute("stroke", this._options.color);
      this._circle.setAttribute("stroke-opacity", this._options.opacity);
      const fillColor = this._options.fillColor || this._options.color;
      this._circle.setAttribute("fill", fillColor);
      this._circle.setAttribute("fill-opacity", this._options.fillOpacity);
      map._overlaySvg.appendChild(this._circle);
      this._update();
    }

    onRemove() {
      if (this._circle && this._circle.parentNode) {
        this._circle.parentNode.removeChild(this._circle);
      }
      this._circle = null;
      this._map = null;
    }

    _update() {
      if (!this._map || !this._circle) return;
      const zoom = this._map._zoom;
      const centerPoint = project(this._map._center, zoom);
      const pt = project(this._latlng, zoom);
      const x = pt.x - centerPoint.x + this._map._size.width / 2;
      const y = pt.y - centerPoint.y + this._map._size.height / 2;
      this._circle.setAttribute("cx", x.toFixed(2));
      this._circle.setAttribute("cy", y.toFixed(2));
    }
  }

  class Map {
    constructor(container, options = {}) {
      this._container = typeof container === "string" ? document.getElementById(container) : container;
      if (!this._container) {
        throw new Error("Map container not found");
      }
      this._options = {
        center: DEFAULT_CENTER,
        zoom: DEFAULT_ZOOM,
        scrollWheelZoom: true,
        ...options,
      };
      this._zoom = clampZoom(this._options.zoom);
      this._center = { lat: this._options.center.lat, lng: this._options.center.lng };
      this._layers = new Set();
      this._size = { width: this._container.clientWidth, height: this._container.clientHeight };
      this._handlers = {};
      this._setupStructure();
      this._bindInteractions();
      this._update();
    }

    _setupStructure() {
      this._container.classList.add("leaflet-container");
      this._tilePane = document.createElement("div");
      this._tilePane.className = "leaflet-tile-pane";
      this._overlayPane = document.createElement("div");
      this._overlayPane.className = "leaflet-overlay-pane";
      this._container.appendChild(this._tilePane);
      this._container.appendChild(this._overlayPane);

      if (this._options.zoomControl !== false) {
        const zoomControl = document.createElement("div");
        zoomControl.className = "leaflet-control-zoom";
        const zoomIn = document.createElement("button");
        zoomIn.type = "button";
        zoomIn.textContent = "+";
        const zoomOut = document.createElement("button");
        zoomOut.type = "button";
        zoomOut.textContent = "-";
        zoomIn.addEventListener("click", () => this.setZoom(this._zoom + 1));
        zoomOut.addEventListener("click", () => this.setZoom(this._zoom - 1));
        zoomControl.appendChild(zoomIn);
        zoomControl.appendChild(zoomOut);
        this._container.appendChild(zoomControl);
      }
    }

    _bindInteractions() {
      this._dragging = false;
      this._lastDragPoint = null;

      const onPointerDown = (event) => {
        if (event.button !== 0) return;
        this._dragging = true;
        this._container.setPointerCapture(event.pointerId);
        this._lastDragPoint = { x: event.clientX, y: event.clientY };
      };

      const onPointerMove = (event) => {
        if (!this._dragging || !this._lastDragPoint) return;
        const dx = event.clientX - this._lastDragPoint.x;
        const dy = event.clientY - this._lastDragPoint.y;
        this._lastDragPoint = { x: event.clientX, y: event.clientY };
        this._panBy(dx, dy);
      };

      const onPointerUp = (event) => {
        if (!this._dragging) return;
        this._dragging = false;
        this._lastDragPoint = null;
        this._container.releasePointerCapture(event.pointerId);
      };

      const onPointerLeave = () => {
        this._dragging = false;
        this._lastDragPoint = null;
      };

      const onWheel = (event) => {
        if (this._options.scrollWheelZoom === false) return;
        event.preventDefault();
        const delta = event.deltaY > 0 ? -1 : 1;
        const newZoom = clampZoom(this._zoom + delta);
        if (newZoom === this._zoom) return;
        const rect = this._container.getBoundingClientRect();
        const point = {
          x: event.clientX - rect.left,
          y: event.clientY - rect.top,
        };
        this._zoomAround(point, newZoom);
      };

      const onResize = () => this.invalidateSize();

      this._handlers.pointerdown = onPointerDown;
      this._handlers.pointermove = onPointerMove;
      this._handlers.pointerup = onPointerUp;
      this._handlers.pointerleave = onPointerLeave;
      this._handlers.wheel = onWheel;
      this._handlers.resize = onResize;

      this._container.addEventListener("pointerdown", onPointerDown);
      this._container.addEventListener("pointermove", onPointerMove);
      this._container.addEventListener("pointerup", onPointerUp);
      this._container.addEventListener("pointerleave", onPointerLeave);
      this._container.addEventListener("wheel", onWheel, { passive: false });
      window.addEventListener("resize", onResize);
    }

    _panBy(dx, dy) {
      const zoom = this._zoom;
      const scale = TILE_SIZE * Math.pow(2, zoom);
      const centerPoint = project(this._center, zoom);
      const newPoint = {
        x: centerPoint.x - dx,
        y: centerPoint.y - dy,
      };
      this._center = unproject(newPoint, zoom);
      this._update();
    }

    _zoomAround(pixel, newZoom) {
      const oldZoom = this._zoom;
      if (newZoom === oldZoom) return;
      const centerPointOld = project(this._center, oldZoom);
      const containerCenter = {
        x: this._size.width / 2,
        y: this._size.height / 2,
      };
      const offset = {
        x: pixel.x - containerCenter.x,
        y: pixel.y - containerCenter.y,
      };
      const targetPoint = {
        x: centerPointOld.x + offset.x,
        y: centerPointOld.y + offset.y,
      };
      const targetLatLng = unproject(targetPoint, oldZoom);
      this._zoom = clampZoom(newZoom);
      const newTargetPoint = project(targetLatLng, this._zoom);
      const newCenterPoint = {
        x: newTargetPoint.x - offset.x,
        y: newTargetPoint.y - offset.y,
      };
      this._center = unproject(newCenterPoint, this._zoom);
      this._update();
    }

    _updateSize() {
      this._size = {
        width: this._container.clientWidth,
        height: this._container.clientHeight,
      };
      if (this._overlaySvg) {
        this._overlaySvg.setAttribute("width", this._size.width);
        this._overlaySvg.setAttribute("height", this._size.height);
      }
    }

    _update() {
      this._updateSize();
      for (const layer of this._layers) {
        if (layer instanceof TileLayer) {
          layer._update();
        }
      }
      if (this._overlaySvg) {
        this._overlaySvg.setAttribute("width", this._size.width);
        this._overlaySvg.setAttribute("height", this._size.height);
      }
      for (const layer of this._layers) {
        if (layer instanceof PolylineLayer || layer instanceof CircleMarkerLayer) {
          layer._update();
        }
      }
    }

    addLayer(layer) {
      if (this._layers.has(layer)) return this;
      this._layers.add(layer);
      if (layer.onAdd) {
        layer.onAdd(this);
      }
      return this;
    }

    removeLayer(layer) {
      if (!this._layers.has(layer)) return this;
      this._layers.delete(layer);
      if (layer.onRemove) {
        layer.onRemove(this);
      }
      return this;
    }

    setView(center, zoom) {
      this._center = { lat: center.lat, lng: center.lng };
      if (zoom !== undefined) {
        this._zoom = clampZoom(zoom);
      }
      this._update();
      return this;
    }

    setZoom(zoom) {
      this._zoom = clampZoom(zoom);
      this._update();
      return this;
    }

    fitBounds(boundsInput, options = {}) {
      const bounds = boundsInput instanceof LatLngBounds ? boundsInput : latLngBounds(boundsInput);
      const padding = options.padding || [0, 0];
      const size = {
        width: this._container.clientWidth - padding[0] * 2,
        height: this._container.clientHeight - padding[1] * 2,
      };
      const sw = bounds.getSouthWest();
      const ne = bounds.getNorthEast();
      let zoom = MAX_ZOOM;
      for (let z = MAX_ZOOM; z >= MIN_ZOOM; z--) {
        const swPoint = project(sw, z);
        const nePoint = project(ne, z);
        const width = Math.abs(nePoint.x - swPoint.x);
        const height = Math.abs(nePoint.y - swPoint.y);
        if (width <= size.width && height <= size.height) {
          zoom = z;
          break;
        }
      }
      const center = {
        lat: (sw.lat + ne.lat) / 2,
        lng: (sw.lng + ne.lng) / 2,
      };
      this._zoom = zoom;
      this._center = center;
      this._update();
      return this;
    }

    invalidateSize() {
      this._update();
      return this;
    }

    remove() {
      for (const layer of Array.from(this._layers)) {
        this.removeLayer(layer);
      }
      this._container.classList.remove("leaflet-container");
      if (this._tilePane) this._tilePane.remove();
      if (this._overlayPane) this._overlayPane.remove();
      this._overlaySvg = null;
      if (this._handlers.pointerdown) this._container.removeEventListener("pointerdown", this._handlers.pointerdown);
      if (this._handlers.pointermove) this._container.removeEventListener("pointermove", this._handlers.pointermove);
      if (this._handlers.pointerup) this._container.removeEventListener("pointerup", this._handlers.pointerup);
      if (this._handlers.pointerleave) this._container.removeEventListener("pointerleave", this._handlers.pointerleave);
      if (this._handlers.wheel) this._container.removeEventListener("wheel", this._handlers.wheel);
      if (this._handlers.resize) window.removeEventListener("resize", this._handlers.resize);
      this._handlers = {};
    }
  }

  function createMap(container, options) {
    return new Map(container, options);
  }

  function createTileLayer(template, options) {
    return new TileLayer(template, options);
  }

  function createPolyline(latlngs, options) {
    return new PolylineLayer(latlngs, options);
  }

  function createCircleMarker(latlng, options) {
    return new CircleMarkerLayer(latlng, options);
  }

  const L = {
    version: "1.9.4-local-lite",
    map: createMap,
    tileLayer: createTileLayer,
    polyline: createPolyline,
    circleMarker: createCircleMarker,
    latLngBounds,
  };

  global.L = L;
})(window);
