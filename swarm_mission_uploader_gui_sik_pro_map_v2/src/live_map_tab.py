"""Live map tab with satellite background rendering."""

from __future__ import annotations

import math
import queue
import threading
import time
import urllib.request
from io import BytesIO
from typing import Dict, List, Optional, Set, Tuple

from tkinter import messagebox, ttk

from matplotlib import patheffects
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from PIL import Image, ImageDraw
from pymavlink import mavutil as pymavutil


class LiveMapTab:
    """Encapsulates the Live Map UI and telemetry rendering."""

    TILE_URL = (
        "https://server.arcgisonline.com/ArcGIS/rest/services/"
        "World_Imagery/MapServer/tile/{z}/{y}/{x}"
    )
    TILE_SIZE = 256
    MIN_ZOOM = 3
    MAX_ZOOM = 17
    PROCESS_INTERVAL_MS = 400

    def __init__(self, gui, parent, connect_fn):
        self.gui = gui
        self.root = gui.root
        self._connect = connect_fn

        self._queue: queue.Queue = queue.Queue()
        self._positions: Dict[int, dict] = {}
        self._rows: Dict[int, str] = {}
        self._threads: List[threading.Thread] = []
        self._alive = False
        self._dirty = False
        self._last_emit: Dict[tuple, float] = {}
        self._last_extent: Optional[Tuple[float, float, float, float]] = None
        self._tile_cache: Dict[tuple, Image.Image] = {}
        self._tile_failures: Set[tuple] = set()
        self._fallback_background: Optional[Image.Image] = None
        self._fallback_unavailable = False
        self._fallback_extent: Tuple[float, float, float, float] = (-180.0, 180.0, -90.0, 90.0)
        self._reported_fallback = False

        self._build(parent)
        self.root.after(self.PROCESS_INTERVAL_MS, self._process_queue)

    # ------------------------------------------------------------------
    # UI construction
    def _build(self, parent):
        top = ttk.Frame(parent, padding=8)
        top.pack(fill="x")
        ttk.Button(top, text="Start Live View", command=self.start).pack(
            side="left", padx=5
        )
        ttk.Button(top, text="Stop", command=self.stop).pack(side="left", padx=5)
        ttk.Button(top, text="Clear", command=self.clear).pack(side="left", padx=5)
        ttk.Label(
            top,
            text="(Streams HEARTBEAT/GPS from configured ports)",
        ).pack(side="left", padx=12)

        status = ttk.Labelframe(parent, text="Vehicle Status", padding=8)
        status.pack(fill="x", padx=8, pady=(0, 8))
        cols = ("sysid", "status", "lat", "lon", "alt", "source", "updated")
        self.tree = ttk.Treeview(status, columns=cols, show="headings", height=8)
        headings = [
            ("sysid", "SYSID", 70, "center"),
            ("status", "Status", 220, "w"),
            ("lat", "Latitude", 120, "center"),
            ("lon", "Longitude", 120, "center"),
            ("alt", "Alt (m)", 90, "center"),
            ("source", "Port", 200, "w"),
            ("updated", "Updated", 120, "center"),
        ]
        for key, title, width, anchor in headings:
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, anchor=anchor)
        vsb = ttk.Scrollbar(status, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        canvas_frame = ttk.Frame(parent, padding=(8, 0, 8, 8))
        canvas_frame.pack(fill="both", expand=True)
        self.fig = Figure(figsize=(6, 4), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=canvas_frame)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(fill="both", expand=True)
        self._set_axes_style()

    # ------------------------------------------------------------------
    # Public controls
    def start(self):
        self.stop()
        if not self.gui.ports:
            messagebox.showinfo(
                "Live Map", "Add at least one SiK port on the Uploader tab."
            )
            return
        self.clear()
        self._alive = True
        self._threads = []
        self._log("[map] Starting live telemetry monitor...")
        for cfg in self.gui.ports:
            thread = threading.Thread(
                target=self._worker,
                args=(cfg,),
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

    def stop(self):
        if not self._alive:
            return
        self._alive = False
        self._log("[map] Live telemetry monitor stopping...")

    def clear(self):
        self._positions.clear()
        self._rows.clear()
        self._last_emit.clear()
        self._last_extent = None
        self._dirty = False
        if hasattr(self, "tree"):
            self.tree.delete(*self.tree.get_children())
        self._set_axes_style()
        self.canvas.draw_idle()

    # ------------------------------------------------------------------
    # Telemetry processing
    def _worker(self, cfg):
        bauds = [cfg["baud"]] if not cfg.get("autobaud") else [57600, 115200]
        for baud in bauds:
            if not self._alive:
                return
            endpoint = f"serial:{cfg['port']},{baud}"
            self._log(f"[map] Listening on {endpoint}")
            try:
                mav = self._connect(endpoint)
            except Exception as exc:  # pragma: no cover - connection feedback
                self._log(f"[map] {endpoint} connect error: {exc}")
                continue
            try:
                while self._alive:
                    msg = mav.recv_match(blocking=False)
                    if not msg:
                        time.sleep(0.1)
                        continue
                    self._handle_message(msg, endpoint)
            finally:
                try:
                    mav.close()
                except Exception:
                    pass
            return
        self._log(f"[map] No telemetry received via {cfg['port']}")

    def _handle_message(self, msg, endpoint):
        try:
            sysid = msg.get_srcSystem()
        except Exception:
            sysid = None
        if not sysid:
            return
        mtype = msg.get_type()
        if mtype == "GLOBAL_POSITION_INT":
            lat = getattr(msg, "lat", None)
            lon = getattr(msg, "lon", None)
            if lat is None or lon is None:
                return
            lat = lat / 1e7
            lon = lon / 1e7
            alt = getattr(msg, "relative_alt", getattr(msg, "alt", None))
            if alt is not None:
                alt = alt / 1000.0
            if self._should_emit(sysid, "pos", 0.2):
                self._queue.put(
                    ("position", sysid, lat, lon, alt, endpoint, "Global Position")
                )
        elif mtype == "GPS_RAW_INT":
            lat = getattr(msg, "lat", None)
            lon = getattr(msg, "lon", None)
            if lat is None or lon is None:
                return
            lat = lat / 1e7
            lon = lon / 1e7
            alt = getattr(msg, "alt", None)
            if alt is not None:
                alt = alt / 1000.0
            fix_type = getattr(msg, "fix_type", None)
            status = f"GPS Raw ({self._describe_fix(fix_type)})"
            if self._should_emit(sysid, "gps", 0.5):
                self._queue.put(("position", sysid, lat, lon, alt, endpoint, status))
        elif mtype == "HEARTBEAT":
            if self._should_emit(sysid, "hb", 1.0):
                status = self._describe_heartbeat(msg)
                self._queue.put(("status", sysid, status, endpoint))

    def _should_emit(self, sysid, kind, interval):
        key = (sysid, kind)
        now = time.time()
        last = self._last_emit.get(key, 0.0)
        if now - last >= interval:
            self._last_emit[key] = now
            return True
        return False

    def _process_queue(self):
        try:
            while True:
                item = self._queue.get_nowait()
                if not item:
                    continue
                if item[0] == "position":
                    _, sysid, lat, lon, alt, endpoint, status = item
                    self._handle_position(sysid, lat, lon, alt, endpoint, status)
                elif item[0] == "status":
                    _, sysid, status, endpoint = item
                    self._handle_status(sysid, status, endpoint)
        except queue.Empty:
            pass
        self._draw_if_dirty()
        self.root.after(self.PROCESS_INTERVAL_MS, self._process_queue)

    def _handle_position(self, sysid, lat, lon, alt, endpoint, status):
        data = self._positions.get(sysid, {})
        data.update(
            {
                "lat": lat,
                "lon": lon,
                "alt": alt,
                "source": self._format_source(endpoint),
                "timestamp": time.time(),
                "status": status,
            }
        )
        self._positions[sysid] = data
        self._update_tree(sysid, data)
        self._dirty = True

    def _handle_status(self, sysid, status, endpoint):
        data = self._positions.get(sysid, {})
        data.update(
            {
                "status": status,
                "source": self._format_source(endpoint),
                "timestamp": time.time(),
            }
        )
        self._positions[sysid] = data
        self._update_tree(sysid, data)

    def _update_tree(self, sysid, data):
        lat = data.get("lat")
        lon = data.get("lon")
        alt = data.get("alt")
        lat_txt = f"{lat:.6f}" if lat is not None else "—"
        lon_txt = f"{lon:.6f}" if lon is not None else "—"
        alt_txt = f"{alt:.1f}" if alt is not None else "—"
        status = data.get("status", "")
        src = data.get("source", "")
        ts = time.strftime("%H:%M:%S", time.localtime(data.get("timestamp", time.time())))
        values = (sysid, status, lat_txt, lon_txt, alt_txt, src, ts)
        iid = self._rows.get(sysid)
        if iid and self.tree.exists(iid):
            self.tree.item(iid, values=values)
        else:
            self._rows[sysid] = self.tree.insert("", "end", values=values)

    def _draw_if_dirty(self):
        if not self._dirty:
            return
        self._draw_points()
        self._dirty = False

    def _draw_points(self):
        self.ax.clear()
        self._set_axes_style()
        pts = [
            (data.get("lon"), data.get("lat"), sid)
            for sid, data in self._positions.items()
            if data.get("lat") is not None and data.get("lon") is not None
        ]
        lons = [p[0] for p in pts]
        lats = [p[1] for p in pts]
        extent = self._draw_background(lats, lons)
        if extent:
            self.ax.set_xlim(extent[0], extent[1])
            self.ax.set_ylim(extent[2], extent[3])
        else:
            self.ax.set_xlim(-180.0, 180.0)
            self.ax.set_ylim(-85.0, 85.0)
        if pts:
            self.ax.scatter(
                lons,
                lats,
                c="#f1c40f",
                edgecolors="black",
                linewidths=0.6,
                s=70,
                zorder=5,
                alpha=0.95,
            )
            for lon, lat, sid in pts:
                txt = self.ax.text(
                    lon,
                    lat,
                    str(sid),
                    fontsize=9,
                    ha="left",
                    va="bottom",
                    color="white",
                    zorder=6,
                )
                txt.set_path_effects(
                    [patheffects.withStroke(linewidth=2, foreground="black")]
                )
        else:
            self.ax.text(
                0.5,
                0.5,
                "Waiting for GPS fixes…",
                fontsize=12,
                color="white",
                ha="center",
                va="center",
                transform=self.ax.transAxes,
            )
        try:
            self.ax.set_aspect("equal", adjustable="box")
        except Exception:
            pass
        self.canvas.draw_idle()

    def _draw_background(self, lats, lons):
        bounds = self._compute_bounds(lats, lons)
        image = None
        extent = None
        if bounds:
            lat_min, lat_max, lon_min, lon_max = bounds
            try:
                image, extent = self._fetch_satellite_tiles(
                    lat_min, lat_max, lon_min, lon_max
                )
            except Exception as exc:  # pragma: no cover - network failures
                key = ("tiles", str(exc))
                if key not in self._tile_failures:
                    self._tile_failures.add(key)
                    self._log(f"[map] Satellite background error: {exc}")
                image = None
                extent = None
        if image is None:
            fallback = self._load_fallback_background()
            if fallback:
                image, extent = fallback
        if image is None or extent is None:
            return None
        self.ax.imshow(image, extent=extent, origin="upper", zorder=1)
        self._last_extent = extent
        return extent

    def _compute_bounds(self, lats, lons):
        if lats and lons:
            lat_min = min(lats)
            lat_max = max(lats)
            lon_min = min(lons)
            lon_max = max(lons)
            if lon_max - lon_min > 340:
                lon_min, lon_max = -180.0, 180.0
            pad_lat = max((lat_max - lat_min) * 0.3, 0.01)
            pad_lon = max((lon_max - lon_min) * 0.3, 0.01)
            lat_min = max(-85.0, lat_min - pad_lat)
            lat_max = min(85.0, lat_max + pad_lat)
            lon_min = max(-180.0, lon_min - pad_lon)
            lon_max = min(180.0, lon_max + pad_lon)
            if lat_max <= lat_min:
                lat_min -= 0.01
                lat_max += 0.01
            if lon_max <= lon_min:
                lon_min -= 0.01
                lon_max += 0.01
            return lat_min, lat_max, lon_min, lon_max
        if self._last_extent:
            lon_min, lon_max, lat_min, lat_max = self._last_extent
            return lat_min, lat_max, lon_min, lon_max
        return None

    # ------------------------------------------------------------------
    # Satellite tile helpers
    def _fetch_satellite_tiles(self, lat_min, lat_max, lon_min, lon_max):
        zoom = self._select_zoom(lat_min, lat_max, lon_min, lon_max)
        x0, y0 = self._tile_coords(lat_max, lon_min, zoom)
        x1, y1 = self._tile_coords(lat_min, lon_max, zoom)
        x_min = int(math.floor(min(x0, x1)))
        x_max = int(math.floor(max(x0, x1)))
        y_min = int(math.floor(min(y0, y1)))
        y_max = int(math.floor(max(y0, y1)))
        n = 2 ** zoom
        x_min = max(x_min, 0)
        x_max = min(x_max, n - 1)
        y_min = max(y_min, 0)
        y_max = min(y_max, n - 1)
        width = (x_max - x_min + 1) * self.TILE_SIZE
        height = (y_max - y_min + 1) * self.TILE_SIZE
        mosaic = Image.new("RGB", (width, height))
        any_tile = False
        for xi, x in enumerate(range(x_min, x_max + 1)):
            for yi, y in enumerate(range(y_min, y_max + 1)):
                tile = self._get_tile(zoom, x, y)
                if tile is None:
                    continue
                mosaic.paste(tile, (xi * self.TILE_SIZE, yi * self.TILE_SIZE))
                any_tile = True
        if not any_tile:
            return None, None
        lon_left = self._tile_x_to_lon(x_min, zoom)
        lon_right = self._tile_x_to_lon(x_max + 1, zoom)
        lat_top = self._tile_y_to_lat(y_min, zoom)
        lat_bottom = self._tile_y_to_lat(y_max + 1, zoom)
        extent = (lon_left, lon_right, lat_bottom, lat_top)
        return mosaic, extent

    def _select_zoom(self, lat_min, lat_max, lon_min, lon_max):
        lon_span = max(lon_max - lon_min, 1e-6)
        lat_span = max(lat_max - lat_min, 1e-6)
        lon_zoom = math.log2(360.0 / lon_span)
        lat_zoom = math.log2(170.0 / lat_span)
        return int(max(self.MIN_ZOOM, min(self.MAX_ZOOM, lon_zoom, lat_zoom)))

    def _tile_coords(self, lat, lon, zoom):
        lat = max(min(lat, 85.0), -85.0)
        n = 2 ** zoom
        x = (lon + 180.0) / 360.0 * n
        lat_rad = math.radians(lat)
        y = (1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n
        return x, y

    def _tile_x_to_lon(self, x, zoom):
        n = 2 ** zoom
        return x / n * 360.0 - 180.0

    def _tile_y_to_lat(self, y, zoom):
        n = 2 ** zoom
        lat_rad = math.atan(math.sinh(math.pi - 2.0 * math.pi * y / n))
        return math.degrees(lat_rad)

    def _get_tile(self, zoom, x, y):
        n = 2 ** zoom
        x_wrapped = int(x) % n
        if y < 0 or y >= n:
            return None
        key = (zoom, x_wrapped, int(y))
        if key in self._tile_cache:
            return self._tile_cache[key]
        try:
            tile = self._fetch_tile_image(zoom, x_wrapped, int(y))
        except Exception as exc:  # pragma: no cover - network failures
            err_key = ("fetch", zoom, x_wrapped, int(y), str(exc))
            if err_key not in self._tile_failures:
                self._tile_failures.add(err_key)
                self._log(f"[map] Tile fetch failed ({zoom}/{x_wrapped}/{y}): {exc}")
            return None
        self._tile_cache[key] = tile
        return tile

    def _fetch_tile_image(self, zoom, x, y):
        url = self.TILE_URL.format(z=zoom, x=x, y=y)
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "SwarmMissionUploader/1.0"},
        )
        with urllib.request.urlopen(request, timeout=10) as resp:
            data = resp.read()
        return Image.open(BytesIO(data)).convert("RGB")

    # ------------------------------------------------------------------
    # Fallback imagery helpers
    def _load_fallback_background(self):
        if self._fallback_unavailable:
            return None
        if self._fallback_background is not None:
            return self._fallback_background, self._fallback_extent
        try:
            from matplotlib import cbook

            path = cbook.get_sample_data("bluemarble.jpg", asfileobj=False)
            image = Image.open(path).convert("RGB")
        except Exception:
            image = self._generate_fallback_image()
        if image is None:
            self._fallback_unavailable = True
            return None
        self._fallback_background = image
        if not self._reported_fallback:
            self._log("[map] Using fallback imagery for live map background.")
            self._reported_fallback = True
        return image, self._fallback_extent

    def _generate_fallback_image(self):
        try:
            width, height = 2048, 1024
            image = Image.new("RGB", (width, height))
            draw = ImageDraw.Draw(image)
            for y in range(height):
                ratio = y / (height - 1)
                r = int(20 + 50 * (1 - ratio))
                g = int(60 + 90 * (1 - ratio))
                b = int(120 + 120 * (1 - ratio))
                draw.line([(0, y), (width, y)], fill=(r, g, b))
            continents = [
                [(-130, 55), (-60, 75), (-20, 60), (-10, 25), (-70, 0), (-85, -35), (-130, 0)],
                [(-80, -5), (-30, 10), (-20, -20), (-50, -30)],
                [(10, 70), (70, 55), (120, 50), (140, 20), (90, -5), (50, 10), (30, 25)],
                [(25, -10), (90, -25), (130, -10), (150, -45), (95, -50), (40, -35)],
                [(-20, -20), (20, -30), (55, -50), (20, -65), (-15, -55)],
            ]
            for polygon in continents:
                draw.polygon(
                    [self._lonlat_to_xy(pt, width, height) for pt in polygon],
                    fill=(55, 120, 75),
                    outline=(30, 75, 45),
                )
            return image
        except Exception:
            return None

    def _lonlat_to_xy(self, point, width, height):
        lon, lat = point
        x = (lon + 180.0) / 360.0 * width
        y = (90.0 - lat) / 180.0 * height
        return x, y

    # ------------------------------------------------------------------
    # Formatting helpers
    def _format_source(self, endpoint):
        if isinstance(endpoint, str) and endpoint.startswith("serial:"):
            return endpoint.split("serial:")[1]
        return endpoint

    def _describe_fix(self, fix_type):
        mapping = {1: "No Fix", 2: "2D", 3: "3D", 4: "DGPS", 5: "RTK Float", 6: "RTK Fixed"}
        return mapping.get(fix_type, "Unknown")

    def _describe_heartbeat(self, msg):
        try:
            mode = pymavutil.mode_string_v10(msg)
        except Exception:
            mode = "UNKNOWN"
        try:
            armed = bool(msg.base_mode & pymavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED)
        except Exception:
            armed = None
        if armed is None:
            return f"Heartbeat ({mode})"
        state = "ARMED" if armed else "DISARMED"
        return f"{mode} ({state})"

    def _set_axes_style(self):
        self.ax.set_facecolor("#000000")
        self.ax.set_xlabel("Longitude")
        self.ax.set_ylabel("Latitude")
        self.ax.set_title("Live Vehicle Positions")
        self.ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.3, color="white")

    def _log(self, message):
        if hasattr(self.gui, "log_write"):
            self.gui.log_write(message)
