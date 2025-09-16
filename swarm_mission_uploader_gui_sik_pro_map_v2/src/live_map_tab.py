import math
import queue
import threading
import time
import urllib.error
import urllib.request
from io import BytesIO
from tkinter import messagebox, ttk

import numpy as np
from matplotlib import patheffects
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from PIL import Image
from pymavlink import mavutil as pymavutil


class LiveMapTab:
    """Encapsulates the Live Map UI and background telemetry threads."""

    TILE_URL = (
        "https://server.arcgisonline.com/ArcGIS/rest/services/"
        "World_Imagery/MapServer/tile/{z}/{y}/{x}"
    )
    TILE_SIZE = 256
    PROCESS_INTERVAL_MS = 400

    def __init__(self, gui, parent, connect_fn):
        self.gui = gui
        self.root = gui.root
        self._mavlink_connect = connect_fn
        self._queue: queue.Queue = queue.Queue()
        self._positions: dict[int, dict] = {}
        self._rows: dict[int, str] = {}
        self._threads: list[threading.Thread] = []
        self._alive = False
        self._dirty = False
        self._last_emit: dict[tuple[int, str], float] = {}
        self._last_extent = None
        self._tile_cache: dict[tuple[int, int, int], Image.Image] = {}
        self._tile_failures: set = set()

        self._build(parent)
        # kick off the queue processor once widgets exist
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
                mav = self._mavlink_connect(endpoint)
            except Exception as exc:  # pragma: no cover - GUI feedback
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
                    (
                        "position",
                        sysid,
                        lat,
                        lon,
                        alt,
                        endpoint,
                        "Global Position",
                    )
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
        last = self._last_emit.get(key, 0)
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
        if not extent:
            if lons and lats:
                lon_min, lon_max = min(lons), max(lons)
                lat_min, lat_max = min(lats), max(lats)
                pad_lon = max((lon_max - lon_min) * 0.2, 0.01)
                pad_lat = max((lat_max - lat_min) * 0.2, 0.01)
                self.ax.set_xlim(lon_min - pad_lon, lon_max + pad_lon)
                self.ax.set_ylim(lat_min - pad_lat, lat_max + pad_lat)
            else:
                self.ax.set_xlim(-0.05, 0.05)
                self.ax.set_ylim(-0.05, 0.05)
        else:
            self.ax.set_xlim(extent[0], extent[1])
            self.ax.set_ylim(extent[2], extent[3])
        if pts:
            self.ax.scatter(
                lons,
                lats,
                c="#4dd0e1",
                edgecolors="black",
                linewidths=0.6,
                s=64,
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
        target_lats = lats or []
        target_lons = lons or []
        if target_lats and target_lons:
            lat_min = min(target_lats)
            lat_max = max(target_lats)
            lon_min = min(target_lons)
            lon_max = max(target_lons)
            pad_lat = max((lat_max - lat_min) * 0.3, 0.01)
            pad_lon = max((lon_max - lon_min) * 0.3, 0.01)
            lat_min -= pad_lat
            lat_max += pad_lat
            lon_min -= pad_lon
            lon_max += pad_lon
        elif self._last_extent:
            lon_min, lon_max, lat_min, lat_max = self._last_extent
        else:
            lat_min, lat_max = -1.0, 1.0
            lon_min, lon_max = -1.0, 1.0
        lat_min = max(lat_min, -85.0)
        lat_max = min(lat_max, 85.0)
        lon_min = max(lon_min, -180.0)
        lon_max = min(lon_max, 180.0)
        if lat_min >= lat_max:
            lat_min -= 0.01
            lat_max += 0.01
        if lon_min == lon_max:
            lon_min -= 0.01
            lon_max += 0.01
        if lon_min > lon_max:
            lon_min, lon_max = lon_max, lon_min
        try:
            image, extent = self._fetch_satellite_tiles(lat_min, lat_max, lon_min, lon_max)
        except Exception as exc:  # pragma: no cover - network failures
            key = ("tiles", str(exc))
            if key not in self._tile_failures:
                self._tile_failures.add(key)
                self._log(f"[map] Satellite background error: {exc}")
            return None
        if image is None:
            return None
        self.ax.imshow(image, extent=extent, origin="upper", zorder=1)
        self._last_extent = extent
        return extent

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
        return np.asarray(mosaic), extent

    def _select_zoom(self, lat_min, lat_max, lon_min, lon_max):
        lon_span = max(lon_max - lon_min, 1e-6)
        lat_span = max(lat_max - lat_min, 1e-6)
        lon_zoom = math.log2(360.0 / lon_span)
        lat_zoom = math.log2(170.0 / lat_span)
        return int(max(3, min(17, lon_zoom, lat_zoom)))

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
            err_key = ("fetch", f"{zoom}-{x_wrapped}-{y}")
            if err_key not in self._tile_failures:
                self._tile_failures.add(err_key)
                self._log(f"[map] Tile fetch failed ({zoom}/{x_wrapped}/{y}): {exc}")
            return None
        self._tile_cache[key] = tile
        return tile

    def _fetch_tile_image(self, zoom, x, y):
        url = self.TILE_URL.format(z=zoom, x=x, y=y)
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = resp.read()
        return Image.open(BytesIO(data)).convert("RGB")

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

