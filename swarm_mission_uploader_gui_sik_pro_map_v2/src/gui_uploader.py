
#!/usr/bin/env python3
import threading, queue, time, csv, os, math
from pathlib import Path
from tkinter import Tk, StringVar, BooleanVar, DoubleVar, IntVar, ttk, filedialog, Text, END, DISABLED, NORMAL, messagebox, Toplevel, Listbox, SINGLE
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from pymavlink import mavutil as pymavutil

from uploader_core import (mavlink_connect, wait_heartbeat_all, read_qgc_wpl, write_qgc_wpl, mission_upload_pref_float, arm_and_start, apply_offset)

try:
    from serial.tools import list_ports
except Exception:
    list_ports = None

COMMON_BAUDS=[57600,115200]

class SwarmGUI:
    def __init__(self, root):
        self.root=root
        root.title('Swarm Mission Uploader — Pro (Map Preview)')
        root.geometry('1320x900')

        # Shared state
        self.mapping = {}          # sysid -> abs mission path
        self.discovered = {}       # sysid -> compid
        self.discovered_port = {}  # sysid -> endpoint
        self.ports = []            # list of dicts

        self.discover_s=DoubleVar(value=10.0)
        self.retries=IntVar(value=4)
        self.timeout_s=DoubleVar(value=6.0)
        self.arm_start=BooleanVar(value=False)
        self.stagger_s=DoubleVar(value=6.0)
        self.clear_first=BooleanVar(value=True)
        self.map_file=StringVar(value='')

        self.stop_flag=threading.Event(); self.log_q=queue.Queue()
        self._map_queue=queue.Queue(); self._map_positions={}; self._map_rows={}
        self._map_threads=[]; self._map_alive=False; self._map_dirty=False
        self._map_last_emit={}

        # Notebook tabs
        nb=ttk.Notebook(root); nb.pack(fill='both',expand=True)
        self.tab_upload=ttk.Frame(nb); nb.add(self.tab_upload,text='Uploader')
        self.tab_health=ttk.Frame(nb); nb.add(self.tab_health,text='Radio Health')
        self.tab_map=ttk.Frame(nb); nb.add(self.tab_map,text='Live Map')
        self.tab_gen=ttk.Frame(nb); nb.add(self.tab_gen,text='Batch Mission Generator')

        self._build_tab_uploader(self.tab_upload)
        self._build_tab_health(self.tab_health)
        self._build_tab_map(self.tab_map)
        self._build_tab_generator(self.tab_gen)

        self.root.after(100,self.flush_logs)
        self.root.after(200,self._map_process_queue)
        self.on_refresh_system_ports()

    # ---------- Logging ----------
    def log_write(self,msg): self.log_q.put(msg)
    def flush_logs(self):
        try:
            while True:
                msg=self.log_q.get_nowait()
                self.log.configure(state=NORMAL); self.log.insert(END,msg+'\n'); self.log.see(END); self.log.configure(state=DISABLED)
        except queue.Empty:
            pass
        self.root.after(100,self.flush_logs)

    # ---------- Uploader Tab ----------
    def _build_tab_uploader(self, parent):
        top=ttk.Frame(parent,padding=8); top.pack(fill='x')
        ttk.Label(top,text='Mapping CSV:').grid(row=0,column=0,sticky='w')
        ttk.Entry(top,textvariable=self.map_file,width=60).grid(row=0,column=1,columnspan=2,sticky='we',padx=5)
        ttk.Button(top,text='Load CSV…',command=self.on_load_csv).grid(row=0,column=3,sticky='w',padx=5)
        ttk.Button(top,text='Save CSV…',command=self.on_save_csv).grid(row=0,column=4,sticky='w')

        ports_frame=ttk.Labelframe(parent,text='SiK Ports',padding=8)
        ports_frame.pack(fill='x', padx=8)
        self.ports_tree=ttk.Treeview(ports_frame,columns=('port','baud','autobaud'),show='headings',height=4,selectmode='extended')
        self.ports_tree.heading('port',text='Serial Port'); self.ports_tree.heading('baud',text='Baud'); self.ports_tree.heading('autobaud',text='Auto-baud')
        self.ports_tree.column('port',width=320,anchor='w'); self.ports_tree.column('baud',width=100,anchor='center'); self.ports_tree.column('autobaud',width=100,anchor='center')
        self.ports_tree.pack(side='left',fill='x',expand=True)
        btns_port=ttk.Frame(ports_frame); btns_port.pack(side='right',fill='y')
        ttk.Button(btns_port,text='Add Port…',command=self.on_add_port).pack(fill='x',padx=5,pady=2)
        ttk.Button(btns_port,text='Remove Selected',command=self.on_remove_port).pack(fill='x',padx=5,pady=2)
        ttk.Button(btns_port,text='Refresh System Ports',command=self.on_refresh_system_ports).pack(fill='x',padx=5,pady=2)

        ctrl=ttk.Frame(parent,padding=8); ctrl.pack(fill='x')
        ttk.Label(ctrl,text='Discover (s)').grid(row=0,column=0,sticky='w'); ttk.Entry(ctrl,textvariable=self.discover_s,width=8).grid(row=0,column=1,sticky='w')
        ttk.Label(ctrl,text='Retries').grid(row=0,column=2,sticky='e'); ttk.Entry(ctrl,textvariable=self.retries,width=6).grid(row=0,column=3,sticky='w')
        ttk.Label(ctrl,text='Timeout (s)').grid(row=0,column=4,sticky='e'); ttk.Entry(ctrl,textvariable=self.timeout_s,width=6).grid(row=0,column=5,sticky='w')
        ttk.Checkbutton(ctrl,text='Clear previous mission before upload',variable=self.clear_first).grid(row=0,column=6,sticky='w',padx=12)
        ttk.Checkbutton(ctrl,text='Arm + AUTO + Start',variable=self.arm_start).grid(row=0,column=7,sticky='w',padx=12)
        ttk.Label(ctrl,text='Stagger (s)').grid(row=0,column=8,sticky='e'); ttk.Entry(ctrl,textvariable=self.stagger_s,width=6).grid(row=0,column=9,sticky='w')

        actions=ttk.Frame(parent,padding=(8,0)); actions.pack(fill='x')
        ttk.Button(actions,text='Discover via All Ports',command=self.on_discover).pack(side='left',padx=5)
        ttk.Button(actions,text='Assign Mission…',command=self.on_assign).pack(side='left',padx=5)
        self.btn_upload=ttk.Button(actions,text='Upload Missions (Parallel)',command=self.on_upload); self.btn_upload.pack(side='left',padx=5)
        self.btn_start=ttk.Button(actions,text='Start Missions',command=self.on_start,state=DISABLED); self.btn_start.pack(side='left',padx=5)
        self.btn_stop=ttk.Button(actions,text='Stop',command=self.on_stop,state=DISABLED); self.btn_stop.pack(side='left',padx=5)

        mid=ttk.Frame(parent,padding=8); mid.pack(fill='both',expand=True)
        self.tree=ttk.Treeview(mid,columns=('sysid','mission','status','port'),show='headings',height=12,selectmode='extended')
        self.tree.heading('sysid',text='SYSID'); self.tree.heading('mission',text='Mission File'); self.tree.heading('status',text='Status'); self.tree.heading('port',text='Discovered via Port')
        self.tree.column('sysid',width=80,anchor='center'); self.tree.column('mission',width=700,anchor='w'); self.tree.column('status',width=260,anchor='w'); self.tree.column('port',width=220,anchor='w')
        self.tree.pack(fill='x',pady=5)

        self.progress=ttk.Progressbar(mid,mode='determinate'); self.progress.pack(fill='x',pady=4)
        ttk.Label(mid,text='Log:').pack(anchor='w')
        self.log=Text(mid,height=16); self.log.pack(fill='both',expand=True)

    # ---------- Health Tab ----------
    def _build_tab_health(self, parent):
        top=ttk.Frame(parent,padding=8); top.pack(fill='x')
        ttk.Button(top,text='Start Monitoring',command=self.health_start).pack(side='left',padx=5)
        ttk.Button(top,text='Stop Monitoring',command=self.health_stop).pack(side='left',padx=5)
        ttk.Label(top,text='(Monitors RADIO/RADIO_STATUS from each configured port)').pack(side='left',padx=12)

        self.health_tree=ttk.Treeview(parent,columns=('port','rssi','remrssi','noise','rxerr','updated'),show='headings',height=14)
        for c,t,w in [('port','Port',340),('rssi','RSSI',100),('remrssi','Remote RSSI',120),('noise','Noise',100),('rxerr','RX Errors',100),('updated','Updated',160)]:
            self.health_tree.heading(c,text=t); self.health_tree.column(c,width=w,anchor='center' if c!='port' else 'w')
        self.health_tree.pack(fill='both',expand=True,padx=8,pady=8)
        self._health_threads=[]; self._health_alive=False; self._health_rows={}

    def health_start(self):
        self.health_stop()
        if not self.ports:
            messagebox.showinfo('Radio Health','Add at least one port on the Uploader tab.'); return
        self._health_alive=True; self._health_threads=[]
        for cfg in self.ports:
            t=threading.Thread(target=self._health_worker,args=(cfg,),daemon=True); self._health_threads.append(t); t.start()

    def health_stop(self): self._health_alive=False

    def _health_worker(self, cfg):
        for b in ([cfg['baud']] if not cfg['autobaud'] else [57600,115200]):
            if not self._health_alive: return
            ep=f"serial:{cfg['port']},{b}"
            try:
                m=mavlink_connect(ep)
            except Exception as e:
                self.log_write(f'[health] {ep} connect error: {e}'); continue
            self.log_write(f'[health] Monitoring {ep}')
            while self._health_alive:
                msg=m.recv_match(blocking=False)
                if not msg: time.sleep(0.05); continue
                if msg.get_type() in ('RADIO_STATUS', 'RADIO'):
                    vals=(getattr(msg,'rssi',None),getattr(msg,'remrssi',None),getattr(msg,'noise',None),getattr(msg,'rxerrors',None))
                    self._health_update_row(ep, *vals)
            break

    def _health_update_row(self, ep, rssi, remrssi, noise, rxerr):
        if not hasattr(self,'health_tree'): return
        label=ep.split('serial:')[1]
        # find row by label
        for iid in self.health_tree.get_children():
            v=self.health_tree.item(iid,'values')
            if v and v[0]==label:
                self.health_tree.item(iid, values=(label, rssi, remrssi, noise, rxerr, time.strftime('%H:%M:%S')))
                return
        self.health_tree.insert('', 'end', values=(label, rssi, remrssi, noise, rxerr, time.strftime('%H:%M:%S')))

    # ---------- Live Map Tab ----------
    def _build_tab_map(self, parent):
        top=ttk.Frame(parent,padding=8); top.pack(fill='x')
        ttk.Button(top,text='Start Live View',command=self.map_start).pack(side='left',padx=5)
        ttk.Button(top,text='Stop',command=self.map_stop).pack(side='left',padx=5)
        ttk.Button(top,text='Clear',command=self.map_clear).pack(side='left',padx=5)
        ttk.Label(top,text='(Streams HEARTBEAT/GPS from configured ports)').pack(side='left',padx=12)

        status=ttk.Labelframe(parent,text='Vehicle Status',padding=8)
        status.pack(fill='x',padx=8,pady=(0,8))
        cols=('sysid','status','lat','lon','alt','source','updated')
        self.map_tree=ttk.Treeview(status,columns=cols,show='headings',height=8)
        headings=[('sysid','SYSID',70,'center'),('status','Status',220,'w'),('lat','Latitude',120,'center'),
                  ('lon','Longitude',120,'center'),('alt','Alt (m)',90,'center'),('source','Port',200,'w'),('updated','Updated',120,'center')]
        for key,title,width,anchor in headings:
            self.map_tree.heading(key,text=title)
            self.map_tree.column(key,width=width,anchor=anchor)
        vsb=ttk.Scrollbar(status,orient='vertical',command=self.map_tree.yview)
        self.map_tree.configure(yscrollcommand=vsb.set)
        self.map_tree.pack(side='left',fill='both',expand=True)
        vsb.pack(side='right',fill='y')

        canvas_frame=ttk.Frame(parent,padding=(8,0,8,8))
        canvas_frame.pack(fill='both',expand=True)
        self.live_fig=Figure(figsize=(6,4), dpi=100)
        self.live_ax=self.live_fig.add_subplot(111)
        self.live_canvas=FigureCanvasTkAgg(self.live_fig, master=canvas_frame)
        self.live_canvas_widget=self.live_canvas.get_tk_widget()
        self.live_canvas_widget.pack(fill='both',expand=True)
        self._map_reset_axes()

    def map_start(self):
        self.map_stop()
        if not self.ports:
            messagebox.showinfo('Live Map','Add at least one SiK port on the Uploader tab.'); return
        self.map_clear()
        self._map_alive=True; self._map_threads=[]
        self.log_write('[map] Starting live telemetry monitor...')
        for cfg in self.ports:
            t=threading.Thread(target=self._map_worker,args=(cfg,),daemon=True)
            self._map_threads.append(t); t.start()

    def map_stop(self):
        if not self._map_alive:
            return
        self._map_alive=False
        self.log_write('[map] Live telemetry monitor stopping...')

    def map_clear(self):
        self._map_positions.clear(); self._map_rows.clear(); self._map_last_emit.clear()
        if hasattr(self,'map_tree'):
            self.map_tree.delete(*self.map_tree.get_children())
        self._map_reset_axes(); self._map_dirty=False

    def _map_reset_axes(self):
        if not hasattr(self,'live_ax'):
            return
        self.live_ax.clear()
        self.live_ax.set_xlabel('Latitude'); self.live_ax.set_ylabel('Longitude'); self.live_ax.set_title('Live Vehicle Positions')
        self.live_ax.grid(True, linestyle='--', linewidth=0.5)
        try:
            self.live_ax.set_aspect('equal', adjustable='datalim')
        except Exception:
            pass
        if hasattr(self,'live_canvas'):
            self.live_canvas.draw_idle()

    def _map_worker(self, cfg):
        bauds=[cfg['baud']] if not cfg['autobaud'] else [57600,115200]
        for b in bauds:
            if not self._map_alive:
                return
            ep=f"serial:{cfg['port']},{b}"
            self.log_write(f'[map] Listening on {ep}')
            try:
                m=mavlink_connect(ep)
            except Exception as e:
                self.log_write(f'[map] {ep} connect error: {e}')
                continue
            while self._map_alive:
                msg=m.recv_match(blocking=False)
                if not msg:
                    time.sleep(0.1); continue
                mtype=msg.get_type()
                try:
                    sysid=msg.get_srcSystem()
                except Exception:
                    sysid=None
                if not sysid:
                    continue
                if mtype=='GLOBAL_POSITION_INT':
                    lat=getattr(msg,'lat',None); lon=getattr(msg,'lon',None)
                    if lat is None or lon is None:
                        continue
                    lat=lat/1e7; lon=lon/1e7
                    alt=getattr(msg,'relative_alt',getattr(msg,'alt',None))
                    if alt is not None:
                        alt=alt/1000.0
                    if self._should_emit(sysid,'pos',0.2):
                        self._map_queue.put(('position', sysid, lat, lon, alt, ep, 'Global Position'))
                elif mtype=='GPS_RAW_INT':
                    lat=getattr(msg,'lat',None); lon=getattr(msg,'lon',None)
                    if lat is None or lon is None:
                        continue
                    lat=lat/1e7; lon=lon/1e7
                    alt=getattr(msg,'alt',None)
                    if alt is not None:
                        alt=alt/1000.0
                    fix_type=getattr(msg,'fix_type',None)
                    status=f'GPS Raw ({self._describe_fix(fix_type)})'
                    if self._should_emit(sysid,'gps',0.5):
                        self._map_queue.put(('position', sysid, lat, lon, alt, ep, status))
                elif mtype=='HEARTBEAT':
                    if self._should_emit(sysid,'hb',1.0):
                        status=self._describe_heartbeat(msg)
                        self._map_queue.put(('status', sysid, status, ep))
            try:
                m.close()
            except Exception:
                pass
            return
        self.log_write(f'[map] No telemetry received via {cfg["port"]}')

    def _should_emit(self, sysid, kind, interval):
        key=(sysid,kind)
        now=time.time(); last=self._map_last_emit.get(key,0)
        if now-last>=interval:
            self._map_last_emit[key]=now
            return True
        return False

    def _map_process_queue(self):
        try:
            while True:
                item=self._map_queue.get_nowait()
                if not item:
                    continue
                if item[0]=='position':
                    _, sysid, lat, lon, alt, ep, status=item
                    self._map_handle_position(sysid, lat, lon, alt, ep, status)
                elif item[0]=='status':
                    _, sysid, status, ep=item
                    self._map_handle_status(sysid, status, ep)
        except queue.Empty:
            pass
        self._map_draw_if_dirty()
        self.root.after(400,self._map_process_queue)

    def _map_handle_position(self, sysid, lat, lon, alt, ep, status):
        data=self._map_positions.get(sysid,{})
        data.update({'lat':lat,'lon':lon,'alt':alt,'source':self._format_source(ep),'timestamp':time.time(),'status':status})
        self._map_positions[sysid]=data
        self._update_map_tree(sysid,data)
        self._map_dirty=True

    def _map_handle_status(self, sysid, status, ep):
        data=self._map_positions.get(sysid,{})
        data.update({'status':status,'source':self._format_source(ep),'timestamp':time.time()})
        self._map_positions[sysid]=data
        self._update_map_tree(sysid,data)

    def _update_map_tree(self, sysid, data=None):
        if not hasattr(self,'map_tree'):
            return
        if data is None:
            data=self._map_positions.get(sysid,{})
        lat=data.get('lat'); lon=data.get('lon'); alt=data.get('alt')
        lat_txt=f'{lat:.6f}' if lat is not None else '—'
        lon_txt=f'{lon:.6f}' if lon is not None else '—'
        alt_txt=f'{alt:.1f}' if alt is not None else '—'
        status=data.get('status','')
        src=data.get('source','')
        ts=time.strftime('%H:%M:%S', time.localtime(data.get('timestamp',time.time())))
        vals=(sysid, status, lat_txt, lon_txt, alt_txt, src, ts)
        iid=self._map_rows.get(sysid)
        if iid and self.map_tree.exists(iid):
            self.map_tree.item(iid, values=vals)
        else:
            self._map_rows[sysid]=self.map_tree.insert('', 'end', values=vals)

    def _map_draw_if_dirty(self):
        if not self._map_dirty:
            return
        self._map_draw_points(); self._map_dirty=False

    def _map_draw_points(self):
        if not hasattr(self,'live_ax'):
            return
        self.live_ax.clear()
        self.live_ax.set_xlabel('Latitude'); self.live_ax.set_ylabel('Longitude'); self.live_ax.set_title('Live Vehicle Positions')
        self.live_ax.grid(True, linestyle='--', linewidth=0.5)
        pts=[(data.get('lat'), data.get('lon'), sid) for sid,data in self._map_positions.items() if data.get('lat') is not None and data.get('lon') is not None]
        if pts:
            xs=[p[0] for p in pts]; ys=[p[1] for p in pts]
            self.live_ax.scatter(xs, ys, c='tab:blue', s=50)
            for lat, lon, sid in pts:
                self.live_ax.text(lat, lon, str(sid), fontsize=9, ha='left', va='bottom')
            try:
                self.live_ax.set_aspect('equal', adjustable='datalim')
            except Exception:
                pass
            self.live_ax.margins(0.1)
        self.live_canvas.draw_idle()

    def _format_source(self, ep):
        if isinstance(ep,str) and ep.startswith('serial:'):
            return ep.split('serial:')[1]
        return ep

    def _describe_fix(self, fix_type):
        mapping={1:'No Fix',2:'2D',3:'3D',4:'DGPS',5:'RTK Float',6:'RTK Fixed'}
        return mapping.get(fix_type,'Unknown')

    def _describe_heartbeat(self, msg):
        try:
            mode=pymavutil.mode_string_v10(msg)
        except Exception:
            mode='UNKNOWN'
        try:
            armed=bool(msg.base_mode & pymavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED)
        except Exception:
            armed=None
        if armed is None:
            return f'Heartbeat ({mode})'
        state='ARMED' if armed else 'DISARMED'
        return f'{mode} ({state})'

    # ---------- Generator Tab (with Map Preview) ----------
    def _build_tab_generator(self, parent):
        top=ttk.Frame(parent,padding=8); top.pack(fill='x')
        self.template_path=StringVar(value='')
        ttk.Label(top,text='Template .waypoints:').grid(row=0,column=0,sticky='w')
        ttk.Entry(top,textvariable=self.template_path,width=60).grid(row=0,column=1,sticky='we',padx=5)
        ttk.Button(top,text='Browse…',command=self.on_pick_template).grid(row=0,column=2,sticky='w')

        mode_frame=ttk.Frame(parent,padding=8); mode_frame.pack(fill='x')
        self.mode=StringVar(value='Line')
        ttk.Label(mode_frame,text='Pattern:').pack(side='left')
        ttk.Radiobutton(mode_frame,text='Line',variable=self.mode,value='Line').pack(side='left',padx=6)
        ttk.Radiobutton(mode_frame,text='Circle',variable=self.mode,value='Circle').pack(side='left',padx=6)

        params=ttk.Labelframe(parent,text='Parameters',padding=8); params.pack(fill='x',padx=8)
        self.dx=DoubleVar(value=10.0); self.dy=DoubleVar(value=0.0); self.alt_dz=DoubleVar(value=0.0); self.radius=DoubleVar(value=20.0)
        ttk.Label(params,text='dx per index (m)').grid(row=0,column=0,sticky='e'); ttk.Entry(params,textvariable=self.dx,width=8).grid(row=0,column=1,sticky='w')
        ttk.Label(params,text='dy per index (m)').grid(row=0,column=2,sticky='e'); ttk.Entry(params,textvariable=self.dy,width=8).grid(row=0,column=3,sticky='w')
        ttk.Label(params,text='Altitude delta (m)').grid(row=0,column=4,sticky='e'); ttk.Entry(params,textvariable=self.alt_dz,width=8).grid(row=0,column=5,sticky='w')
        ttk.Label(params,text='Circle radius (m)').grid(row=1,column=0,sticky='e'); ttk.Entry(params,textvariable=self.radius,width=8).grid(row=1,column=1,sticky='w')

        out=ttk.Labelframe(parent,text='Output',padding=8); out.pack(fill='x',padx=8)
        self.out_dir=StringVar(value=str((Path.cwd()/ 'generated_missions').resolve()))
        ttk.Label(out,text='Save to folder:').grid(row=0,column=0,sticky='w')
        ttk.Entry(out,textvariable=self.out_dir,width=60).grid(row=0,column=1,sticky='we',padx=5)
        ttk.Button(out,text='Choose…',command=self.on_pick_outdir).grid(row=0,column=2,sticky='w')

        actions=ttk.Frame(parent,padding=8); actions.pack(fill='x')
        ttk.Button(actions,text='Generate for Selected SYSIDs',command=self.on_generate_for_selected).pack(side='left',padx=5)
        ttk.Button(actions,text='Assign to Selected (no save)',command=self.on_assign_generated_nosave).pack(side='left',padx=5)
        ttk.Button(actions,text='Preview on Map',command=self.on_preview_map).pack(side='left',padx=5)
        ttk.Button(actions,text='Clear Preview',command=self._clear_preview).pack(side='left',padx=5)

        # Help
        helpbox=Text(parent,height=6); helpbox.pack(fill='x',padx=8,pady=6)
        helpbox.insert(END, """Select SYSIDs on the Uploader tab, choose a template mission, pick Line or Circle pattern.
Line: applies dx,dy meters * index per SYSID; optional altitude delta.
Circle: evenly spaces SYSIDs on a circle of radius (m) around the first waypoint.
Generated files save as mission_sysidXX.waypoints; mapping updates automatically.
""")
        helpbox.configure(state=DISABLED)

        # Map preview
        self._init_map_preview(parent)

    # ----- Map preview helpers -----
    def _init_map_preview(self, parent):
        self.fig = Figure(figsize=(6,4), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.ax.set_xlabel('Latitude'); self.ax.set_ylabel('Longitude'); self.ax.set_title('Mission Preview')
        self.canvas = FigureCanvasTkAgg(self.fig, master=parent)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(fill='both', expand=True, padx=8, pady=8)

    def _clear_preview(self):
        if hasattr(self,'ax'):
            self.ax.clear(); self.ax.set_xlabel('Latitude'); self.ax.set_ylabel('Longitude'); self.ax.set_title('Mission Preview')
            self.ax.grid(True, linestyle='--', linewidth=0.5)
            self.canvas.draw_idle()

    def _plot_items(self, items, label=None):
        if not items: return
        xs=[it['x'] for it in items]; ys=[it['y'] for it in items]
        self.ax.plot(xs, ys, marker='o', linewidth=1.5)
        if label is not None:
            self.ax.text(xs[0], ys[0], str(label))
        try:
            self.ax.set_aspect('equal', adjustable='datalim')
        except Exception:
            pass
        self.ax.grid(True, linestyle='--', linewidth=0.5)

    def on_preview_map(self):
        base=self._load_template()
        if base is None: return
        sids=self._get_selected_sysids()
        if not sids:
            messagebox.showinfo('Preview','Select SYSIDs on the Uploader tab first.'); return
        self._clear_preview()
        for idx, sid in enumerate(sids):
            items=self._generate_items(base, idx, len(sids))
            self._plot_items(items, label=sid)
        self.canvas.draw_idle()

    # ----- Template / generation helpers -----
    def on_pick_template(self):
        fn=filedialog.askopenfilename(title='Choose template (.waypoints)', filetypes=[('Waypoints','*.waypoints'),('All','*.*')])
        if fn: self.template_path.set(fn)

    def on_pick_outdir(self):
        d=filedialog.askdirectory(title='Choose output folder')
        if d: self.out_dir.set(d)

    def _get_selected_sysids(self):
        sids=[]; 
        for iid in self.tree.selection():
            v=self.tree.item(iid,'values')
            if v: sids.append(int(v[0]))
        return sorted(sids)

    def _load_template(self):
        p=Path(self.template_path.get()).expanduser()
        if not p.exists():
            messagebox.showerror('Template','Pick a valid .waypoints template first.'); return None
        try:
            return read_qgc_wpl(p)
        except Exception as e:
            messagebox.showerror('Template', f'Failed to read template: {e}'); return None

    def _generate_items(self, base_items, index, total):
        if self.mode.get()=='Line':
            dx=self.dx.get()*index; dy=self.dy.get()*index; dz=self.alt_dz.get()
            return apply_offset(base_items, dx_m=dx, dy_m=dy, dz_m=dz)
        else:
            r=self.radius.get(); angle=(index/max(1,total))*2*math.pi
            dx=r*math.cos(angle); dy=r*math.sin(angle); dz=self.alt_dz.get()
            return apply_offset(base_items, dx_m=dx, dy_m=dy, dz_m=dz)

    def on_generate_for_selected(self):
        base=self._load_template()
        if base is None: return
        sids=self._get_selected_sysids()
        if not sids:
            messagebox.showinfo('Generate','Select SYSIDs on the Uploader tab first.'); return
        outdir=Path(self.out_dir.get()).expanduser(); outdir.mkdir(parents=True, exist_ok=True)
        for idx, sid in enumerate(sids):
            items=self._generate_items(base, idx, len(sids))
            fn=outdir / f'mission_sysid{sid}.waypoints'
            write_qgc_wpl(fn, items)
            self.mapping[sid]=str(fn.resolve())
            self._update_tree_cell(sid,'mission', self.mapping[sid])
        self.log_write(f'Generated missions for SYSIDs {sids} into {outdir}')
        messagebox.showinfo('Generate', 'Missions generated and assigned.')

    def on_assign_generated_nosave(self):
        base=self._load_template()
        if base is None: return
        sids=self._get_selected_sysids()
        if not sids:
            messagebox.showinfo('Assign','Select SYSIDs on the Uploader tab first.'); return
        tmpdir=Path(self.out_dir.get()).expanduser(); tmpdir.mkdir(parents=True, exist_ok=True)
        for idx, sid in enumerate(sids):
            items=self._generate_items(base, idx, len(sids))
            fn=tmpdir / f'mission_sysid{sid}.waypoints'
            write_qgc_wpl(fn, items)
            self.mapping[sid]=str(fn.resolve())
            self._update_tree_cell(sid,'mission', self.mapping[sid])
        self.log_write(f'Assigned generated missions (no explicit save path) for SYSIDs {sids} into {self.out_dir.get()}')

    # ----- Shared utils (uploader) -----
    def on_refresh_system_ports(self):
        if list_ports is None:
            self._system_ports=[]; self.log_write('pyserial not available; cannot enumerate ports.'); return
        ports=[p.device for p in list_ports.comports()]; self._system_ports=ports
        self.log_write(f'Available ports: {ports}')

    def _port_picker_dialog(self):
        dlg=Toplevel(self.root); dlg.title('Add Port'); dlg.grab_set()
        ttk.Label(dlg,text='Select a serial port:').pack(anchor='w',padx=10,pady=(10,4))
        lb=Listbox(dlg,selectmode=SINGLE,height=8)
        choices=[p for p in getattr(self,'_system_ports',[]) if all(d['port']!=p for d in self.ports)]
        for p in choices: lb.insert(END,p)
        lb.pack(fill='both',expand=True,padx=10)
        frm=ttk.Frame(dlg); frm.pack(fill='x',padx=10,pady=8)
        ttk.Label(frm,text='Baud:').grid(row=0,column=0,sticky='e'); baud_var=IntVar(value=57600)
        baud_box=ttk.Combobox(frm,textvariable=baud_var,values=COMMON_BAUDS,state='readonly',width=10); baud_box.grid(row=0,column=1,sticky='w',padx=6)
        autobaud_var=BooleanVar(value=True); ttk.Checkbutton(frm,text='Auto-baud 57600→115200',variable=autobaud_var).grid(row=0,column=2,sticky='w',padx=6)
        btns=ttk.Frame(dlg); btns.pack(fill='x',padx=10,pady=(0,10))
        result={'value':None}
        def do_ok():
            sel=lb.curselection()
            if not sel:
                messagebox.showinfo('Add Port','Select a port from the list.'); return
            result['value']=(lb.get(sel[0]), int(baud_var.get()), bool(autobaud_var.get()))
            dlg.destroy()
        def do_cancel(): dlg.destroy()
        ttk.Button(btns,text='Add',command=do_ok).pack(side='left'); ttk.Button(btns,text='Cancel',command=do_cancel).pack(side='right')
        dlg.wait_window(); return result['value']

    def on_add_port(self):
        if not hasattr(self,'_system_ports') or not self._system_ports:
            self.on_refresh_system_ports()
            if not self._system_ports:
                messagebox.showinfo('Add Port','No system ports found.'); return
        picked=self._port_picker_dialog()
        if not picked: return
        port, baud, autobaud = picked
        self.ports.append({'port':port,'baud':baud,'autobaud':autobaud})
        self.ports_tree.insert('', 'end', values=(port, baud, 'Yes' if autobaud else 'No'))
        self.log_write(f'Added port: {port} (baud {baud}, autobaud {autobaud})')

    def on_remove_port(self):
        sel=self.ports_tree.selection()
        for iid in sel:
            vals=self.ports_tree.item(iid,'values'); port=vals[0]
            self.ports=[d for d in self.ports if d['port']!=port]
            self.ports_tree.delete(iid)
            self.log_write(f'Removed port: {port}')

    def on_load_csv(self):
        path=self.map_file.get().strip()
        if not path:
            path=filedialog.askopenfilename(title='Load mapping CSV', filetypes=[('CSV','*.csv')])
            if not path: return
            self.map_file.set(path)
        try:
            with open(path,newline='') as f:
                rdr=csv.DictReader(f); self.mapping.clear()
                for row in rdr: self.mapping[int(row['sysid'])]=os.path.abspath(os.path.expanduser(row['mission_file']))
        except Exception as e:
            messagebox.showerror('Load CSV', f'Failed to load: {e}'); return
        for iid in self.tree.get_children():
            sid=int(self.tree.item(iid,'values')[0])
            if sid in self.mapping:
                self.tree.set(iid,'mission', self.mapping[sid])
        self.log_write(f'Loaded mapping CSV: {path}')

    def on_save_csv(self):
        path=self.map_file.get().strip()
        if not path:
            path=filedialog.asksaveasfilename(title='Save mapping CSV as', defaultextension='.csv', filetypes=[('CSV','*.csv')])
            if not path: return
            self.map_file.set(path)
        try:
            with open(path,'w',newline='') as f:
                w=csv.writer(f); w.writerow(['sysid','mission_file'])
                for sid,mpath in sorted(self.mapping.items()):
                    w.writerow([sid, mpath])
            self.log_write(f'Saved mapping CSV to {path}')
        except Exception as e:
            messagebox.showerror('Save CSV', f'Failed to save: {e}')

    def on_assign(self):
        sel=self.tree.selection()
        if not sel:
            messagebox.showinfo('Assign Mission','Select one or more SYSIDs in the table first.'); return
        fn=filedialog.askopenfilename(title='Choose mission (.waypoints)', filetypes=[('Waypoints','*.waypoints'),('All','*.*')])
        if not fn: return
        for iid in sel:
            sid=int(self.tree.item(iid,'values')[0])
            self.mapping[sid]=os.path.abspath(fn); self.tree.set(iid,'mission', self.mapping[sid])
            self._update_tree_cell(sid,'status','Assigned')
        self.log_write(f'Assigned mission: {fn} to SYSIDs {[int(self.tree.item(iid,"values")[0]) for iid in sel]}')

    def on_discover(self):
        self.tree.delete(*self.tree.get_children()); self.progress['value']=0; self.stop_flag.clear()
        self.discovered.clear(); self.discovered_port.clear()
        if not self.ports:
            messagebox.showinfo('Discover','Add at least one SiK port first.'); return
        threading.Thread(target=self._discover_thread,daemon=True).start()

    def _discover_on_port(self, port, baud, autobaud, out_q, dur):
        try:
            if autobaud:
                for b in [57600,115200]:
                    if self.stop_flag.is_set(): return
                    ep=f'serial:{port},{b}'; self.log_write(f'Trying {ep} ...')
                    try:
                        m=mavlink_connect(ep); found=wait_heartbeat_all(m,timeout=3.0)
                        if found:
                            self.log_write(f'Auto-baud success on {port} at {b}; SYSIDs {sorted(found.keys())}')
                            extra=wait_heartbeat_all(m,timeout=dur)
                            for k,v in (found|extra).items(): out_q.put((k,v,ep))
                            return
                    except Exception as e:
                        self.log_write(f'{port}@{b} error: {e}')
                self.log_write(f'Auto-baud failed on {port}')
            else:
                ep=f'serial:{port},{baud}'; self.log_write(f'Connecting {ep} ...')
                m=mavlink_connect(ep); found=wait_heartbeat_all(m,timeout=dur)
                for k,v in found.items(): out_q.put((k,v,ep))
        except Exception as e:
            self.log_write(f'{port} discover error: {e}')

    def _discover_thread(self):
        dur=float(self.discover_s.get()); out_q=queue.Queue(); threads=[]
        for cfg in self.ports:
            t=threading.Thread(target=self._discover_on_port,args=(cfg['port'],cfg['baud'],cfg['autobaud'],out_q,dur),daemon=True)
            threads.append(t); t.start()
        alive=True; start=time.time()
        while alive and time.time()-start < dur+6:
            alive=any(t.is_alive() for t in threads)
            try:
                sid,comp,ep=out_q.get(timeout=0.2)
                self.discovered[sid]=comp; self.discovered_port[sid]=ep
                self.tree.insert('', 'end', values=(sid, self.mapping.get(sid,''), 'Discovered', ep.split('serial:')[1]))
            except queue.Empty:
                pass
        self.log_write(f'Discovered SYSIDs: {sorted(self.discovered.keys())}')
        self.btn_start.config(state='disabled')

    def on_upload(self):
        if not self.discovered:
            messagebox.showinfo('Upload','Discover vehicles first.'); return
        self.stop_flag.clear(); self.btn_stop.config(state='normal'); self.btn_start.config(state='disabled')
        threading.Thread(target=self._upload_thread,daemon=True).start()

    def _upload_worker(self, ep, targets, missions, retries, timeout_s, clear):
        self.log_write(f'[{ep}] Uploading to SYSIDs {targets}')
        try:
            m=mavlink_connect(ep)
        except Exception as e:
            self.log_write(f'[{ep}] connect error: {e}'); return False
        ok=True
        for sid in targets:
            if self.stop_flag.is_set(): self.log_write('Stopped by user.'); return False
            compid=self.discovered[sid]; self._update_tree_cell(sid,'status','Uploading mission...')
            try:
                mission_upload_pref_float(m,sid,compid,missions[sid],retries=retries,timeout=timeout_s,clear_first=clear,on_log=self.log_write)
                self._update_tree_cell(sid,'status','Upload OK')
            except Exception as e:
                ok=False; self._update_tree_cell(sid,'status',f'Upload FAILED: {e}')
        return ok

    def _upload_thread(self):
        targets=[sid for sid in sorted(self.discovered.keys()) if sid in self.mapping]
        if not targets:
            self.log_write('No SYSIDs have missions assigned. Use "Assign Mission…" or the Generator tab.'); return
        missions={}
        for sid in targets:
            try:
                p=Path(self.mapping[sid]).expanduser().resolve()
                missions[sid]=read_qgc_wpl(p); self.log_write(f'[sysid {sid}] Parsed {len(missions[sid])} items from {p.name}')
            except Exception as e:
                self.log_write(f'[sysid {sid}] Parse error: {e}'); return
        groups={}
        for sid in targets:
            ep=self.discovered_port.get(sid); groups.setdefault(ep,[]).append(sid)
        retries=int(self.retries.get()); timeout_s=float(self.timeout_s.get()); clear=bool(self.clear_first.get())
        threads=[]; res_q=queue.Queue()
        for ep, sids in groups.items():
            t=threading.Thread(target=lambda: res_q.put(self._upload_worker(ep,sids,missions,retries,timeout_s,clear)),daemon=True)
            threads.append(t); t.start()
        for t in threads: t.join()
        all_ok=True
        while not res_q.empty(): all_ok=all_ok and bool(res_q.get())
        if all_ok:
            self.btn_start.config(state='normal'); self.log_write("All uploads OK across ports. 'Start Missions' is enabled.")
        else:
            self.btn_start.config(state='disabled')
        self.btn_stop.config(state='disabled')

    def on_start(self):
        if not self.arm_start.get():
            self.log_write('Arm + AUTO + Start is OFF. Not starting missions.'); return
        self.stop_flag.clear(); self.btn_stop.config(state='normal')
        threading.Thread(target=self._start_thread,daemon=True).start()

    def _start_thread(self):
        groups={}
        for sid in sorted(self.discovered.keys()):
            ep=self.discovered_port.get(sid); groups.setdefault(ep,[]).append(sid)
        if not groups:
            self.log_write('No groups/ports to start.'); return
        stagger=float(self.stagger_s.get())
        for ep, sids in groups.items():
            try: m=mavlink_connect(ep)
            except Exception as e: self.log_write(f'[{ep}] connect error: {e}'); continue
            for i,sid in enumerate(sids,1):
                if self.stop_flag.is_set(): self.log_write('Stopped by user.'); break
                self._update_tree_cell(sid,'status','ARM + AUTO + START ...')
                try: arm_and_start(m,sid); self._update_tree_cell(sid,'status','Started')
                except Exception as e: self._update_tree_cell(sid,'status',f'Start FAILED: {e}')
                if stagger>0 and i<len(sids): time.sleep(stagger)
        self.btn_stop.config(state='disabled')

    def on_stop(self): self.stop_flag.set(); self.log_write('Stop requested...')

    def _update_tree_cell(self,sid,col,val):
        for iid in self.tree.get_children():
            vals=self.tree.item(iid,'values')
            if vals and int(vals[0])==int(sid):
                self.tree.set(iid,col,val); break

def main():
    root=Tk(); app=SwarmGUI(root); root.mainloop()
if __name__=='__main__':
    main()
