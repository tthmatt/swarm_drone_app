
from typing import Dict, Tuple, Optional, List
from pathlib import Path
import time, math
from pymavlink import mavutil

MAV_MISSION_TYPE_MISSION = 0

def parse_endpoint(endpoint: str):
    if endpoint.startswith('serial:'):
        path_baud = endpoint.split('serial:')[1]
        if ',' in path_baud:
            path, baud = path_baud.split(',', 1)
            return ('serial', path, int(baud))
        return ('serial', path_baud, 115200)
    return ('net', endpoint, None)

def mavlink_connect(endpoint: str):
    kind, addr, baud = parse_endpoint(endpoint)
    if kind == 'serial':
        m = mavutil.mavlink_connection(addr, baud=baud)
    else:
        m = mavutil.mavlink_connection(addr)
    try:
        m.mav.request_data_stream_send(0, 0, mavutil.mavlink.MAV_DATA_STREAM_ALL, 2, 1)
    except Exception:
        pass
    return m

def wait_heartbeat_all(m, timeout: float = 10.0) -> Dict[int,int]:
    start=time.time(); found={}
    while time.time()-start<timeout:
        msg=m.recv_match(blocking=False)
        if not msg:
            time.sleep(0.05); continue
        if msg.get_type()=='HEARTBEAT':
            found[msg.get_srcSystem()]=msg.get_srcComponent()
    return found

def read_qgc_wpl(path: Path):
    lines=[ln.strip() for ln in path.read_text().splitlines() if ln.strip() and not ln.strip().startswith('#')]
    if not lines: raise ValueError(f'Mission file {path} is empty')
    header=lines[0]; items=[]
    for ln in lines[1:] if 'QGC WPL 110' in header else lines:
        parts=ln.split()
        if len(parts)<12: raise ValueError(f'Bad waypoint line: {ln}')
        idx=int(parts[0]); current=int(parts[1]); frame=int(parts[2]); command=int(parts[3])
        p1,p2,p3,p4=[float(parts[i]) for i in range(4,8)]
        x=float(parts[8]); y=float(parts[9]); z=float(parts[10])
        autocontinue=int(parts[11]) if len(parts)>11 else 1
        items.append(dict(seq=idx,frame=frame,command=command,current=current,autocontinue=autocontinue,
                          p1=p1,p2=p2,p3=p3,p4=p4,x=x,y=y,z=z))
    return items

def write_qgc_wpl(path: Path, items: List[dict]):
    lines=['QGC WPL 110']
    for it in items:
        fields=[it['seq'], it['current'], it['frame'], it['command'], it['p1'], it['p2'], it['p3'], it['p4'], it['x'], it['y'], it['z'], it['autocontinue']]
        lines.append("\t".join(str(v) for v in fields))
    path.write_text("\n".join(lines))

def _send_clear_all(m, sysid:int, compid:int, on_log=None):
    try:
        m.mav.mission_clear_all_send(sysid, compid, MAV_MISSION_TYPE_MISSION)
        if on_log: on_log(f'[sysid {sysid}] Sent MISSION_CLEAR_ALL')
    except Exception as e:
        if on_log: on_log(f'[sysid {sysid}] CLEAR_ALL error: {e}')

def _upload_proto_float(m, sysid:int, compid:int, items, timeout:float, on_log=None):
    count=len(items)
    try:
        m.mav.mission_count_send(sysid, compid, count, MAV_MISSION_TYPE_MISSION, 0)
    except TypeError:
        m.mav.mission_count_send(sysid, compid, count)
    if on_log: on_log(f'[sysid {sysid}] FLOAT: MISSION_COUNT={count} type=MISSION')
    next_seq=0; deadline=time.time()+timeout
    while next_seq<count:
        msg=m.recv_match(type=['MISSION_REQUEST','MISSION_ACK','RADIO_STATUS'],blocking=False)
        if msg is None:
            if time.time()>deadline:
                try:
                    m.mav.mission_count_send(sysid, compid, count, MAV_MISSION_TYPE_MISSION, 0)
                except TypeError:
                    m.mav.mission_count_send(sysid, compid, count)
                deadline=time.time()+timeout
            time.sleep(0.05); continue
        t=msg.get_type()
        if t=='RADIO_STATUS' and on_log:
            try: on_log(f'[radio] rssi={msg.rssi} remrssi={msg.remrssi} noise={msg.noise} rxerrors={msg.rxerrors}')
            except Exception: pass
            continue
        if t=='MISSION_ACK':
            if next_seq>=count:
                if on_log: on_log(f'[sysid {sysid}] FLOAT: Mission ACK'); return True
            else: continue
        if t=='MISSION_REQUEST':
            req=msg.seq
            if on_log: on_log(f'[sysid {sysid}] FLOAT: REQUEST seq={req}')
            if req!=next_seq: next_seq=req
            it=items[next_seq]
            m.mav.mission_item_send(sysid, compid, it['seq'], it['frame'], it['command'], it['current'], it['autocontinue'],
                                    it['p1'], it['p2'], it['p3'], it['p4'], it['x'], it['y'], it['z'])
            if on_log: on_log(f'[sysid {sysid}] FLOAT: SENT seq={next_seq}')
            next_seq+=1; deadline=time.time()+timeout
    end=time.time()+timeout
    while time.time()<end:
        msg=m.recv_match(type='MISSION_ACK',blocking=False)
        if msg:
            if on_log: on_log(f'[sysid {sysid}] FLOAT: Mission complete.'); return True
        time.sleep(0.05)
    return False

def _upload_proto_int(m, sysid:int, compid:int, items, timeout:float, on_log=None):
    count=len(items)
    try:
        m.mav.mission_count_send(sysid, compid, count, MAV_MISSION_TYPE_MISSION, 0)
    except TypeError:
        m.mav.mission_count_send(sysid, compid, count)
    if on_log: on_log(f'[sysid {sysid}] INT: MISSION_COUNT={count} type=MISSION')
    next_seq=0; deadline=time.time()+timeout
    from pymavlink import mavutil as _mv
    while next_seq<count:
        msg=m.recv_match(type=['MISSION_REQUEST_INT','MISSION_ACK','RADIO_STATUS'],blocking=False)
        if msg is None:
            if time.time()>deadline:
                try:
                    m.mav.mission_count_send(sysid, compid, count, MAV_MISSION_TYPE_MISSION, 0)
                except TypeError:
                    m.mav.mission_count_send(sysid, compid, count)
                deadline=time.time()+timeout
            time.sleep(0.05); continue
        t=msg.get_type()
        if t=='RADIO_STATUS' and on_log:
            try: on_log(f'[radio] rssi={msg.rssi} remrssi={msg.remrssi} noise={msg.noise} rxerrors={msg.rxerrors}')
            except Exception: pass
            continue
        if t=='MISSION_ACK':
            if next_seq>=count:
                if on_log: on_log(f'[sysid {sysid}] INT: Mission ACK'); return True
            else: continue
        if t=='MISSION_REQUEST_INT':
            req=msg.seq
            if on_log: on_log(f'[sysid {sysid}] INT: REQUEST seq={req}')
            if req!=next_seq: next_seq=req
            it=items[next_seq]
            if it['frame'] in (_mv.mavlink.MAV_FRAME_GLOBAL, _mv.mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT, _mv.mavlink.MAV_FRAME_GLOBAL_TERRAIN_ALT):
                xi=int(it['x']*1e7); yi=int(it['y']*1e7)
            else:
                xi=int(it['x']*100); yi=int(it['y']*100)
            zi=int(it['z']*100)
            m.mav.mission_item_int_send(sysid, compid, it['seq'], it['frame'], it['command'], it['current'], it['autocontinue'],
                                        it['p1'], it['p2'], it['p3'], it['p4'], xi, yi, zi, MAV_MISSION_TYPE_MISSION)
            if on_log: on_log(f'[sysid {sysid}] INT: SENT seq={next_seq}')
            next_seq+=1; deadline=time.time()+timeout
    end=time.time()+timeout
    while time.time()<end:
        msg=m.recv_match(type='MISSION_ACK',blocking=False)
        if msg:
            if on_log: on_log(f'[sysid {sysid}] INT: Mission complete.'); return True
        time.sleep(0.05)
    return False

def mission_upload_pref_float(m, sysid:int, compid:int, items, retries:int=3, timeout:float=6.0, clear_first:bool=True, on_log=None):
    for attempt in range(1, retries+1):
        if clear_first:
            _send_clear_all(m, sysid, compid, on_log=on_log); time.sleep(0.2)
        if on_log: on_log(f'[sysid {sysid}] Attempt {attempt}/{retries} using FLOAT protocol first')
        ok=_upload_proto_float(m, sysid, compid, items, timeout, on_log=on_log)
        if ok: return
        if on_log: on_log(f'[sysid {sysid}] FLOAT path timed out; trying INT fallback')
        ok=_upload_proto_int(m, sysid, compid, items, timeout, on_log=on_log)
        if ok: return
        if on_log: on_log(f'[sysid {sysid}] INT path also timed out; retrying...')
    raise TimeoutError(f'Mission upload failed after {retries} attempts for sysid {sysid}')

def set_mode_auto(m, target_sys:int, custom_mode:int=3):
    m.mav.set_mode_send(target_sys, mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED, custom_mode)

def arm_and_start(m, target_sys:int):
    m.mav.command_long_send(target_sys,0,mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,0,1,0,0,0,0,0,0)
    import time as _t; _t.sleep(0.5)
    set_mode_auto(m,target_sys,custom_mode=3); _t.sleep(0.2)
    m.mav.command_long_send(target_sys,0,mavutil.mavlink.MAV_CMD_MISSION_START,0,0,0,0,0,0,0,0)

# --- Mission generator helpers ---
def meters_to_deg(lat0, dx_m, dy_m):
    dlat = dy_m / 111111.0
    dlon = dx_m / (111111.0 * max(0.1, math.cos(math.radians(lat0))))
    return dlat, dlon

def apply_offset(items, dx_m=0.0, dy_m=0.0, dz_m=0.0):
    if not items: return items
    lat0 = items[0]['x']
    dlat, dlon = meters_to_deg(lat0, dx_m, dy_m)
    out=[]
    for it in items:
        it2=it.copy()
        it2['x']=it['x'] + dlat
        it2['y']=it['y'] + dlon
        it2['z']=it['z'] + dz_m
        out.append(it2)
    return out
