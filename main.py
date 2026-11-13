# -*- coding: utf-8 -*-
"""
SERVANT_OF_ALLAH - محمّل فيديو مبني على yt-dlp (يعمل على GitHub Actions خلف Cloudflare Worker)
نفس الملف في Repo-A و Repo-B.
"""
import glob
import json
import os
import re
import shutil
import signal
import subprocess
import threading
import time
import uuid

from flask import Flask, Response, g, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
try:
    app.json.sort_keys = False
except Exception:
    pass
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ----------------------------------------------------------------- الإعدادات
PORT = int(os.environ.get("PORT", "8090"))
DATA_DIR = os.path.abspath("data")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")
JOBS_FILE = os.path.join(DATA_DIR, "jobs.json")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

PROXY = os.environ.get("YTDLP_PROXY", "").strip()           # يضبطه run.yml (http://127.0.0.1:8118 أو بروكسي مخصص)
WARP_ROTATE = os.environ.get("WARP_ROTATE", "0") == "1"     # تدوير هوية WARP عند الحظر
WARP_PORT = os.environ.get("WARP_PORT", "40000")
DIRECT_LANE = os.environ.get("DIRECT_LANE", "1") == "1"     # مسار مباشر بدون بروكسي (سرعة 1.6 Gbps)

FILE_TTL = int(os.environ.get("FILE_TTL", "1200"))          # الملف يُحذف بعد 20 دقيقة من اكتماله
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "3"))     # عدد المهام المتوازية
MAX_ENCODES = int(os.environ.get("MAX_ENCODES", "2"))       # عمليات الترميز (قص) المتوازية
MAX_USER_ACTIVE = int(os.environ.get("MAX_USER_ACTIVE", "3"))
MAX_USER_JOBS = int(os.environ.get("MAX_USER_JOBS", "30"))
FRAGMENTS = os.environ.get("CONCURRENT_FRAGMENTS", "16")    # للبث المباشر/المقاطع المجزأة
HTTP_CHUNK = os.environ.get("HTTP_CHUNK_SIZE", "10M")       # كسر تحديد سرعة يوتيوب
ARIA_CONN = os.environ.get("ARIA_CONN", "16")               # 16 اتصال متوازي لـ aria2c لكل ملف
ENC_PRESET = os.environ.get("ENC_PRESET", "ultrafast")
ENC_CRF = os.environ.get("ENC_CRF", "21")
WAIT_CLEAN_SEC = int(os.environ.get("WAIT_CLEAN_SEC", "600"))
BAD_RECHECK_SEC = int(os.environ.get("BAD_RECHECK_SEC", "25"))
OK_RECHECK_SEC = int(os.environ.get("OK_RECHECK_SEC", "300"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "8"))
PREFLIGHT_URL = os.environ.get("PREFLIGHT_URL", "https://www.youtube.com/watch?v=jNQXAC9IVRw")
SPEED_URL = "https://speed.cloudflare.com/__down?bytes=100000000"
COOKIE_NAME = "uid"

# مجموعات عملاء يوتيوب (تستفيد من PO Token على المسار المباشر والبروكسي)
YT_CLIENT_SETS = [
    [],
    ["--extractor-args", "youtube:player_client=tv,web_embedded"],
    ["--extractor-args", "youtube:player_client=tv_downgraded,web_safari,mweb"],
    ["--extractor-args", "youtube:player_client=android_vr,visionos,ios"],
]

# ----------------------------------------------------------------- الحالة
jobs = {}
jobs_lock = threading.RLock()
_file_lock = threading.Lock()
procs = {}                          # job_id -> Popen
stop_req = {}                       # job_id -> "save" | "cancel"
slots = threading.BoundedSemaphore(MAX_PARALLEL)
enc_sem = threading.BoundedSemaphore(MAX_ENCODES)
rotate_lock = threading.Lock()
_last_rotate = 0.0
lanes = {}                          # name -> dict
lanes_lock = threading.RLock()
monitor_wake = threading.Event()
state = {"preflight": "pending", "ytdlp": "", "pot": None, "rotations": 0}


def log(msg):
    print(msg, flush=True)


# ----------------------------------------------------------------- تخزين المهام
def save_jobs():
    with jobs_lock:
        snap = json.dumps(jobs, ensure_ascii=False)
    with _file_lock:
        try:
            tmp = JOBS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(snap)
            os.replace(tmp, JOBS_FILE)
        except Exception as e:
            log(f"[jobs] save error: {e}")


def load_jobs():
    data = {}
    if os.path.exists(JOBS_FILE):
        try:
            with open(JOBS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    now = time.time()
    for jid, j in list(data.items()):
        if not j.get("owner"):
            data.pop(jid, None)
            continue
        if j.get("status") in ("queued", "running"):
            j["status"] = "failed"
            j["error"] = "انقطع التحميل أثناء تبديل/إعادة تشغيل السيرفر. أعد المحاولة."
            j["finished_at"] = now
        if j.get("status") == "finished":
            fn = j.get("filename")
            if not fn or not os.path.exists(os.path.join(DOWNLOAD_DIR, fn)):
                j["status"] = "expired"
    with jobs_lock:
        jobs.update(data)
    save_jobs()


def update_job(job_id, **kw):
    with jobs_lock:
        j = jobs.get(job_id)
        if j is None:
            return False
        j.update(kw)
    return True


# ----------------------------------------------------------------- أدوات مساعدة
def normalize_url(u):
    u = (u or "").strip()
    if not u or any(c.isspace() for c in u) or u.startswith("-"):
        return ""
    if u.startswith("ttps://"):
        u = "h" + u
    elif u.startswith("tps://"):
        u = "ht" + u
    elif u.startswith("ps://"):
        u = "htt" + u
    elif not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    return u


def is_youtube(url):
    return bool(re.search(r"(youtube\.com|youtu\.be|youtube-nocookie\.com)", url or "", re.I))


def parse_hms(s):
    try:
        parts = [int(x) for x in str(s).strip().split(":")]
    except Exception:
        return None
    if not 1 <= len(parts) <= 3 or any(p < 0 for p in parts):
        return None
    sec = 0
    for p in parts:
        sec = sec * 60 + p
    return sec


def safe_title(t):
    t = re.sub(r'[\\/:*?"<>|\r\n\t]+', " ", t or "").strip(" .")
    return re.sub(r"\s+", " ", t)[:120]


TEMP_SUFFIX = (".part", ".ytdl", ".temp", ".tmp", ".json", ".txt", ".aria2")


def find_output(job_id):
    best = None
    for p in glob.glob(os.path.join(DOWNLOAD_DIR, f"video_{job_id}.*")):
        name = os.path.basename(p)
        if name.endswith(TEMP_SUFFIX) or re.search(r"\.f\d+\.", name) or ".part-" in name:
            continue
        try:
            if best is None or os.path.getsize(p) > os.path.getsize(best):
                best = p
        except OSError:
            pass
    return best


def remove_job_files(job_id):
    for pat in (f"video_{job_id}*", f"title_{job_id}*"):
        for p in glob.glob(os.path.join(DOWNLOAD_DIR, pat)):
            try:
                os.remove(p)
            except Exception:
                pass


def read_title(job_id):
    try:
        with open(os.path.join(DOWNLOAD_DIR, f"title_{job_id}.txt"), encoding="utf-8", errors="replace") as f:
            return safe_title(f.readline())
    except Exception:
        return ""


def run_proc(job_id, cmd, on_line=None):
    """يشغّل أمر ويقرأ مخرجاته سطراً سطراً. قابل للإيقاف عبر stop_job."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            encoding="utf-8", errors="replace", bufsize=1, start_new_session=True)
    procs[job_id] = proc
    lines = []
    try:
        for line in proc.stdout:
            s = line.rstrip("\n")
            if not s.strip():
                continue
            lines.append(s)
            if len(lines) > 400:
                lines.pop(0)
            if on_line:
                on_line(s.strip())
    finally:
        proc.wait()
        procs.pop(job_id, None)
    return proc.returncode, lines


def err_text(lines):
    errs = [s.strip() for s in lines if s.strip().startswith("ERROR")]
    return " | ".join(errs[-3:]) or " | ".join(s.strip()[:300] for s in lines[-3:])


# ----------------------------------------------------------------- الشبكة والمسارات (Lanes)
def net_args(proxy):
    a = ["--socket-timeout", "30", "--retries", "8"]
    if shutil.which("deno"):
        a += ["--js-runtimes", "deno"]
    if shutil.which("node"):
        a += ["--js-runtimes", "node"]
    if proxy:
        a += ["--proxy", proxy]
    return a


def curl_base(proxy, timeout):
    cmd = ["curl", "-s", "--max-time", str(timeout)]
    if proxy:
        cmd += ["-x", proxy]
    return cmd


def curl_trace(proxy, timeout=12):
    try:
        out = subprocess.run(curl_base(proxy, timeout) + ["https://www.cloudflare.com/cdn-cgi/trace"],
                             capture_output=True, text=True, timeout=timeout + 3).stdout
    except Exception:
        return {}
    res = dict(l.split("=", 1) for l in out.splitlines() if "=" in l)
    return {"warp": res.get("warp", "?"), "loc": res.get("loc", "?")}


def speed_test(proxy):
    """قياس سرعة تقريبي (Mbps) لمسار معين عبر ملف اختبار من Cloudflare."""
    try:
        out = subprocess.run(curl_base(proxy, 12) + ["-o", "/dev/null", "-w", "%{speed_download}", SPEED_URL],
                             capture_output=True, text=True, timeout=20).stdout.strip()
        return round(float(out) * 8 / 1e6, 1)
    except Exception:
        return None


def add_lane(name, proxy, rotate=False):
    with lanes_lock:
        lanes[name] = {"name": name, "proxy": proxy, "rotate": rotate, "yt_ok": None, "checked": 0.0,
                       "next_check": 0.0, "fails": 0, "mbps": None, "speed_at": 0.0,
                       "client_idx": 0, "last_error": "", "egress": {}}


def _warp(*args, timeout=40):
    try:
        r = subprocess.run(["warp-cli", "--accept-tos", *args], capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, (r.stdout + r.stderr).strip()
    except Exception as e:
        return False, str(e)


def rotate_warp_ip(reason=""):
    """يغيّر هوية WARP للحصول على IP جديد دون المساس بترافيك Tailscale و Cloudflared."""
    global _last_rotate
    if not (WARP_ROTATE and shutil.which("warp-cli")):
        return False
    with rotate_lock:
        if time.time() - _last_rotate < 25:
            return True
        log(f"[warp] rotating identity ({reason})")
        _warp("disconnect")
        _warp("registration", "delete")
        if not _warp("mode", "proxy")[0]:
            _warp("set-mode", "proxy")
        if not _warp("proxy", "port", WARP_PORT)[0]:
            _warp("set-proxy-port", WARP_PORT)
        ok = False
        for i in range(4):
            ok, msg = _warp("registration", "new")
            if not ok:
                ok, msg = _warp("register")
            if ok:
                break
            log(f"[warp] registration failed ({i + 1}/4): {msg[:120]}")
            time.sleep(6 * (i + 1))
        if not _warp("mode", "proxy")[0]:
            _warp("set-mode", "proxy")
        if not _warp("proxy", "port", WARP_PORT)[0]:
            _warp("set-proxy-port", WARP_PORT)
        _warp("connect")
        for _ in range(25):
            if "Connected" in _warp("status")[1]:
                break
            time.sleep(1)
        _last_rotate = time.time()
        state["rotations"] += 1
        time.sleep(2)
        return ok


def is_blocked(text):
    return bool(re.search(r"confirm you.{0,3}re not a bot|HTTP Error 429|Too Many Requests|"
                          r"content isn.t available.{0,30}try again later|Sign in to confirm", text or "", re.I))


def is_format_problem(text):
    return bool(re.search(r"Requested format is not available|Only images are available|"
                          r"No video formats found|nsig extraction failed", text or "", re.I))


FATALS = [
    (re.compile(r"confirm your age|age[- ]restricted|age verification", re.I),
     "الفيديو مقيّد بالعمر ويحتاج تسجيل دخول (غير ممكن بدون كوكيز)"),
    (re.compile(r"Private video", re.I), "هذا الفيديو خاص"),
    (re.compile(r"members[- ]only|Join this channel", re.I), "فيديو خاص بأعضاء القناة فقط"),
    (re.compile(r"live event will begin|Premieres in|will begin in", re.I),
     "البث/العرض الأول لم يبدأ بعد، جرّب عند بدايته"),
    (re.compile(r"Video unavailable|has been removed|account.*terminated", re.I), "الفيديو غير متاح أو محذوف"),
    (re.compile(r"not available in your country|blocked it in your country|geo.?restrict", re.I),
     "الفيديو محجوب في بلد السيرفر الحالي"),
    (re.compile(r"Unsupported URL", re.I), "الرابط غير مدعوم"),
]


def fatal_message(text):
    for rx, msg in FATALS:
        if rx.search(text or ""):
            return msg
    return None


def probe_youtube(proxy):
    """يجرّب يوتيوب عبر مسار معين بكل مجموعات العملاء. يرجع (نجح؟، رقم المجموعة، نص الخطأ)."""
    last = ""
    for idx, extra in enumerate(YT_CLIENT_SETS):
        cmd = ["yt-dlp", "--ignore-config", "--no-playlist", "--no-warnings", "--print", "%(title)s"]
        cmd += net_args(proxy) + extra + ["--", PREFLIGHT_URL]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            last = "timeout"
            continue
        except Exception as e:
            last = str(e)
            continue
        if r.returncode == 0 and r.stdout.strip():
            return True, idx, ""
        last = r.stdout + r.stderr
    return False, 0, last


def check_lane(l):
    """يفحص مسار؛ لو فشل ومسار WARP يغيّر الـ IP ويعيد الفحص (حتى 3 مرات)."""
    tries = 3 if l["rotate"] else 1
    ok, idx, out = False, 0, ""
    for t in range(tries):
        ok, idx, out = probe_youtube(l["proxy"])
        if ok:
            break
        log(f"[lane {l['name']}] youtube probe failed ({t + 1}/{tries}): {out.strip()[-120:]}")
        if l["rotate"] and t < tries - 1:
            rotate_warp_ip(f"lane {l['name']} probe failed")
    now = time.time()
    with lanes_lock:
        l["checked"] = now
        if ok:
            l.update(yt_ok=True, client_idx=idx, last_error="", fails=0, next_check=now + OK_RECHECK_SEC)
        else:
            l["fails"] += 1
            l.update(yt_ok=False, last_error=out.strip()[-240:],
                     next_check=now + min(BAD_RECHECK_SEC * l["fails"], 300))
    if ok and now - l["speed_at"] > 1800:
        mbps = speed_test(l["proxy"])
        with lanes_lock:
            l["mbps"], l["speed_at"] = mbps, time.time()
        l["egress"] = curl_trace(l["proxy"]) if l["proxy"] else {}
    return ok


def mark_lane_bad(lane, err):
    with lanes_lock:
        lane["yt_ok"] = False
        lane["checked"] = time.time()
        lane["next_check"] = 0.0
        lane["last_error"] = (err or "")[-240:]
    monitor_wake.set()


def lane_monitor():
    """مراقب دائم: يفحص المسارات دورياً ويصلح المحظور منها تلقائياً."""
    try:
        r = subprocess.run(["yt-dlp", "--version"], capture_output=True, text=True, timeout=20)
        state["ytdlp"] = r.stdout.strip()
    except Exception:
        state["ytdlp"] = "?"
    try:
        r = subprocess.run(["curl", "-s", "--max-time", "4", "http://127.0.0.1:4416/ping"],
                           capture_output=True, text=True, timeout=8)
        state["pot"] = bool(r.stdout.strip())
    except Exception:
        state["pot"] = False

    state["preflight"] = "running"
    ths = [threading.Thread(target=check_lane, args=(l,), daemon=True) for l in list(lanes.values())]
    for t in ths:
        t.start()
    for t in ths:
        t.join(timeout=170)
    state["preflight"] = "done"
    log(f"[monitor] first pass done: {lane_summary()}")

    while True:
        monitor_wake.wait(timeout=15)
        monitor_wake.clear()
        now = time.time()
        for l in list(lanes.values()):
            if now >= l["next_check"]:
                try:
                    check_lane(l)
                except Exception as e:
                    log(f"[monitor] {l['name']}: {e}")


def lane_summary():
    with lanes_lock:
        return [{"name": l["name"], "yt_ok": l["yt_ok"], "mbps": l["mbps"], "fails": l["fails"],
                 "egress": l["egress"], "last_error": l["last_error"][-120:]} for l in lanes.values()]


def pick_lane(url, exclude=()):
    """يختار أسرع مسار متاح (direct بـ 1.6Gbps أولاً إذا نجح، وإلا proxy)."""
    with lanes_lock:
        cand = [l for l in lanes.values() if l["name"] not in exclude]
        if is_youtube(url):
            good = [l for l in cand if l["yt_ok"] is True]
            if good:
                return max(good, key=lambda l: (10000 if l["name"] == "direct" else 0) + (l["mbps"] or 0))
            unknown = [l for l in cand if l["yt_ok"] is None]
            return unknown[0] if unknown else None
        for name in ("direct", "proxy"):
            for l in cand:
                if l["name"] == name:
                    return l
        return cand[0] if cand else None


# ----------------------------------------------------------------- بناء أوامر التحميل
def format_args(quality, audio_only):
    if audio_only:
        return ["-f", "ba/b", "-x", "--audio-format", "mp3", "--audio-quality", "0"]
    sort = ["-S", "res,vcodec:h264,acodec:aac", "--merge-output-format", "mp4/mkv"]
    if quality == "best":
        return ["-f", "bv*+ba/b"] + sort
    h = int(quality)
    return ["-f", f"bv*[height<={h}]+ba/b[height<={h}]/bv*+ba/b"] + sort


def aria_args():
    n = ARIA_CONN
    return ["--downloader", "aria2c", "--downloader", "dash,m3u8:native",
            "--downloader-args", f"aria2c:-x{n} -s{n} -k1M --max-tries=5 --retry-wait=2"]


PCT_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
SPEED_RE = re.compile(r"at\s+(\S+/s)")
ETA_RE = re.compile(r"ETA\s+(\S+)")
SIZE_RE = re.compile(r"\[download\]\s+(~?\s*[\d.]+\s*\w*B)\s+at")
ARIA_RE = re.compile(r"\((\d+)%\).*?DL:(\S+)(?:.*?ETA:(\S+))?")


def handle_line(job_id, s):
    m = PCT_RE.search(s)
    if m:
        sp, eta = SPEED_RE.search(s), ETA_RE.search(s)
        update_job(job_id, progress=f"{float(m.group(1)):.1f}%", phase="جاري التحميل",
                   speed=sp.group(1) if sp else "", eta=eta.group(1) if eta else "")
        return
    m = ARIA_RE.search(s)
    if m and s.startswith("["):
        update_job(job_id, progress=f"{m.group(1)}%", phase="جاري التحميل", speed=m.group(2) + "/s",
                   eta=m.group(3) or "")
        return
    m = SIZE_RE.search(s)
    if m:
        sp = SPEED_RE.search(s)
        update_job(job_id, progress=m.group(1).replace(" ", ""), phase="جاري التسجيل/التحميل",
                   speed=sp.group(1) if sp else "", eta="")
        return
    if s.startswith("[Merger]") or "Merging formats" in s:
        update_job(job_id, phase="جاري دمج الصوت والصورة", speed="", eta="")
    elif s.startswith("[ExtractAudio]"):
        update_job(job_id, phase="جاري التحويل إلى mp3", speed="", eta="")
    elif s.startswith("[youtube]") or s.startswith("[info]"):
        update_job(job_id, phase="جاري تحليل الرابط")
    elif s.startswith("[download] Destination") or s.startswith("[hlsnative]") or s.startswith("[dashsegments]"):
        update_job(job_id, phase="جاري التحميل")


def client_args(url, lane, shift):
    if not is_youtube(url):
        return []
    idx = (lane.get("client_idx", 0) + shift) % len(YT_CLIENT_SETS)
    return YT_CLIENT_SETS[idx]


def ytdlp_download(job_id, data, lane, ctx):
    title_file = os.path.join(DOWNLOAD_DIR, f"title_{job_id}.txt")
    try:
        os.remove(title_file)
    except Exception:
        pass
    out_tpl = os.path.join(DOWNLOAD_DIR, f"video_{job_id}.%(ext)s")
    cmd = ["yt-dlp", "--ignore-config", "--no-playlist", "--newline", "--no-colors", "--no-mtime",
           "-N", FRAGMENTS, "--http-chunk-size", HTTP_CHUNK, "--fragment-retries", "12",
           "--print-to-file", "%(title)s", title_file]
    cmd += net_args(lane["proxy"]) + client_args(data["url"], lane, ctx["shift"])
    cmd += format_args(data["quality"], data["audio_only"])
    if ctx.get("aria"):
        cmd += aria_args()
    if data.get("live_from_start"):
        cmd.append("--live-from-start")
    cmd += ["-o", out_tpl, "--", data["url"]]
    rc, lines = run_proc(job_id, cmd, on_line=lambda s: handle_line(job_id, s))
    path = find_output(job_id) if rc == 0 else None
    log(f"[job {job_id}] yt-dlp lane={lane['name']} aria={bool(ctx.get('aria'))} rc={rc} {err_text(lines)[:160]}")
    return {"ok": bool(path), "path": path, "error": err_text(lines), "tail": lines[-14:],
            "title": read_title(job_id)}


# ----------------------------------------------------------------- القص (Segment)
def ffmpeg_progress_cb(job_id, dur):
    def cb(s):
        if s.startswith("out_time_us=") or s.startswith("out_time_ms="):
            try:
                t = int(s.split("=", 1)[1]) / 1e6
            except ValueError:
                return
            pct = min(99.0, max(0.0, t / dur * 100)) if dur > 0 else 0.0
            update_job(job_id, progress=f"{pct:.1f}%", phase="جاري قص وترميز الجزء المطلوب")
        elif s.startswith("speed="):
            update_job(job_id, speed=s.split("=", 1)[1].strip(), eta="")
    return cb


def build_ffmpeg_cut(inputs, start, dur, out, precise, audio_only, proxy):
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-progress", "pipe:1", "-nostats"]
    for inp in inputs:
        if inp.get("url"):
            if proxy and proxy.startswith("http"):
                cmd += ["-http_proxy", proxy]
            hdr = "".join(f"{k}: {v}\r\n" for k, v in (inp.get("headers") or {}).items())
            if hdr:
                cmd += ["-headers", hdr]
            cmd += ["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5"]
        cmd += ["-ss", str(start), "-i", inp["src"]]
    cmd += ["-t", str(dur)]
    if len(inputs) == 2:
        vi = next((i for i, x in enumerate(inputs) if x.get("kind") == "v"), 0)
        ai = next((i for i, x in enumerate(inputs) if x.get("kind") == "a"), 1)
        cmd += ["-map", f"{vi}:v:0", "-map", f"{ai}:a:0"]
    if audio_only:
        cmd += ["-vn", "-c:a", "libmp3lame", "-q:a", "0"]
    elif precise:
        cmd += ["-c:v", "libx264", "-preset", ENC_PRESET, "-crf", ENC_CRF, "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart"]
    else:
        cmd += ["-c", "copy", "-avoid_negative_ts", "make_zero", "-movflags", "+faststart"]
    cmd.append(out)
    return cmd


def seg_ext(data, src_ext="mp4"):
    if data["audio_only"]:
        return "mp3"
    return "mp4" if data.get("precise", True) else src_ext


def segment_fast(job_id, data, lane, ctx):
    """يحمّل الجزء المطلوب فقط عبر yt-dlp (--download-sections) مع كسر تحديد السرعة."""
    q = "1080" if data["quality"] == "best" else data["quality"]
    start, end = data["start"], data["end"]
    title_file = os.path.join(DOWNLOAD_DIR, f"title_{job_id}.txt")
    try:
        os.remove(title_file)
    except Exception:
        pass
    out_tpl = os.path.join(DOWNLOAD_DIR, f"video_{job_id}.%(ext)s")

    cmd = ["yt-dlp", "--ignore-config", "--no-playlist", "--newline", "--no-colors", "--no-mtime",
           "-N", FRAGMENTS, "--http-chunk-size", HTTP_CHUNK, "--fragment-retries", "12",
           "--download-sections", f"*{start}-{end}",
           "--print-to-file", "%(title)s", title_file]
    if data.get("precise", True) and not data["audio_only"]:
        cmd.append("--force-keyframes-at-cuts")
    cmd += net_args(lane["proxy"]) + client_args(data["url"], lane, ctx["shift"])
    cmd += format_args(q, data["audio_only"])
    cmd += ["-o", out_tpl, "--", data["url"]]

    update_job(job_id, phase="جاري تحميل وقص الجزء المطلوب", progress="0%")
    with enc_sem:
        rc, lines = run_proc(job_id, cmd, on_line=lambda s: handle_line(job_id, s))
    path = find_output(job_id) if rc == 0 else None
    err = err_text(lines)
    log(f"[job {job_id}] section-cut lane={lane['name']} rc={rc} {err[:160]}")
    if path and os.path.getsize(path) > 0:
        return {"ok": True, "path": path, "title": read_title(job_id), "tail": []}
    fm = fatal_message(err)
    if fm:
        return {"ok": False, "fatal": fm, "error": err, "tail": lines[-14:]}
    if is_blocked(err):
        return {"ok": False, "error": err, "tail": lines[-14:]}
    return {"ok": False, "fallback": True, "error": err, "tail": lines[-14:]}


def segment_via_full(job_id, data, lane, ctx):
    """خطة بديلة: تحميل الفيديو كاملاً بـ 16 اتصال aria2c ثم قص الجزء محلياً."""
    full = dict(data, mode="full")
    if full["quality"] == "best":
        full["quality"] = "1080"
    res = ytdlp_download(job_id, full, lane, ctx)
    if not res["ok"]:
        return res
    src = res["path"]
    out = os.path.join(DOWNLOAD_DIR, f"video_{job_id}.cut.{seg_ext(data, os.path.splitext(src)[1].lstrip('.') or 'mp4')}")
    dur = data["end"] - data["start"]
    update_job(job_id, phase="جاري قص الجزء المطلوب", progress="0%")
    ff = build_ffmpeg_cut([{"src": src}], data["start"], dur, out, data.get("precise", True),
                          data["audio_only"], None)
    with enc_sem:
        rc, flines = run_proc(job_id, ff, on_line=ffmpeg_progress_cb(job_id, dur))
    try:
        os.remove(src)
    except Exception:
        pass
    if rc == 0 and os.path.exists(out) and os.path.getsize(out) > 0:
        return {"ok": True, "path": out, "title": res.get("title", ""), "tail": []}
    return {"ok": False, "error": "فشل القص: " + err_text(flines), "tail": flines[-14:]}


def full_attempt(job_id, data, lane, ctx):
    return ytdlp_download(job_id, data, lane, ctx)


def segment_attempt(job_id, data, lane, ctx):
    res = segment_fast(job_id, data, lane, ctx)
    if res.get("fallback"):
        log(f"[job {job_id}] section cut unavailable -> full download + local cut")
        return segment_via_full(job_id, data, lane, ctx)
    return res


# ----------------------------------------------------------------- حلقة المحاولات
def attempt_loop(job_id, data, worker):
    url = data["url"]
    yt = is_youtube(url)
    deadline = time.time() + WAIT_CLEAN_SEC
    exclude = set()
    ctx = {"shift": 0, "aria": bool(shutil.which("aria2c"))}
    last_err, last_tail, n = "", [], 0
    while n < MAX_ATTEMPTS:
        if stop_req.get(job_id):
            return None
        lane = pick_lane(url, exclude)
        if lane is None:
            if yt and time.time() < deadline:
                update_job(job_id, phase="كل المسارات محظورة مؤقتاً، جاري تغيير الـ IP...", speed="", eta="")
                monitor_wake.set()
                time.sleep(4)
                continue
            if not yt and exclude:
                exclude.clear()
                n += 1
                continue
            break
        update_job(job_id, attempt=n + 1, lane=lane["name"], progress="0%", speed="", eta="")
        if n > 0:
            update_job(job_id, phase=f"إعادة المحاولة ({n + 1}) عبر مسار {lane['name']}")
        res = worker(job_id, data, lane, ctx)
        n += 1
        if stop_req.get(job_id):
            return None
        if res.get("ok"):
            return res
        err = res.get("error", "")
        last_err, last_tail = err, res.get("tail", [])
        if res.get("fatal"):
            return {"ok": False, "error": res["fatal"], "tail": last_tail}
        fm = fatal_message(err)
        if fm:
            return {"ok": False, "error": fm, "tail": last_tail}
        if is_blocked(err):
            if yt:
                mark_lane_bad(lane, err)
            else:
                exclude.add(lane["name"])
        elif is_format_problem(err):
            ctx["shift"] += 1
        else:
            ctx["aria"] = False
            ctx["shift"] += 1
            if not yt:
                exclude.add(lane["name"])
        time.sleep(2)
    if is_blocked(last_err) or (yt and last_err == ""):
        last_err = ("يوتيوب حظر الـ IP الحالي مؤقتاً. السيرفر بيغيّر الـ IP تلقائياً "
                    "في الخلفية، انتظر دقيقة وجرّب مرة أخرى.")
    return {"ok": False, "error": (last_err or "فشل غير معروف")[:400], "tail": last_tail}


# ----------------------------------------------------------------- تشغيل المهام
def salvage_partial(job_id, audio_only):
    parts = [p for p in glob.glob(os.path.join(DOWNLOAD_DIR, f"video_{job_id}*.part")) if os.path.getsize(p) > 0]
    if not parts:
        return None
    parts.sort(key=os.path.getsize, reverse=True)
    out = os.path.join(DOWNLOAD_DIR, f"video_{job_id}.partial.{'mp3' if audio_only else 'mp4'}")
    cmd = ["ffmpeg", "-y", "-loglevel", "error"]
    for p in parts[:2]:
        cmd += ["-i", p]
    cmd += (["-vn", "-c:a", "libmp3lame", "-q:a", "0"] if audio_only else ["-c", "copy", "-movflags", "+faststart"])
    cmd.append(out)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    except Exception:
        return None
    if r.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0:
        return out
    return None


def finish_ok(job_id, path, title=None):
    title = safe_title(title) if title else read_title(job_id)
    update_job(job_id, status="finished", filename=os.path.basename(path), display_name=title,
               progress="100%", phase="تم", speed="", eta="", finished_at=time.time(), error="", log=[])
    for p in glob.glob(os.path.join(DOWNLOAD_DIR, f"video_{job_id}*")):
        if os.path.abspath(p) != os.path.abspath(path):
            try:
                os.remove(p)
            except Exception:
                pass
    save_jobs()


def fail_job(job_id, msg, tail=None):
    update_job(job_id, status="failed", error=msg, phase="فشل", speed="", eta="",
               finished_at=time.time(), log=tail or [])
    save_jobs()


def run_job(job_id, data):
    try:
        with slots:
            if stop_req.get(job_id) == "cancel":
                return
            update_job(job_id, status="running", started=time.time(), phase="جاري التحليل", progress="0%")
            save_jobs()
            worker = segment_attempt if data["mode"] == "segment" else full_attempt
            res = attempt_loop(job_id, data, worker)
            req = stop_req.get(job_id)
            if req == "cancel":
                return
            if req == "save":
                out = salvage_partial(job_id, data["audio_only"]) or find_output(job_id)
                if out:
                    finish_ok(job_id, out)
                else:
                    fail_job(job_id, "تم الإيقاف لكن لا يوجد جزء مسجّل يمكن حفظه.")
                return
            if res and res.get("ok"):
                finish_ok(job_id, res["path"], res.get("title"))
            else:
                fail_job(job_id, (res or {}).get("error", "فشل غير معروف"), (res or {}).get("tail"))
    except Exception as e:
        log(f"[job {job_id}] exception: {e}")
        fail_job(job_id, f"خطأ داخلي: {e}")
    finally:
        stop_req.pop(job_id, None)
        procs.pop(job_id, None)
        with jobs_lock:
            exists = job_id in jobs
        if not exists:
            remove_job_files(job_id)
        else:
            try:
                os.remove(os.path.join(DOWNLOAD_DIR, f"title_{job_id}.txt"))
            except Exception:
                pass


def _escalate(p):
    for sig, wait in ((signal.SIGTERM, 15), (signal.SIGKILL, 10)):
        t0 = time.time()
        while time.time() - t0 < wait:
            if p.poll() is not None:
                return
            time.sleep(0.5)
        try:
            os.killpg(os.getpgid(p.pid), sig)
        except Exception:
            return


def stop_job(job_id, mode):
    with jobs_lock:
        j = jobs.get(job_id)
        active = bool(j) and j.get("status") in ("queued", "running")
    if not active:
        return False
    stop_req[job_id] = mode
    p = procs.get(job_id)
    if p is not None and p.poll() is None:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGINT if mode == "save" else signal.SIGTERM)
        except Exception:
            pass
        threading.Thread(target=_escalate, args=(p,), daemon=True).start()
    return True


def cleanup_loop():
    while True:
        try:
            now = time.time()
            changed = False
            with jobs_lock:
                for jid, j in list(jobs.items()):
                    st = j.get("status")
                    if st == "finished" and now - j.get("finished_at", now) > FILE_TTL:
                        remove_job_files(jid)
                        j["status"] = "expired"
                        changed = True
                    elif st in ("failed", "expired") and now - j.get("finished_at", j.get("created", now)) > 3600:
                        del jobs[jid]
                        changed = True
                active = {jid for jid, j in jobs.items() if j.get("status") in ("queued", "running", "finished")}
            for p in glob.glob(os.path.join(DOWNLOAD_DIR, "*")):
                m = re.search(r"(?:video|title)_([0-9a-f]{8,32})", os.path.basename(p))
                if m and m.group(1) in active:
                    continue
                try:
                    if now - os.path.getmtime(p) > 3 * 3600:
                        os.remove(p)
                except Exception:
                    pass
            if changed:
                save_jobs()
        except Exception as e:
            log(f"[cleanup] {e}")
        time.sleep(30)


# ----------------------------------------------------------------- هوية الزائر (كوكي عشوائي)
@app.before_request
def identify_visitor():
    uid = request.cookies.get(COOKIE_NAME, "")
    if re.fullmatch(r"[0-9a-f]{32}", uid):
        g.uid, g.new_uid = uid, False
    else:
        g.uid, g.new_uid = uuid.uuid4().hex, True


@app.after_request
def set_visitor_cookie(resp):
    if getattr(g, "new_uid", False) and request.path not in ("/api/health", "/og-image.png"):
        https = (request.headers.get("X-Forwarded-Proto", "").split(",")[0].strip() == "https"
                 or bool(request.headers.get("X-Public-Host")))
        resp.set_cookie(COOKIE_NAME, g.uid, max_age=60 * 60 * 24 * 365, httponly=True,
                        samesite="Lax", secure=https)
    return resp


def owned_job(job_id):
    with jobs_lock:
        j = jobs.get(job_id)
        return j if (j and j.get("owner") == g.uid) else None


# ----------------------------------------------------------------- الواجهة
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SERVANT_OF_ALLAH</title>
<meta property="og:title" content="SERVANT_OF_ALLAH">
<meta property="og:description" content="أداة فائقة السرعة لتحميل واقتطاع الفيديوهات بأعلى جودة">
<meta property="og:image" content="__HOST_URL__og-image.png">
<meta property="og:type" content="website">
<style>
  * { box-sizing: border-box; font-family: system-ui, -apple-system, sans-serif; }
  body { background-color: #0d131e; color: #f8fafc; margin: 0; padding: 20px; display: flex; flex-direction: column; align-items: center; min-height: 100vh; }
  .header-box { text-align: center; margin-bottom: 25px; max-width: 700px; width: 100%; }
  .site-title { color: #2dd4bf; font-size: 22px; font-weight: bold; margin-bottom: 20px; letter-spacing: 1px; }
  .quran-ayah { font-size: 26px; line-height: 1.8; color: #ffffff; font-weight: normal; margin-bottom: 12px; }
  .quran-meta { color: #94a3b8; font-size: 13px; margin-bottom: 15px; }
  .quran-trans { color: #cbd5e1; font-size: 14px; line-height: 1.6; max-width: 600px; margin: 0 auto; direction: ltr; }
  .divider { height: 1px; background: #1e293b; margin: 30px 0 20px 0; width: 100%; max-width: 700px; }
  .container { background-color: #161e2e; border-radius: 12px; padding: 24px; width: 100%; max-width: 520px; border: 1px solid #283548; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
  .main-head { text-align: center; font-size: 20px; font-weight: bold; color: #fff; margin-bottom: 20px; }
  .form-group { margin-bottom: 16px; }
  label { display: block; margin-bottom: 8px; color: #94a3b8; font-size: 14px; }
  input[type="text"], select { width: 100%; padding: 12px; background-color: #0d131e; border: 1px solid #283548; border-radius: 8px; color: #fff; font-size: 15px; outline: none; }
  input[type="text"]:focus, select:focus { border-color: #14b8a6; }
  .btn-tabs { display: flex; background: #0d131e; border-radius: 8px; padding: 4px; gap: 4px; margin-bottom: 16px; }
  .btn-tab { flex: 1; padding: 10px; border: none; background: transparent; color: #94a3b8; font-weight: bold; border-radius: 6px; cursor: pointer; font-size: 14px; }
  .btn-tab.active { background: #14b8a6; color: #0d131e; }
  .time-picker-group { background: #0d131e; border-radius: 8px; padding: 12px; margin-bottom: 16px; border: 1px solid #283548; display: none; }
  .time-row { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
  .time-row:last-child { margin-bottom: 0; }
  .time-label { width: 40px; color: #94a3b8; font-size: 14px; }
  .time-spinners { display: flex; align-items: center; gap: 6px; direction: ltr; }
  .spinner-box { display: flex; flex-direction: column; align-items: center; }
  .spinner-btn { background: none; border: none; color: #94a3b8; cursor: pointer; font-size: 12px; padding: 2px 6px; }
  .spinner-btn:hover { color: #14b8a6; }
  .spinner-val { background: #161e2e; border: 1px solid #283548; color: #fff; border-radius: 6px; width: 44px; height: 34px; text-align: center; line-height: 32px; font-weight: bold; font-size: 15px; }
  .checkbox-group { display: flex; align-items: center; gap: 8px; margin-bottom: 14px; color: #cbd5e1; font-size: 14px; cursor: pointer; }
  .checkbox-group input { width: 16px; height: 16px; accent-color: #14b8a6; cursor: pointer; }
  .checkbox-group label { margin: 0; cursor: pointer; color: #cbd5e1; }
  .btn-submit { width: 100%; padding: 14px; background: #2dd4bf; color: #0d131e; border: none; border-radius: 8px; font-size: 16px; font-weight: bold; cursor: pointer; }
  .btn-submit:hover { background: #14b8a6; }
  .btn-submit:disabled { opacity: 0.6; cursor: not-allowed; }
  .footer-note { text-align: center; color: #64748b; font-size: 12px; margin-top: 12px; }
  .srv-status { text-align: center; color: #475569; font-size: 11px; margin-top: 6px; direction: ltr; }
  .jobs-container { margin-top: 20px; background: #0d131e; border-radius: 8px; padding: 16px; border: 1px solid #283548; min-height: 60px; }
  .job-card { margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid #161e2e; word-break: break-all; font-size: 13px; }
  .job-card:last-child { margin-bottom: 0; padding-bottom: 0; border-bottom: none; }
  .job-url { color: #94a3b8; margin-bottom: 6px; text-align: left; }
  .job-status-running { color: #38bdf8; }
  .job-status-failed { color: #f87171; font-weight: bold; }
  .job-status-success { color: #4ade80; font-weight: bold; }
  .job-link { color: #2dd4bf; text-decoration: underline; font-weight: bold; margin-left: 8px; }
  .job-del { color: #94a3b8; cursor: pointer; text-decoration: underline; font-size: 12px; margin-left: 12px; }
  .job-actions { margin-top: 6px; }
  .bar { height: 6px; background: #161e2e; border-radius: 4px; margin: 8px 0 2px; overflow: hidden; }
  .bar-in { height: 100%; background: #2dd4bf; border-radius: 4px; transition: width .3s; }
  .job-log { margin-top: 6px; color: #64748b; font-size: 11px; direction: ltr; text-align: left; white-space: pre-wrap; }
  .empty-jobs { text-align: center; color: #64748b; font-size: 14px; margin: 10px 0; }
  .error-msg { color: #f87171; font-size: 13px; text-align: center; margin-top: 10px; display: none; }
  .conn-note { color: #fbbf24; font-size: 12px; text-align: center; margin-top: 10px; display: none; }
</style>
</head>
<body>

<div class="header-box">
  <div class="site-title">SERVANT_OF_ALLAH</div>
  <div class="quran-ayah">﴿ وَاتَّقُوا يَوْمًا تُرْجَعُونَ فِيهِ إِلَى اللَّهِ ۖ ثُمَّ تُوَفَّىٰ كُلُّ نَفْسٍ مَّا كَسَبَتْ وَهُمْ لَا يُظْلَمُونَ ﴾</div>
  <div class="quran-meta">سورة البقرة، الآية 281، الجزء 3، الصفحة 47</div>
  <div class="quran-trans">And be afraid of the Day when you shall be brought back to Allah. Then every person shall be paid what he earned, and they shall not be dealt with unjustly.</div>
</div>

<div class="divider"></div>

<div class="container">
  <div class="main-head">تحميل فيديو</div>

  <div class="form-group">
    <label>رابط الفيديو</label>
    <input type="text" id="url" placeholder="https://www.youtube.com/watch?v=..." />
  </div>

  <label>نوع التحميل</label>
  <div class="btn-tabs">
    <button class="btn-tab active" id="tab-full" onclick="setMode('full')">فيديو كامل</button>
    <button class="btn-tab" id="tab-segment" onclick="setMode('segment')">جزء من الفيديو</button>
  </div>

  <div id="time-picker" class="time-picker-group">
    <div class="time-row">
      <div class="time-label">من</div>
      <div class="time-spinners">
        <div class="spinner-box"><button class="spinner-btn" onclick="stepTime('s_h', 1)">▲</button><div class="spinner-val" id="s_h">00</div><button class="spinner-btn" onclick="stepTime('s_h', -1)">▼</button></div>
        <span style="color:#94a3b8;font-weight:bold;">:</span>
        <div class="spinner-box"><button class="spinner-btn" onclick="stepTime('s_m', 1)">▲</button><div class="spinner-val" id="s_m">00</div><button class="spinner-btn" onclick="stepTime('s_m', -1)">▼</button></div>
        <span style="color:#94a3b8;font-weight:bold;">:</span>
        <div class="spinner-box"><button class="spinner-btn" onclick="stepTime('s_s', 1)">▲</button><div class="spinner-val" id="s_s">00</div><button class="spinner-btn" onclick="stepTime('s_s', -1)">▼</button></div>
      </div>
    </div>
    <div class="time-row">
      <div class="time-label">إلى</div>
      <div class="time-spinners">
        <div class="spinner-box"><button class="spinner-btn" onclick="stepTime('e_h', 1)">▲</button><div class="spinner-val" id="e_h">00</div><button class="spinner-btn" onclick="stepTime('e_h', -1)">▼</button></div>
        <span style="color:#94a3b8;font-weight:bold;">:</span>
        <div class="spinner-box"><button class="spinner-btn" onclick="stepTime('e_m', 1)">▲</button><div class="spinner-val" id="e_m">03</div><button class="spinner-btn" onclick="stepTime('e_m', -1)">▼</button></div>
        <span style="color:#94a3b8;font-weight:bold;">:</span>
        <div class="spinner-box"><button class="spinner-btn" onclick="stepTime('e_s', 1)">▲</button><div class="spinner-val" id="e_s">00</div><button class="spinner-btn" onclick="stepTime('e_s', -1)">▼</button></div>
      </div>
    </div>
  </div>

  <div class="checkbox-group" id="precise-wrap" style="display:none">
    <input type="checkbox" id="precise" checked />
    <label for="precise">قص دقيق وصوت متزامن 100% (موصى به)</label>
  </div>

  <div class="form-group">
    <label>الجودة</label>
    <select id="quality">
      <option value="720" selected>720p</option>
      <option value="1080">1080p</option>
      <option value="480">480p</option>
      <option value="360">360p</option>
      <option value="best">أعلى جودة متاحة (في القص حتى 1080p)</option>
    </select>
  </div>

  <div class="checkbox-group">
    <input type="checkbox" id="audio_only" />
    <label for="audio_only">صوت فقط (mp3)</label>
  </div>

  <div class="checkbox-group">
    <input type="checkbox" id="live_start" />
    <label for="live_start">لو الرابط بث مباشر: ابدأ من أول البث</label>
  </div>

  <button class="btn-submit" id="btn-submit" onclick="submitDownload()">ابدأ التحميل</button>
  <div class="error-msg" id="err-box"></div>
  <div class="conn-note" id="conn-note"></div>
  <div class="footer-note">تحميلاتك تظهر لك أنت فقط. يُحذف الملف تلقائياً بعد 20 دقيقة من اكتماله.</div>
  <div class="srv-status" id="srv-status"></div>

  <div class="jobs-container" id="jobs-box">
    <div class="empty-jobs">لا توجد تحميلات حالياً. أضف رابطاً وابدأ.</div>
  </div>
</div>

<script>
let currentMode = 'full';
let pollTimer = null;
const $ = id => document.getElementById(id);
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&','<':'<','>':'>','"':'"',"'":'''}[c]));

function setMode(mode) {
  currentMode = mode;
  $('tab-segment').classList.toggle('active', mode === 'segment');
  $('tab-full').classList.toggle('active', mode === 'full');
  $('time-picker').style.display = (mode === 'segment') ? 'block' : 'none';
  $('precise-wrap').style.display = (mode === 'segment') ? 'flex' : 'none';
}

function stepTime(id, dir) {
  const el = $(id);
  const max = id.endsWith('_h') ? 99 : 59;
  let v = parseInt(el.innerText, 10) + dir;
  if (v < 0) v = max; else if (v > max) v = 0;
  el.innerText = String(v).padStart(2, '0');
}

function getTimeStr(p) { return $(p + '_h').innerText + ':' + $(p + '_m').innerText + ':' + $(p + '_s').innerText; }
function toSec(p) { return parseInt($(p + '_h').innerText, 10) * 3600 + parseInt($(p + '_m').innerText, 10) * 60 + parseInt($(p + '_s').innerText, 10); }

function normalizeUrl(u) {
  if (!u) return '';
  if (u.startsWith('ttps://')) return 'h' + u;
  if (u.startsWith('tps://')) return 'ht' + u;
  if (u.startsWith('ps://')) return 'htt' + u;
  if (!/^https?:\/\//i.test(u)) return 'https://' + u;
  return u;
}

function showErr(msg) { const b = $('err-box'); b.innerText = msg; b.style.display = 'block'; }

async function submitDownload() {
  $('err-box').style.display = 'none';
  const url = normalizeUrl($('url').value.trim());
  if (!url) { showErr('ضع رابط الفيديو أولاً'); return; }
  if (currentMode === 'segment' && toSec('e') <= toSec('s')) { showErr('وقت النهاية يجب أن يكون بعد وقت البداية'); return; }
  const data = {
    url: url, mode: currentMode,
    start_time: getTimeStr('s'), end_time: getTimeStr('e'),
    quality: $('quality').value, audio_only: $('audio_only').checked,
    live_from_start: $('live_start').checked, precise: $('precise').checked
  };
  const btn = $('btn-submit');
  btn.disabled = true;
  btn.innerText = 'جاري بدء التحميل...';
  try {
    const res = await fetch('/api/download', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
    let body = {};
    try { body = await res.json(); } catch (e) {}
    if (!res.ok) {
      let msg = body.error || body.message || ('خطأ في الاتصال بالسيرفر. الكود: ' + res.status);
      if (body.tried && body.tried.length) msg += ' [' + body.tried.map(t => t.status || t.error || '?').join(', ') + ']';
      throw new Error(msg);
    }
    $('url').value = '';
    await fetchJobs();
  } catch (err) {
    showErr('حدث خطأ: ' + err.message);
  } finally {
    btn.disabled = false;
    btn.innerText = 'ابدأ التحميل';
  }
}

function jobAction(path, id) { return fetch(path + id, { method: 'POST' }).then(fetchJobs).catch(fetchJobs); }
function deleteJob(id) { return jobAction('/api/delete/', id); }
function stopJob(id) { return jobAction('/api/stop/', id); }

function renderJob(j) {
  let h = '<div class="job-card"><div class="job-url" dir="ltr">' + esc(j.url) + '</div>';
  if (j.status === 'queued') {
    h += '<div class="job-status-running">في قائمة الانتظار...</div>';
    h += '<div class="job-actions"><span class="job-del" onclick="deleteJob(\'' + j.id + '\')">إلغاء</span></div>';
  } else if (j.status === 'running') {
    let line = esc(j.phase || 'جاري التحميل') + ' ' + esc(j.progress || '');
    if (j.lane) line += ' [' + esc(j.lane) + ']';
    if (j.speed) line += ' • ' + esc(j.speed);
    if (j.eta) line += ' • ETA ' + esc(j.eta);
    h += '<div class="job-status-running">' + line + '</div>';
    if (/%$/.test(j.progress || '')) h += '<div class="bar"><div class="bar-in" style="width:' + Math.min(parseFloat(j.progress), 100) + '%"></div></div>';
    h += '<div class="job-actions"><span class="job-del" onclick="stopJob(\'' + j.id + '\')">إيقاف وحفظ ما تم</span><span class="job-del" onclick="deleteJob(\'' + j.id + '\')">إلغاء</span></div>';
  } else if (j.status === 'finished') {
    const name = j.display_name ? j.display_name : j.filename;
    h += '<div class="job-status-success"><a class="job-link" href="/downloads/' + encodeURIComponent(j.filename) + '" download>تحميل الملف: ' + esc(name) + '</a>';
    h += '<span class="job-del" onclick="deleteJob(\'' + j.id + '\')">حذف</span></div>';
  } else if (j.status === 'failed') {
    h += '<div class="job-status-failed">فشل التحميل: ' + esc(j.error) + '</div>';
    if (j.log && j.log.length) h += '<details><summary class="job-del">تفاصيل تقنية</summary><div class="job-log">' + esc(j.log.join('\n')) + '</div></details>';
    h += '<div class="job-actions"><span class="job-del" onclick="deleteJob(\'' + j.id + '\')">إزالة</span></div>';
  }
  return h + '</div>';
}

function renderJobs(jobs) {
  const box = $('jobs-box');
  const visible = jobs.filter(j => j.status !== 'expired');
  if (!visible.length) { box.innerHTML = '<div class="empty-jobs">لا توجد تحميلات حالياً. أضف رابطاً وابدأ.</div>'; return; }
  box.innerHTML = visible.map(renderJob).join('');
}

async function fetchJobs() {
  clearTimeout(pollTimer);
  let delay = 12000;
  try {
    const res = await fetch('/api/jobs', { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const jobs = await res.json();
    $('conn-note').style.display = 'none';
    renderJobs(jobs);
    if (jobs.some(j => j.status === 'running' || j.status === 'queued')) delay = 2000;
  } catch (e) {
    $('conn-note').innerText = 'السيرفر غير متاح لحظياً (ربما يتم تبديله). جاري إعادة المحاولة...';
    $('conn-note').style.display = 'block';
    delay = 6000;
  }
  if (!document.hidden) pollTimer = setTimeout(fetchJobs, delay);
}

async function fetchStatus() {
  try {
    const h = await (await fetch('/api/health', { cache: 'no-store' })).json();
    const parts = (h.lanes || []).map(l => l.name + (l.yt_ok === true ? ' ✓' : (l.yt_ok === false ? ' ✗' : ' …')) + (l.mbps ? ' ' + Math.round(l.mbps) + 'Mbps' : ''));
    $('srv-status').textContent = parts.length ? ('YouTube: ' + parts.join(' | ')) : '';
  } catch (e) { $('srv-status').textContent = ''; }
}

document.addEventListener('visibilitychange', () => { if (!document.hidden) { fetchJobs(); fetchStatus(); } });
setInterval(() => { if (!document.hidden) fetchStatus(); }, 60000);
fetchJobs();
fetchStatus();
</script>
</body>
</html>"""

SVG_POSTER = """<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
  <rect width="1200" height="630" fill="#0d131e"/>
  <text x="600" y="80" text-anchor="middle" fill="#2dd4bf" font-family="sans-serif" font-size="36" font-weight="bold" letter-spacing="2">SERVANT_OF_ALLAH</text>
  <text x="600" y="210" text-anchor="middle" fill="#ffffff" font-family="sans-serif" font-size="42" font-weight="normal">( وَاتَّقُوا يَوْمًا تُرْجَعُونَ فِيهِ إِلَى اللَّهِ ۖ ثُمَّ تُوَفَّىٰ كُلُّ نَفْسٍ مَّا كَسَبَتْ وَهُمْ لَا يُظْلَمُونَ )</text>
  <text x="600" y="320" text-anchor="middle" fill="#38bdf8" font-family="sans-serif" font-size="28">سورة البقرة، الآية 281، الجزء 3، الصفحة 47</text>
  <text x="600" y="440" text-anchor="middle" fill="#cbd5e1" font-family="sans-serif" font-size="26">And be afraid of the Day when you shall be brought back to Allah. Then every person</text>
  <text x="600" y="490" text-anchor="middle" fill="#cbd5e1" font-family="sans-serif" font-size="26">shall be paid what he earned, and they shall not be dealt with unjustly.</text>
</svg>"""


# ----------------------------------------------------------------- المسارات (Routes)
@app.route("/")
def index():
    host = request.headers.get("X-Public-Host")
    base = f"https://{host}/" if host else request.host_url
    resp = Response(HTML_TEMPLATE.replace("__HOST_URL__", base), mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/og-image.png")
def og_image():
    return Response(SVG_POSTER, mimetype="image/svg+xml")


@app.route("/api/health")
def api_health():
    with jobs_lock:
        running = sum(1 for j in jobs.values() if j.get("status") in ("queued", "running"))
    lns = lane_summary()
    return jsonify({
        "ok": True, "running": running, "preflight": state["preflight"],
        "youtube_ok": any(l["yt_ok"] for l in lns) if lns else None,
        "lanes": lns, "ytdlp": state["ytdlp"], "pot_server": state["pot"],
        "aria2c": bool(shutil.which("aria2c")), "warp_rotations": state["rotations"],
    })


@app.route("/api/jobs")
def api_jobs():
    with jobs_lock:
        items = []
        for jid, j in jobs.items():
            if j.get("owner") != g.uid:
                continue
            d = {k: v for k, v in j.items() if k != "owner"}
            d["id"] = jid
            items.append(d)
    items.sort(key=lambda x: x.get("created", 0), reverse=True)
    resp = jsonify(items)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/download", methods=["POST"])
def api_download():
    data = request.get_json(force=True, silent=True) or {}
    url = normalize_url(data.get("url", ""))
    if not url:
        return jsonify({"error": "لا يوجد رابط صالح"}), 400
    mode = "segment" if data.get("mode") == "segment" else "full"
    quality = str(data.get("quality", "720"))
    if quality not in ("360", "480", "720", "1080", "best"):
        quality = "720"
    start = end = None
    if mode == "segment":
        start, end = parse_hms(data.get("start_time")), parse_hms(data.get("end_time"))
        if start is None or end is None or end <= start:
            return jsonify({"error": "وقت النهاية يجب أن يكون بعد وقت البداية"}), 400
    opts = {"url": url, "mode": mode, "quality": quality, "start": start, "end": end,
            "audio_only": bool(data.get("audio_only")),
            "live_from_start": bool(data.get("live_from_start")),
            "precise": bool(data.get("precise", True))}
    job_id = uuid.uuid4().hex[:12]
    with jobs_lock:
        mine = [j for j in jobs.values() if j.get("owner") == g.uid]
        if sum(1 for j in mine if j.get("status") in ("queued", "running")) >= MAX_USER_ACTIVE:
            return jsonify({"error": f"عندك {MAX_USER_ACTIVE} تحميلات شغالة بالفعل، استنى لحد ما تخلص."}), 429
        if len(mine) >= MAX_USER_JOBS:
            return jsonify({"error": "عندك تحميلات كتير، احذف القديم الأول."}), 429
        jobs[job_id] = {"owner": g.uid, "url": url, "status": "queued", "phase": "في قائمة الانتظار",
                        "progress": "0%", "speed": "", "eta": "", "filename": "", "display_name": "",
                        "error": "", "created": time.time(), "mode": mode, "lane": ""}
    save_jobs()
    threading.Thread(target=run_job, args=(job_id, opts), daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/api/stop/<job_id>", methods=["POST"])
def api_stop(job_id):
    if not owned_job(job_id):
        return jsonify({"status": "not_found"}), 404
    return jsonify({"status": "ok" if stop_job(job_id, "save") else "not_running"})


@app.route("/api/delete/<job_id>", methods=["POST"])
def api_delete(job_id):
    if not owned_job(job_id):
        return jsonify({"status": "not_found"}), 404
    stop_job(job_id, "cancel")
    with jobs_lock:
        jobs.pop(job_id, None)
    remove_job_files(job_id)
    save_jobs()
    return jsonify({"status": "ok"})


@app.route("/downloads/<path:filename>")
def serve_file(filename):
    name = None
    with jobs_lock:
        for j in jobs.values():
            if j.get("filename") == filename and j.get("owner") == g.uid and j.get("status") == "finished":
                name = j.get("display_name") or ""
                break
        else:
            return jsonify({"error": "غير موجود"}), 404
    kw = {"as_attachment": True}
    if name:
        kw["download_name"] = name + os.path.splitext(filename)[1]
    return send_from_directory(DOWNLOAD_DIR, filename, **kw)


# ----------------------------------------------------------------- التشغيل
def main():
    load_jobs()
    if DIRECT_LANE:
        add_lane("direct", "")
    if PROXY:
        add_lane("proxy", PROXY, rotate=WARP_ROTATE)
    if not lanes:
        add_lane("direct", "")
    threading.Thread(target=cleanup_loop, daemon=True).start()
    threading.Thread(target=lane_monitor, daemon=True).start()
    log(f"[server] 127.0.0.1:{PORT} lanes={list(lanes)} warp_rotate={WARP_ROTATE} aria2c={bool(shutil.which('aria2c'))}")
    try:
        from waitress import serve
        serve(app, host="127.0.0.1", port=PORT, threads=48, channel_timeout=600)
    except ImportError:
        app.run(host="127.0.0.1", port=PORT, threaded=True)


if __name__ == "__main__":
    main()
