# -*- coding: utf-8 -*-
"""
SERVANT_OF_ALLAH - محمّل فيديو مبني على yt-dlp
يعمل على GitHub Actions خلف Cloudflare Worker (نفس الملف في Repo-A و Repo-B).

أهم الإصلاحات في هذه النسخة:
1) إصلاح قفل متداخل (Deadlock) كان يجمّد السيرفر كله عند أول تحميل -> كان يظهر 503/524.
2) كل الشبكة عبر بروكسي واحد (YTDLP_PROXY) بدون أي كوكيز.
3) عند حظر يوتيوب للـ IP: تدوير هوية WARP تلقائياً + إعادة المحاولة بعملاء (clients) مختلفين.
4) قص أجزاء بدقة (إعادة ترميز عند نقاط القص) => الصوت والصورة متزامنان 100%.
5) دعم اللايف (من البداية / إيقاف وحفظ ما تم تسجيله).
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

from flask import Flask, Response, jsonify, request, send_from_directory
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

PROXY = os.environ.get("YTDLP_PROXY", "").strip()          # يضبطه ملف run.yml
WARP_ROTATE = os.environ.get("WARP_ROTATE", "0") == "1"    # تدوير IP عبر warp-cli
WARP_PORT = os.environ.get("WARP_PORT", "40000")
FILE_TTL = int(os.environ.get("FILE_TTL", "1200"))         # يحذف الملف بعد 20 دقيقة
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "3"))    # عدد التحميلات المتوازية
FRAGMENTS = os.environ.get("CONCURRENT_FRAGMENTS", "8")    # سرعة تحميل القطع (HLS/DASH)
PREFLIGHT_URL = os.environ.get("PREFLIGHT_URL", "https://www.youtube.com/watch?v=jNQXAC9IVRw")

# مجموعات عملاء يوتيوب: نجرب الافتراضي أولاً ثم بدائل لو فشل/انحظر
YT_CLIENT_SETS = [
    [],
    ["--extractor-args", "youtube:player_client=tv_downgraded,web_safari,mweb"],
    ["--extractor-args", "youtube:player_client=android_vr,visionos,ios"],
]

# ----------------------------------------------------------------- الحالة
jobs = {}
jobs_lock = threading.RLock()      # RLock: يمنع الـ deadlock حتى لو تداخل الاستدعاء
_file_lock = threading.Lock()
procs = {}                         # job_id -> Popen
stop_req = {}                      # job_id -> "save" | "cancel"
slots = threading.BoundedSemaphore(MAX_PARALLEL)
rotate_lock = threading.Lock()
_last_rotate = 0.0
state = {"preflight": "pending", "youtube_ok": None, "rotations": 0,
         "egress": {}, "ytdlp": "", "pot": None, "last_probe_error": ""}


def log(msg):
    print(msg, flush=True)


# ----------------------------------------------------------------- تخزين المهام
def save_jobs():
    """يحفظ jobs.json. لا يمسك القفل أثناء الكتابة على الملف."""
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
        if j.get("status") in ("queued", "running"):
            # العملية كانت شغالة على السيرفر القديم وانقطعت أثناء التبديل
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
    # روابط ناقصة بسبب النسخ/اللصق (ttps:// أو tps:// أو ps://)
    if u.startswith("ttps://"):
        u = "h" + u
    elif u.startswith("tps://"):
        u = "ht" + u
    elif u.startswith("ps://"):
        u = "htt" + u
    elif not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    return u


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


def fmt_hms(sec):
    return f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}:{sec % 60:02d}"


def safe_title(t):
    t = re.sub(r'[\\/:*?"<>|\r\n\t]+', " ", t or "").strip(" .")
    return re.sub(r"\s+", " ", t)[:120]


TEMP_SUFFIX = (".part", ".ytdl", ".temp", ".tmp", ".json", ".txt")


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
    pats = [f"video_{job_id}*", f"title_{job_id}*"]
    for pat in pats:
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


# ----------------------------------------------------------------- البروكسي / WARP
def net_args():
    a = ["--socket-timeout", "30", "--retries", "5"]
    if shutil.which("node"):
        a += ["--js-runtimes", "node"]     # yt-dlp يحتاج JS runtime لحل تحديات يوتيوب
    if PROXY:
        a += ["--proxy", PROXY]
    return a


def curl_trace(timeout=12):
    cmd = ["curl", "-s", "--max-time", str(timeout)]
    if PROXY:
        cmd += ["-x", PROXY]
    cmd.append("https://www.cloudflare.com/cdn-cgi/trace")
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 3).stdout
    except Exception:
        return {}
    res = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            res[k.strip()] = v.strip()
    return {"warp": res.get("warp", "?"), "loc": res.get("loc", "?")}


def _warp(*args, timeout=40):
    try:
        r = subprocess.run(["warp-cli", "--accept-tos", *args], capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, (r.stdout + r.stderr).strip()
    except Exception as e:
        return False, str(e)


def rotate_warp_ip(reason=""):
    """يغيّر هوية WARP للحصول على IP جديد (مفيد عندما يحظر يوتيوب الـ IP الحالي)."""
    global _last_rotate
    if not (WARP_ROTATE and shutil.which("warp-cli")):
        return False
    with rotate_lock:
        if time.time() - _last_rotate < 40:      # حد آخر غيّرها للتو
            return True
        log(f"[warp] rotating identity ({reason})")
        _warp("disconnect")
        _warp("registration", "delete")
        ok, msg = _warp("registration", "new")
        if not ok:
            ok, msg = _warp("register")
        if not ok:
            log(f"[warp] registration failed: {msg[:200]}")
        if not _warp("mode", "proxy")[0]:
            _warp("set-mode", "proxy")
        if not _warp("proxy", "port", WARP_PORT)[0]:
            _warp("set-proxy-port", WARP_PORT)
        _warp("connect")
        for _ in range(25):
            _, msg = _warp("status")
            if "Connected" in msg:
                break
            time.sleep(1)
        _last_rotate = time.time()
        state["rotations"] += 1
        time.sleep(2)
        return True


def is_blocked(text):
    return bool(re.search(r"confirm you.{0,3}re not a bot|HTTP Error 429|Too Many Requests", text or "", re.I))


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


def yt_probe(timeout=90):
    cmd = ["yt-dlp", "--ignore-config", "--no-playlist", "--no-warnings",
           "--print", "%(title)s"] + net_args() + ["--", PREFLIGHT_URL]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return (r.returncode == 0 and bool(r.stdout.strip())), (r.stdout + r.stderr)
    except Exception as e:
        return False, str(e)


def preflight():
    """فحص عند التشغيل: هل يوتيوب يقبل الـ IP؟ لو لا، نغيّر هوية WARP ونعيد."""
    state["preflight"] = "running"
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
    state["egress"] = curl_trace()
    attempts = 4 if WARP_ROTATE else 1
    for i in range(attempts):
        ok, out = yt_probe()
        if ok:
            state["youtube_ok"] = True
            state["last_probe_error"] = ""
            break
        state["youtube_ok"] = False
        state["last_probe_error"] = out.strip()[-300:]
        log(f"[preflight] attempt {i + 1}/{attempts} failed: {state['last_probe_error'][-160:]}")
        if i < attempts - 1:
            if is_blocked(out) and WARP_ROTATE:
                rotate_warp_ip("preflight")
            else:
                time.sleep(3)
    state["egress"] = curl_trace()
    state["preflight"] = "done"
    log(f"[preflight] done youtube_ok={state['youtube_ok']} egress={state['egress']}")


# ----------------------------------------------------------------- بناء أمر yt-dlp
def format_args(quality, audio_only):
    if audio_only:
        return ["-f", "ba/b", "-x", "--audio-format", "mp3", "--audio-quality", "0"]
    sort = ["-S", "res,vcodec:h264,acodec:aac", "--merge-output-format", "mp4/mkv"]
    if quality == "best":
        return ["-f", "bv*+ba/b"] + sort
    h = int(quality)
    return ["-f", f"bv*[height<={h}]+ba/b[height<={h}]/bv*+ba/b"] + sort


PCT_RE = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
SPEED_RE = re.compile(r"at\s+(\S+/s)")
ETA_RE = re.compile(r"ETA\s+(\S+)")
SIZE_RE = re.compile(r"\[download\]\s+(~?\s*[\d.]+\s*\w*B)\s+at")


def handle_line(job_id, s):
    m = PCT_RE.search(s)
    if m:
        sp, eta = SPEED_RE.search(s), ETA_RE.search(s)
        update_job(job_id, progress=f"{float(m.group(1)):.1f}%", phase="جاري التحميل",
                   speed=sp.group(1) if sp else "", eta=eta.group(1) if eta else "")
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
    elif s.startswith("[ffmpeg]") or "frame=" in s or "time=" in s:
        update_job(job_id, phase="جاري تحميل وقص الجزء المطلوب")


def run_once(job_id, data, extra_args):
    title_file = os.path.join(DOWNLOAD_DIR, f"title_{job_id}.txt")
    try:
        os.remove(title_file)
    except Exception:
        pass
    out_tpl = os.path.join(DOWNLOAD_DIR, f"video_{job_id}.%(ext)s")
    cmd = ["yt-dlp", "--ignore-config", "--no-playlist", "--newline", "--no-colors", "--no-mtime",
           "-N", FRAGMENTS, "--fragment-retries", "10",
           "--print-to-file", "%(title)s", title_file]
    cmd += net_args() + list(extra_args)
    cmd += format_args(data["quality"], data["audio_only"])
    if data.get("live_from_start"):
        cmd.append("--live-from-start")
    if data["mode"] == "segment":
        cmd += ["--download-sections", f"*{fmt_hms(data['start'])}-{fmt_hms(data['end'])}"]
        if data.get("precise", True):
            cmd.append("--force-keyframes-at-cuts")   # قص دقيق + صوت متزامن
    cmd += ["-o", out_tpl, "--", data["url"]]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            encoding="utf-8", errors="replace", bufsize=1, start_new_session=True)
    procs[job_id] = proc
    errors, tail = [], []
    try:
        for line in proc.stdout:
            s = line.strip()
            if not s:
                continue
            tail.append(s)
            if len(tail) > 60:
                tail.pop(0)
            if s.startswith("ERROR"):
                errors.append(s)
            handle_line(job_id, s)
    finally:
        proc.wait()
        procs.pop(job_id, None)
    err_text = " | ".join(errors[-3:]) or " | ".join(tail[-3:])
    log(f"[job {job_id}] rc={proc.returncode} err={err_text[:200]}")
    return {"rc": proc.returncode, "error": err_text, "path": find_output(job_id), "tail": tail[-14:]}


def salvage_partial(job_id, audio_only):
    """بعد 'إيقاف وحفظ' في اللايف: نحوّل ملفات .part إلى ملف قابل للتشغيل."""
    parts = [p for p in glob.glob(os.path.join(DOWNLOAD_DIR, f"video_{job_id}*.part")) if os.path.getsize(p) > 0]
    if not parts:
        return None
    parts.sort(key=os.path.getsize, reverse=True)
    ext = "mp3" if audio_only else "mp4"
    out = os.path.join(DOWNLOAD_DIR, f"video_{job_id}.partial.{ext}")
    cmd = ["ffmpeg", "-y", "-loglevel", "error"]
    for p in parts[:2]:
        cmd += ["-i", p]
    if audio_only:
        cmd += ["-vn", "-c:a", "libmp3lame", "-q:a", "0"]
    else:
        cmd += ["-c", "copy", "-movflags", "+faststart"]
    cmd.append(out)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    except Exception:
        return None
    if r.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0:
        return out
    return None


def finish_ok(job_id, path):
    title = read_title(job_id)
    fname = os.path.basename(path)
    update_job(job_id, status="finished", filename=fname, display_name=title, progress="100%",
               phase="تم", speed="", eta="", finished_at=time.time(), error="", log=[])
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
            is_yt = bool(re.search(r"(youtube\.com|youtu\.be)", data["url"], re.I))
            client_sets = YT_CLIENT_SETS if is_yt else [[], []]
            last_err, last_tail = "", []
            for attempt, extra in enumerate(client_sets, 1):
                if stop_req.get(job_id):
                    break
                update_job(job_id, attempt=attempt, progress="0%", speed="", eta="")
                if attempt > 1:
                    update_job(job_id, phase=f"إعادة المحاولة ({attempt}/{len(client_sets)})")
                res = run_once(job_id, data, extra)
                if stop_req.get(job_id):
                    break
                if res["rc"] == 0 and res["path"]:
                    finish_ok(job_id, res["path"])
                    return
                last_err, last_tail = res["error"], res["tail"]
                fm = fatal_message(last_err)
                if fm:
                    last_err = fm
                    break
                if is_blocked(last_err):
                    update_job(job_id, phase="يوتيوب حظر الـ IP، جاري تغييره...")
                    rotate_warp_ip(f"job {job_id}")
                time.sleep(2)

            req = stop_req.get(job_id)
            if req == "cancel":
                return
            if req == "save":
                out = salvage_partial(job_id, data["audio_only"]) or find_output(job_id)
                if out:
                    finish_ok(job_id, out)
                else:
                    fail_job(job_id, "تم الإيقاف لكن لا يوجد جزء مسجّل يمكن حفظه.", last_tail)
                return
            if is_blocked(last_err):
                last_err = ("يوتيوب يطلب تسجيل دخول لهذا الـ IP (حظر). جرّب بعد قليل أو استخدم "
                            "بروكسي سكني عبر secret اسمه PROXY_URL.")
            fail_job(job_id, (last_err or "فشل غير معروف")[:400], last_tail)
    except Exception as e:
        log(f"[job {job_id}] exception: {e}")
        fail_job(job_id, f"خطأ داخلي: {e}")
    finally:
        stop_req.pop(job_id, None)
        procs.pop(job_id, None)
        with jobs_lock:
            exists = job_id in jobs
        if not exists:                      # المهمة اتلغت/اتحذفت
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


# ----------------------------------------------------------------- تنظيف تلقائي
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
                m = re.search(r"(?:video|title)_([0-9a-f]{8})", os.path.basename(p))
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
    <label for="precise">قص دقيق وصوت متزامن 100% (أبطأ قليلاً)</label>
  </div>

  <div class="form-group">
    <label>الجودة</label>
    <select id="quality">
      <option value="720" selected>720p</option>
      <option value="1080">1080p</option>
      <option value="480">480p</option>
      <option value="360">360p</option>
      <option value="best">أعلى جودة متاحة</option>
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
  <div class="footer-note">يُحذف الملف تلقائياً من السيرفر بعد 20 دقيقة من اكتمال التحميل.</div>

  <div class="jobs-container" id="jobs-box">
    <div class="empty-jobs">لا توجد تحميلات حالياً. أضف رابطاً وابدأ.</div>
  </div>
</div>

<script>
let currentMode = 'full';
let pollTimer = null;
const $ = id => document.getElementById(id);
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

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
  if (!url) { alert('ضع رابط الفيديو أولاً'); return; }
  if (currentMode === 'segment' && toSec('e') <= toSec('s')) { showErr('وقت النهاية يجب أن يكون بعد وقت البداية'); return; }
  const data = {
    url: url, mode: currentMode,
    start_time: getTimeStr('s'), end_time: getTimeStr('e'),
    quality: $('quality').value, audio_only: $('audio_only').checked,
    live_from_start: $('live_start').checked, precise: $('precise').checked
  };
  const btn = $('btn-submit');
  btn.disabled = true;
  try {
    const res = await fetch('/api/download', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
    let body = {};
    try { body = await res.json(); } catch (e) {}
    if (!res.ok) {
      let msg = body.error_ar || body.error || body.message || ('خطأ في الاتصال بالسيرفر. الكود: ' + res.status);
      if (body.tried && body.tried.length) msg += ' [' + body.tried.map(t => t.status || t.error || '?').join(', ') + ']';
      throw new Error(msg);
    }
    $('url').value = '';
    fetchJobs();
  } catch (err) {
    showErr('حدث خطأ: ' + err.message);
  } finally {
    btn.disabled = false;
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

document.addEventListener('visibilitychange', () => { if (!document.hidden) fetchJobs(); });
fetchJobs();
</script>
</body>
</html>"""

SVG_POSTER = """<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
  <rect width="1200" height="630" fill="#0d131e"/>
  <text x="600" y="80" text-anchor="middle" fill="#2dd4bf" font-family="sans-serif" font-size="36" font-weight="bold" letter-spacing="2">SERVANT_OF_ALLAH</text>
  <text x="600" y="210" text-anchor="middle" fill="#ffffff" font-family="sans-serif" font-size="42" font-weight="normal">( وَاتَّقُوا يَوْمًا تُرْجَعُونَ فِيهِ إِلَى اللَّهِ ثُمَّ تُوَفَّىٰ كُلُّ نَفْسٍ مَّا كَسَبَتْ وَهُمْ لا يُظْلَمُونَ )</text>
  <text x="600" y="320" text-anchor="middle" fill="#38bdf8" font-family="sans-serif" font-size="28">سورة البقرة, الآية 281, الجزء 3, الصفحة 47</text>
  <text x="600" y="440" text-anchor="middle" fill="#cbd5e1" font-family="sans-serif" font-size="26">And be afraid of the Day when you shall be brought back to Allah. Then every person</text>
  <text x="600" y="490" text-anchor="middle" fill="#cbd5e1" font-family="sans-serif" font-size="26">shall be paid what he earned, and they shall not be dealt with unjustly.</text>
</svg>"""


# ----------------------------------------------------------------- المسارات (Routes)
@app.route("/")
def index():
    host = request.headers.get("X-Public-Host")          # يرسله الـ Worker
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
    return jsonify({
        "ok": True, "running": running, "preflight": state["preflight"],
        "youtube_ok": state["youtube_ok"], "ytdlp": state["ytdlp"], "pot_server": state["pot"],
        "proxy": bool(PROXY), "egress": state["egress"], "warp_rotations": state["rotations"],
        "last_probe_error": state["last_probe_error"],
    })


@app.route("/api/jobs")
def api_jobs():
    with jobs_lock:
        items = []
        for jid, j in jobs.items():
            d = dict(j)
            d["id"] = jid
            items.append(d)
    items.sort(key=lambda x: x.get("created", 0), reverse=True)
    return jsonify(items)


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
    opts = {
        "url": url, "mode": mode, "quality": quality, "start": start, "end": end,
        "audio_only": bool(data.get("audio_only")),
        "live_from_start": bool(data.get("live_from_start")),
        "precise": bool(data.get("precise", True)),
    }
    job_id = uuid.uuid4().hex[:8]
    with jobs_lock:
        jobs[job_id] = {"url": url, "status": "queued", "phase": "في قائمة الانتظار", "progress": "0%",
                        "speed": "", "eta": "", "filename": "", "display_name": "", "error": "",
                        "created": time.time(), "mode": mode}
    save_jobs()       # خارج القفل (كان هذا هو سبب الـ deadlock في النسخة القديمة)
    threading.Thread(target=run_job, args=(job_id, opts), daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/api/stop/<job_id>", methods=["POST"])
def api_stop(job_id):
    return jsonify({"status": "ok" if stop_job(job_id, "save") else "not_running"})


@app.route("/api/delete/<job_id>", methods=["POST"])
def api_delete(job_id):
    stop_job(job_id, "cancel")
    with jobs_lock:
        existed = jobs.pop(job_id, None) is not None
    if existed:
        remove_job_files(job_id)
    save_jobs()
    return jsonify({"status": "ok"})


@app.route("/downloads/<path:filename>")
def serve_file(filename):
    name = ""
    with jobs_lock:
        for j in jobs.values():
            if j.get("filename") == filename:
                name = j.get("display_name") or ""
                break
    kw = {"as_attachment": True}
    if name:
        kw["download_name"] = name + os.path.splitext(filename)[1]
    return send_from_directory(DOWNLOAD_DIR, filename, **kw)


# ----------------------------------------------------------------- التشغيل
def main():
    load_jobs()
    threading.Thread(target=cleanup_loop, daemon=True).start()
    threading.Thread(target=preflight, daemon=True).start()
    log(f"[server] starting on 127.0.0.1:{PORT} proxy={'yes' if PROXY else 'no'} warp_rotate={WARP_ROTATE}")
    try:
        from waitress import serve
        serve(app, host="127.0.0.1", port=PORT, threads=32, channel_timeout=600)
    except ImportError:
        app.run(host="127.0.0.1", port=PORT, threaded=True)


if __name__ == "__main__":
    main()
