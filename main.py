import os, sys, subprocess, threading, uuid, shlex, json, time
from flask import Flask, render_template_string, request, jsonify, send_from_directory, Response
from flask_cors import CORS

app = Flask(__name__)
# تفعيل CORS لمنع حظر الطلبات
CORS(app, resources={r"/api/*": {"origins": "*"}})

DATA_DIR = os.path.abspath("data")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "downloads")
JOBS_FILE = os.path.join(DATA_DIR, "jobs.json")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

jobs = {}
jobs_lock = threading.Lock()

if os.path.exists(JOBS_FILE):
    try:
        with open(JOBS_FILE, "r", encoding="utf-8") as f:
            jobs = json.load(f)
    except:
        jobs = {}

def save_jobs():
    with jobs_lock:
        try:
            with open(JOBS_FILE, "w", encoding="utf-8") as f:
                json.dump(jobs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error saving jobs: {e}")

# تنظيف الملفات القديمة
def auto_cleanup_loop():
    while True:
        try:
            now = time.time()
            with jobs_lock:
                for fid in list(jobs.keys()):
                    j = jobs[fid]
                    filename = j.get('filename')
                    if filename:
                        fpath = os.path.join(DOWNLOAD_DIR, filename)
                        if os.path.exists(fpath) and (now - os.path.getmtime(fpath) > 600):
                            try: os.remove(fpath)
                            except: pass
                            j['status'] = 'expired'
            save_jobs()
        except:
            pass
        time.sleep(60)

threading.Thread(target=auto_cleanup_loop, daemon=True).start()

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SERVANT_OF_ALLAH</title>
<meta property="og:title" content="SERVANT_OF_ALLAH">
<meta property="og:description" content="أداة فائقة السرعة لتحميل واقتطاع الفيديوهات بأعلى جودة">
<meta property="og:image" content="{{ host_url }}og-image.png">
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
  .checkbox-group { display: flex; align-items: center; gap: 8px; margin-bottom: 18px; color: #cbd5e1; font-size: 14px; cursor: pointer; }
  .checkbox-group input { width: 16px; height: 16px; accent-color: #14b8a6; cursor: pointer; }
  .btn-submit { width: 100%; padding: 14px; background: #2dd4bf; color: #0d131e; border: none; border-radius: 8px; font-size: 16px; font-weight: bold; cursor: pointer; }
  .btn-submit:hover { background: #14b8a6; }
  .btn-submit:disabled { opacity: 0.6; cursor: not-allowed; }
  .footer-note { text-align: center; color: #64748b; font-size: 12px; margin-top: 12px; }
  .jobs-container { margin-top: 20px; background: #0d131e; border-radius: 8px; padding: 16px; border: 1px solid #283548; min-height: 60px; }
  .job-card { margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid #161e2e; word-break: break-all; font-size: 13px; }
  .job-card:last-child { margin-bottom: 0; padding-bottom: 0; border-bottom: none; }
  .job-url { color: #94a3b8; margin-bottom: 6px; }
  .job-status-running { color: #38bdf8; }
  .job-status-failed { color: #f87171; font-weight: bold; }
  .job-status-success { color: #4ade80; font-weight: bold; }
  .job-link { color: #2dd4bf; text-decoration: underline; font-weight: bold; margin-left: 8px; }
  .job-del { color: #94a3b8; cursor: pointer; text-decoration: underline; font-size: 12px; margin-right: 8px; }
  .empty-jobs { text-align: center; color: #64748b; font-size: 14px; margin: 10px 0; }
  .error-msg { color: #f87171; font-size: 13px; text-align: center; margin-top: 10px; display: none; }
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
    <label for="audio_only" style="margin:0; cursor:pointer; color:#cbd5e1;">صوت فقط (mp3)</label>
  </div>

  <button class="btn-submit" onclick="submitDownload()">ابدأ التحميل</button>
  <div class="error-msg" id="err-box"></div>
  <div class="footer-note">بعد تنزيل الملف يتم حذفه تلقائياً من السيرفر.</div>

  <div class="jobs-container" id="jobs-box">
    <div class="empty-jobs">لا توجد تحميلات حالياً. أضف رابطاً وابدأ.</div>
  </div>
</div>

<script>
let currentMode = 'full';

function setMode(mode) {
  currentMode = mode;
  document.getElementById('tab-segment').classList.toggle('active', mode === 'segment');
  document.getElementById('tab-full').classList.toggle('active', mode === 'full');
  document.getElementById('time-picker').style.display = (mode === 'segment') ? 'block' : 'none';
}

function stepTime(id, dir) {
  let el = document.getElementById(id);
  let val = parseInt(el.innerText) + dir;
  if (val < 0) val = 59;
  if (id.endsWith('_h') && val > 99) val = 0;
  else if (!id.endsWith('_h') && val > 59) val = 0;
  el.innerText = String(val).padStart(2, '0');
}

function getTimeStr(prefix) {
  let h = document.getElementById(prefix + '_h').innerText;
  let m = document.getElementById(prefix + '_m').innerText;
  let s = document.getElementById(prefix + '_s').innerText;
  return `${h}:${m}:${s}`;
}

function submitDownload() {
  let url = document.getElementById('url').value.trim();
  const errBox = document.getElementById('err-box');
  errBox.style.display = 'none';

  if (!url) { alert('ضع رابط الفيديو أولاً'); return; }

  if (url.startsWith('tps://')) url = 'h' + url;
  else if (url.startsWith('ps://')) url = 'htt' + url;
  else if (!url.startsWith('http://') && !url.startsWith('https://')) url = 'https://' + url;

  const data = {
    url: url,
    mode: currentMode,
    start_time: getTimeStr('s'),
    end_time: getTimeStr('e'),
    quality: document.getElementById('quality').value,
    audio_only: document.getElementById('audio_only').checked
  };

  const btn = document.querySelector('.btn-submit');
  btn.disabled = true;

  fetch('/api/download', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  })
  .then(res => {
    if (!res.ok) throw new Error('خطأ في الاتصال بالسيرفر. الكود: ' + res.status);
    return res.json();
  })
  .then(resData => {
    btn.disabled = false;
    document.getElementById('url').value = '';
    fetchJobs();
  })
  .catch(err => {
    btn.disabled = false;
    errBox.innerText = 'حدث خطأ: ' + err.message;
    errBox.style.display = 'block';
    fetchJobs();
  });
}

function deleteJob(jobId) {
  fetch('/api/delete/' + jobId, { method: 'POST' }).then(() => fetchJobs());
}

function fetchJobs() {
  fetch('/api/jobs')
  .then(res => res.json())
  .then(jobs => {
    const box = document.getElementById('jobs-box');
    const keys = Object.keys(jobs);
    if (keys.length === 0) {
      box.innerHTML = '<div class="empty-jobs">لا توجد تحميلات حالياً. أضف رابطاً وابدأ.</div>';
      return;
    }
    let html = '';
    keys.reverse().forEach(id => {
      const j = jobs[id];
      if (j.status === 'expired') return;
      html += `<div class="job-card">`;
      html += `<div class="job-url" dir="ltr" style="text-align:left;">${j.url}</div>`;
      if (j.status === 'running') {
        html += `<div class="job-status-running">جاري التحميل ... ${j.progress || '0%'}</div>`;
      } else if (j.status === 'finished') {
        html += `<div class="job-status-success"><a class="job-link" href="/downloads/${j.filename}" download onclick="setTimeout(()=>deleteJob('${id}'), 3000)">تحميل الملف: ${j.filename}</a> <span class="job-del" onclick="deleteJob('${id}')">حذف</span></div>`;
      } else if (j.status === 'failed') {
        html += `<div class="job-status-failed">فشل التحميل: ${j.error}</div>`;
      }
      html += `</div>`;
    });
    box.innerHTML = html || '<div class="empty-jobs">لا توجد تحميلات حالياً. أضف رابطاً وابدأ.</div>';
  })
  .catch(() => {});
}

setInterval(fetchJobs, 3000);
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

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, host_url=request.host_url)

@app.route('/og-image.png')
def og_image():
    return Response(SVG_POSTER, mimetype='image/svg+xml')

@app.route('/api/jobs')
def get_jobs():
    with jobs_lock:
        return jsonify(jobs)

@app.route('/api/delete/<job_id>', methods=['POST'])
def delete_job(job_id):
    with jobs_lock:
        if job_id in jobs:
            file_path = os.path.join(DOWNLOAD_DIR, jobs[job_id].get('filename', ''))
            if os.path.exists(file_path):
                try: os.remove(file_path)
                except: pass
            del jobs[job_id]
            save_jobs()
    return jsonify({'status': 'ok'})

@app.route('/api/download', methods=['POST'])
def start_download():
    # استخدام معالجة قوية للبيانات لتجنب خطأ 500
    try:
        data = request.get_json(force=True, silent=True) or {}
    except:
        data = {}
        
    url = data.get('url', '').strip()
    
    if not url:
        return jsonify({'error': 'لا يوجد رابط'}), 400
        
    if url.startswith('tps://'): url = 'h' + url
    elif url.startswith('ps://'): url = 'htt' + url
    elif not url.startswith('http://') and not url.startswith('https://'): url = 'https://' + url
    data['url'] = url

    job_id = str(uuid.uuid4())[:8]
    with jobs_lock:
        jobs[job_id] = {
            'url': url,
            'status': 'running',
            'progress': '0%',
            'filename': '',
            'error': ''
        }
        save_jobs()

    threading.Thread(target=run_ytdlp, args=(job_id, data)).start()
    return jsonify({'job_id': job_id})

@app.route('/downloads/<path:filename>')
def serve_file(filename):
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)

def run_ytdlp(job_id, data):
    url = data.get('url')
    mode = data.get('mode')
    quality = data.get('quality', '720')
    audio_only = data.get('audio_only', False)
    start_time = data.get('start_time', '00:00:00')
    end_time = data.get('end_time', '00:03:00')

    ext = "mp3" if audio_only else "mp4"
    out_name = f"video_{job_id}.{ext}"
    out_path = os.path.join(DOWNLOAD_DIR, out_name)

    raw_extra = os.environ.get("YTDLP_EXTRA", "")
    try:
        extra_args = shlex.split(raw_extra)
    except:
        extra_args = []

    # أوامر مبسطة ومستقرة للتحميل
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "-N", "4"
    ]

    if audio_only:
        cmd.extend(["-x", "--audio-format", "mp3"])
    else:
        if quality == 'best':
            cmd.extend(["-f", "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"])
        else:
            cmd.extend(["-f", f"bestvideo[height<={quality}][ext=mp4]+bestaudio[ext=m4a]/best[height<={quality}]/best"])
        cmd.extend(["--merge-output-format", "mp4"])

    if mode == 'segment':
        cmd.extend(["--download-sections", f"*{start_time}-{end_time}"])

    cmd.extend(extra_args)
    cmd.extend(["-o", out_path, url])

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            line_str = line.strip()
            if '[download]' in line_str and '%' in line_str:
                try:
                    parts = line_str.split()
                    for p in parts:
                        if '%' in p:
                            with jobs_lock:
                                jobs[job_id]['progress'] = p
                            break
                except: pass
            elif 'ERROR:' in line_str or 'error:' in line_str:
                with jobs_lock:
                    jobs[job_id]['error'] = line_str

        proc.wait()

        actual_files = [f for f in os.listdir(DOWNLOAD_DIR) if job_id in f]
        with jobs_lock:
            if actual_files:
                jobs[job_id]['status'] = 'finished'
                jobs[job_id]['filename'] = actual_files[0]
            elif proc.returncode == 0 and os.path.exists(out_path):
                jobs[job_id]['status'] = 'finished'
                jobs[job_id]['filename'] = out_name
            else:
                jobs[job_id]['status'] = 'failed'
                if not jobs[job_id]['error']:
                    jobs[job_id]['error'] = f'خطأ أثناء التحميل (كود: {proc.returncode})'
    except Exception as e:
        with jobs_lock:
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['error'] = str(e)
    
    save_jobs()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8090)
