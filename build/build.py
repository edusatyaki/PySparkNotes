"""Build the PySpark Class Notes site.

    .venv/bin/python build/build.py --run     # run every code block on real Spark, refresh build/outputs.json, render
    .venv/bin/python build/build.py           # render index.html from content/ + build/outputs.json

Content lives in content/chNN.py (one module per chapter). index.html is generated — never edit it by hand.
"""
import contextlib, datetime, decimal, html, importlib.util, io, json, os, re, shutil, sys, traceback
from pathlib import Path

import markdown as md
from pygments.lexers import PythonLexer
from pygments.token import Comment, Keyword, Name, Number, Operator, String

ROOT = Path(__file__).resolve().parent.parent
TPL = ROOT / 'build' / 'template'
OUT_JSON = ROOT / 'build' / 'outputs.json'
WORK = ROOT / 'build' / 'work'
SPARK_LABEL = 'PySpark 4.2'

# ───────────────────────── content loading ─────────────────────────
def load_chapters():
    mods = []
    for p in sorted((ROOT / 'content').glob('ch*.py')):
        spec = importlib.util.spec_from_file_location(p.stem, p)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        mods.append(m)
    mods.sort(key=lambda m: m.ORDER)
    return mods


def item_id(kind, title, k=0):
    return f'{kind}:{title}' + (f'#{k}' if k else '')


# ───────────────────────── running on Spark ─────────────────────────
def fmt_val(v):
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'true' if v else 'false'
    if isinstance(v, (list, tuple)) and not hasattr(v, '__fields__'):
        return '[' + ', '.join(fmt_val(x) for x in v) + ']'
    if hasattr(v, 'asDict'):
        return '{' + ', '.join(fmt_val(x) for x in v) + '}'
    if isinstance(v, dict):
        return '{' + ', '.join(f'{fmt_val(a)} -> {fmt_val(b)}' for a, b in v.items()) + '}'
    if isinstance(v, datetime.datetime):
        return v.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, decimal.Decimal):
        return str(v)
    return str(v)


def snapshot(df, limit=30):
    rows = df.limit(200).collect()
    cnt = df.count()
    cols = df.columns
    numeric = []
    for f in df.schema.fields:
        numeric.append(f.dataType.typeName() in ('integer', 'long', 'short', 'double', 'float', 'decimal', 'byte'))
    data = [[fmt_val(r[i]) for i in range(len(cols))] for r in rows]
    return {'cols': cols, 'num': numeric, 'rows': data, 'count': cnt}


def short_error(e):
    """The first lines a learner needs from an exception: class, error class and message, plus its Spark cause."""
    name = type(e).__name__
    msg = str(e).strip()
    lines = [l.strip() for l in msg.split('\n') if l.strip()]
    if name == 'Py4JJavaError':
        # unwrap: ": org.apache.spark.SparkException: [ERROR_CLASS] message"
        spark_lines = [l for l in lines if re.search(r'(\w+(Exception|Error)): \[[A-Z_.]+\]', l)]
        out = []
        for l in spark_lines[:3]:
            m = re.search(r'(\w+(?:Exception|Error)): (\[[A-Z_.]+\].*)', l)
            txt = f'{m.group(1)}: {m.group(2)}'
            if txt not in out:
                out.append(txt)
        text = '\n'.join(('Caused by: ' if k else '') + t for k, t in enumerate(out))
        return re.sub(r'\s*SQLSTATE: \w+\.?', '', text) or f'{name}: {lines[0]}'
    if name == 'PythonException':
        last = [l for l in lines if re.match(r'^[A-Za-z_.]*(Error|Exception): ', l)]
        return f'{name}: An exception was thrown from the Python worker:\n' + (last[-1] if last else lines[-1])
    keep = []
    for l in lines:
        if l.startswith('JVM stacktrace') or l.startswith('at ') or l.startswith('=='):
            break
        keep.append(l)
        if len(keep) >= (12 if name == 'PySparkAssertionError' else 3):
            break
    text = '\n'.join(keep)
    text = re.sub(r'\s*SQLSTATE: \w+\.?', '', text)
    return f'{name}: {text}'


def run_all(chapters):
    os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
    from pyspark.sql import SparkSession
    if WORK.exists():
        shutil.rmtree(WORK)
    WORK.mkdir(parents=True)
    os.chdir(WORK)
    from delta import configure_spark_with_delta_pip

    def session(delta):
        # Delta's session extension changes SQL analysis (e.g. named parameters stop binding),
        # so only chapters that set DELTA = True run with it; the rest run on plain Spark.
        builder = (SparkSession.builder.master('local[4]').appName('class-notes')
                   .config('spark.ui.showConsoleProgress', 'false')
                   .config('spark.sql.session.timeZone', 'UTC')
                   .config('spark.driver.memory', '2g'))
        if delta:
            builder = (builder.config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                              .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog'))
        # the Delta JARs must be on the classpath when the JVM first starts, so add them for every session
        sp = configure_spark_with_delta_pip(builder).getOrCreate()
        sp.sparkContext.setLogLevel('ERROR')
        return sp

    mode = False
    spark = session(False)
    only = set(a for a in sys.argv[2:] if not a.startswith('-'))
    results = json.loads(OUT_JSON.read_text()) if OUT_JSON.exists() and only else {}
    for ch in chapters:
        if only and ch.KEY not in only:
            continue
        print(f'── chapter {ch.KEY}: {ch.TITLE}', flush=True)
        want = getattr(ch, 'DELTA', False)
        if want != mode:
            spark.stop()
            spark, mode = session(want), want
        ns = {'spark': spark, '__name__': f'ch{ch.KEY}'}
        res = {}
        if getattr(ch, 'PREP', None):
            exec(compile(ch.PREP, f'ch{ch.KEY}-prep', 'exec'), ns)
        for it in iter_runnable(ch):
            iid, code, tables, expect_error = it
            snap = {}
            for t in tables:
                obj = ns.get(t)
                if obj is None:
                    raise SystemExit(f'[{iid}] table {t!r} is not defined')
                snap[t] = snapshot(obj)
            buf = io.StringIO()
            err = None
            with contextlib.redirect_stdout(buf):
                try:
                    exec(compile(code, iid, 'exec'), ns)
                except Exception as e:  # noqa: BLE001
                    if not expect_error:
                        sys.stdout = sys.__stdout__
                        traceback.print_exc()
                        raise SystemExit(f'\n✗ [{iid}] raised but was not marked error=True')
                    err = short_error(e)
            out = buf.getvalue().rstrip('\n')
            if expect_error and err is None:
                raise SystemExit(f'✗ [{iid}] was marked error=True but ran cleanly')
            if err:
                out = (out + '\n' if out else '') + err
            out = scrub(out)
            res[iid] = {'out': out, 'tables': snap}
            print(f'   ✓ {iid[:90]}', flush=True)
        results[ch.KEY] = res
        spark.catalog.clearCache()
        for v in spark.catalog.listTables():
            if v.isTemporary:
                spark.catalog.dropTempView(v.name)
        for q in spark.streams.active:
            q.stop()
    spark.stop()
    os.chdir(ROOT)
    OUT_JSON.write_text(json.dumps(results, indent=1, ensure_ascii=False))
    print('outputs →', OUT_JSON)


def scrub(text):
    """Shorten this machine's working directory in printed paths (plans print absolute file: URIs)."""
    for p in (str(WORK), str(WORK.resolve()), '/private' + str(WORK)):
        text = text.replace(p, '/work')
    return re.sub(r'\x1b\[[0-9;]*m', '', text)       # terminal colour codes


def iter_runnable(ch):
    if getattr(ch, 'SETUP', None):
        yield item_id('setup', ch.SETUP_TITLE), ch.SETUP, [], False
    for s in ch.SECTIONS:
        if s.get('code') and s.get('run', True):
            yield item_id('sec', s['t']), s['code'], s.get('tables', []), s.get('error', False)
    for g in getattr(ch, 'EXTRA', []):
        if g.get('code') and g.get('run', True):
            yield item_id('gap', g['t']), g['code'], g.get('tables', []), g.get('error', False)
    for k, q in enumerate(getattr(ch, 'MCQS', [])):
        if q.get('code') and q.get('run'):
            yield item_id('mcq', q['q'][:60], k), q['code'], [], q.get('error', False)


# ───────────────────────── rendering helpers ─────────────────────────
E = lambda t: html.escape(str(t), quote=True)


def inline(t):
    """Mini markdown for one-paragraph text: `code`, **bold**, *em*, [text](url), line breaks kept as spaces."""
    t = E(t)
    t = re.sub(r'`([^`]+)`', lambda m: f'<code>{m.group(1)}</code>', t)
    t = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', t)
    t = re.sub(r'(?<![\w*])\*(?!\s)(.+?)(?<!\s)\*(?![\w*])', r'<em>\1</em>', t)
    t = re.sub(r'\[([^\]]+)\]\((https?://[^)]+)\)', r'<a href="\2" target="_blank" rel="noopener">\1</a>', t)
    return t


def paras(t):
    """Explanation text: blank line = new paragraph; '- ' lines = bullet list."""
    out = []
    for block in re.split(r'\n\s*\n', t.strip()):
        lines = [l.strip() for l in block.strip().split('\n')]
        if all(l.startswith('- ') for l in lines):
            out.append('<ul class="xl">' + ''.join(f'<li>{inline(l[2:])}</li>' for l in lines) + '</ul>')
        else:
            out.append(f'<p class="expl">{inline(" ".join(lines))}</p>')
    return '\n'.join(out)


def mdblock(t):
    h = md.markdown(t.strip(), extensions=['tables', 'fenced_code'])
    return h


KWD = {'True', 'False', 'None'}


def highlight(code):
    toks = list(PythonLexer().get_tokens(code.rstrip('\n')))
    pieces = []
    for i, (tt, v) in enumerate(toks):
        nxt = toks[i + 1][1] if i + 1 < len(toks) else ''
        cls = None
        if tt in Comment or tt in Comment.Single:
            cls = 'com'
        elif tt in String:
            cls = 'str'
        elif tt in Number:
            cls = 'num'
        elif tt in Keyword or tt in Operator.Word:
            cls = 'kw'
        elif tt in Name.Builtin or tt in Name.Function or tt in Name.Class:
            cls = 'fn'
        elif tt in Name and nxt.startswith('('):
            cls = 'fn'
        pieces.append((cls, v))
    lines, curl = [], []
    for cls, v in pieces:
        parts = v.split('\n')
        for j, p in enumerate(parts):
            if j:
                lines.append(''.join(curl)); curl = []
            if p:
                curl.append(f'<span class="{cls}">{E(p)}</span>' if cls else E(p))
    lines.append(''.join(curl))
    while lines and not lines[-1].strip():
        lines.pop()
    return lines


CHW = 0.602          # width of one monospace character, as a fraction of the font size (Menlo)


def fit_font(lines, text_px, base, minimum):
    """Largest font size (px) at which the longest line fits text_px, capped at base, floored at minimum."""
    longest = max((len(l) for l in lines), default=0)
    if not longest:
        return base
    return max(minimum, min(base, int(text_px / (longest * CHW))))


def code_fig(code, name='query.py', width=1460, start=1):
    lines = code.rstrip('\n').split('\n')
    f = fit_font(lines, width - 130, 20, 14)
    style = f' style="--code-font:{f}px"' if f < 20 else ''
    rows = ''.join(f'<div class="r"><span class="n">{i}</span><code class="c">{l if l else " "}</code></div>'
                   for i, l in enumerate(highlight(code), start))
    fig = (f'<figure class="code"{style}><figcaption><i></i><i></i><i></i><span>{E(name)}</span>'
           f'<button type="button" class="copy" aria-label="Copy this code">Copy</button></figcaption>'
           f'<div class="code-b">{rows}</div></figure>')
    text_px = width - 130
    wraps = sum(max(1, -(-int(len(l) * f * CHW) // text_px)) for l in lines)
    return fig, 35 + 24 + wraps * f * 1.5


def out_fig(text, cap='PySpark — output', width=1460):
    if not text.strip():
        text = '(no output — this cell only defines things; nothing is printed)'
    lines = text.split('\n')
    f = fit_font(lines, width - 40, 17, 11)
    attr = f' data-f style="--of:{f}px"' if f < 17 else ''
    err = ' err' if re.search(r'^\w*(Exception|Error): |^Caused by: ', text, re.M) else ''
    h = 35 + 28 + len(lines) * f * 1.42
    if err:
        h = 35 + 28 + sum(max(1, -(-int(len(l) * 17 * CHW) // (width - 40))) for l in lines) * 17 * 1.42
    return f'<figure class="out{err}"><figcaption>{E(cap)}</figcaption><pre{attr}>{E(text)}</pre></figure>', h


def text_h(t, width, px_per_char=13.5, line_h=34.8):
    """Rough height of explanation text: paragraphs of plain text at the body size."""
    plain = re.sub(r'[`*]', '', t)
    h = 0
    for para in re.split(r'\n\s*\n', plain.strip()):
        for line in para.split('\n'):
            h += max(1, -(-int(len(line) * px_per_char) // width)) * line_h
        h += 14
    return h


def show_text(snap, limit=None):
    cols, rows = snap['cols'], snap['rows'][: limit or len(snap['rows'])]
    w = [max([len(c)] + [len(r[i]) for r in rows]) for i, c in enumerate(cols)]
    sep = '+' + '+'.join('-' * x for x in w) + '+'
    line = lambda vals: '|' + '|'.join(v.rjust(w[i]) for i, v in enumerate(vals)) + '|'
    body = [sep, line(cols), sep] + [line(r) for r in rows] + [sep]
    return '\n'.join(body)


REFS = {}


def rt_group(name, snap, key, cap_suffix='before this code runs', limit=30):
    n = snap['count']
    rows = snap['rows'][:limit]
    dense = ' dense' if len(rows) > 10 else ''
    th = ''.join(f'<th>{E(c)}</th>' for c in snap['cols'])
    if rows:
        tb = ''.join('<tr>' + ''.join(f'<td class="n">{E(v)}</td>' if snap['num'][i] and v != 'NULL' else f'<td>{E(v)}</td>'
                                      for i, v in enumerate(r)) + '</tr>' for r in rows)
    else:
        tb = f'<tr><td class="empty" colspan="{len(snap["cols"])}">no rows yet</td></tr>'
    more = ''
    if n > len(rows):
        rk = f'{key}:{name}'
        REFS[rk] = show_text(snap)
        more = f'<button type="button" class="refbtn more" data-ref="{E(rk)}">+ {n - len(rows)} more rows — view all {n}</button>'
    word = 'row' if n == 1 else 'rows'
    return (f'<div class="rt-group"><p class="rt-cap"><b>{E(name)}</b> · {n} {word} · {E(cap_suffix)}</p>'
            f'<div class="rt-blocks"><table class="rt{dense}"><thead><tr>{th}</tr></thead><tbody>{tb}</tbody></table></div>{more}</div>')


TIP_SVG = ('<svg class="bulb" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2.8a6.4 6.4 0 0 0-3.9 11.5c.7.6 1.1 1.4 1.2 2.3l.1.6h5.2l.1-.6c.1-.9.5-1.7 1.2-2.3A6.4 6.4 0 0 0 12 2.8z" fill="#ffd84d" stroke="#8a6500" stroke-width="1.5" stroke-linejoin="round"/>'
           '<path d="M9.6 19.6h4.8M10.4 22h3.2" stroke="#8a6500" stroke-width="1.6" stroke-linecap="round"/><path d="M10.4 11.4 12 13l1.6-1.6M12 13v4" stroke="#8a6500" stroke-width="1.2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>')


def tip(t):
    return f'<aside class="note tip">{TIP_SVG}<div class="tip-body">{inline(t)}</div></aside>'


FOOT = None


def foot():
    return FOOT


# ───────────────────────── slide builders ─────────────────────────
class Deck:
    def __init__(self):
        self.slides = []          # html strings
        self.menu = []

    @property
    def n(self):
        return len(self.slides) + 1

    def add(self, cls, ch, label, inner):
        sid = self.n
        self.slides.append(f'<section class="slide {cls}" id="s{sid}" data-ch="{E(ch)}" aria-label="{E(label)}">{inner}{foot()}</section>')
        return sid


QP_W, QP_H = 832, 615        # question-page right column: width and usable height of the body
NT_W, NT_H = 1460, 640       # full-width notes slide


def paginate(blocks, avail):
    """Greedy: keep blocks in order, start a new page when the next block does not fit."""
    pages, cur, used = [], [], 0
    for html_, h in blocks:
        if cur and used + h > avail:
            pages.append(cur); cur, used = [], 0
        cur.append(html_); used += h + 14
    if cur:
        pages.append(cur)
    return pages


def notes_slide(deck, ch, s, res, eyebrow, cls='notes'):
    t = s['t']
    r = res.get(item_id('sec', t), {}) if s.get('code') else {}
    code = s.get('code')
    tables = s.get('tables', [])
    ran = s.get('run', True)
    outtxt = r.get('out', '') if ran else s.get('out', '')
    if code and ran and not r:
        raise SystemExit(f'No output recorded for [{t}] — run build.py --run {ch.KEY}')
    show_out = bool(code) and not s.get('no_out') and (ran or outtxt)
    qp = bool(tables and r.get('tables'))
    W, H = (QP_W, QP_H) if qp else (NT_W, NT_H)
    name = 'query.py' if ran else s.get('file', 'example — not run in these notes')
    lbl = (lambda n, txt, extra='': f'<p class="qlbl{extra}"><i>{n}</i>{txt}</p>') if qp else (lambda n, txt, extra='': f'<p class="lbl{extra}">{txt}</p>')

    blocks = []
    if s.get('lead'):
        blocks.append((paras(s['lead']), text_h(s['lead'], W)))
    if s.get('md'):
        extra = mdblock(s['md']).replace('<table>', '<div class="tbl-wrap"><table>').replace('</table>', '</table></div>')
        blocks.append((extra, 60 + 46 * s['md'].count('\n')))
    if code:
        # side by side only on full-width slides, when both halves stay readable
        half = (W - 14) // 2
        cf_s, ch_s = code_fig(code, name, half)
        of_s, oh_s = out_fig(outtxt, width=half) if show_out else ('', 0)
        cf, chh = code_fig(code, name, W)
        of, oh = out_fig(outtxt, width=W) if show_out else ('', 0)
        stacked = chh + oh + (14 if show_out else 0)
        side_ok = (not qp and show_out and s.get('side', True) and 'data-f style="--of:1' not in of_s
                   and 'code-font:14' not in cf_s and max(ch_s, oh_s) < stacked * 0.8)
        code_lbl = lbl(3, 'Code') if qp else ''
        if side_ok and max(ch_s, oh_s) <= H:
            blocks.append((f'{code_lbl}<div class="pair side">{cf_s}{of_s}</div>', max(ch_s, oh_s) + (41 if qp else 0)))
        elif stacked + (41 if qp else 0) <= H or not show_out:
            blocks.append((f'{code_lbl}<div class="pair">{cf}{of}</div>', stacked + (41 if qp else 0)))
        else:
            # code alone; split it only when it is taller than a page
            if chh + 41 <= H:
                blocks.append((f'{code_lbl}<div class="pair">{cf}</div>', chh + (41 if qp else 0)))
            else:
                hl = highlight(code)
                lines = code.rstrip('\n').split('\n')
                per = max(8, int((H - 100) / 30))
                for k in range(0, len(lines), per):
                    part = '\n'.join(lines[k:k + per])
                    f2, h2 = code_fig(part, name, W, start=k + 1)
                    # re-highlight per chunk loses multi-line strings' colour at the cut; acceptable
                    blocks.append(((code_lbl if k == 0 else '') + f'<div class="pair">{f2}</div>', h2 + (41 if qp and k == 0 else 0)))
            olines = (outtxt or '').split('\n')
            of_font = fit_font(olines, W - 40, 17, 11)
            per = max(6, int((H - 110) / (of_font * 1.42)))
            for k in range(0, len(olines), per):
                part = '\n'.join(olines[k:k + per])
                f2, h2 = out_fig(part, 'PySpark — output' + (' (continued)' if k else ''), W)
                blocks.append(((lbl(3, 'Output') if qp and k == 0 else '') + f'<div class="pair">{f2}</div>', h2 + (41 if qp and k == 0 else 0)))
    if s.get('expl'):
        head = lbl(4, 'Explanation', ' expl-l') if (qp or code) else ''
        paras_ = re.split(r'\n\s*\n', s['expl'].strip())
        # keep the explanation together unless it alone is taller than a page
        if text_h(s['expl'], W) + 41 <= H:
            blocks.append((head + paras(s['expl']), text_h(s['expl'], W) + (41 if head else 0)))
        else:
            for k, para in enumerate(paras_):
                blocks.append(((head if k == 0 else '') + paras(para), text_h(para, W) + (41 if head and k == 0 else 0)))
    if s.get('tip'):
        blocks.append((tip(s['tip']), text_h(s['tip'], W - 120, 11.5, 30) + 40))

    pages = paginate(blocks, H)
    first = None
    for k, page in enumerate(pages):
        cont = ' <span class="cont">(continued)</span>' if k else ''
        label = t + (' (continued)' if k else '')
        body = '\n'.join(page)
        if qp:
            groups = ''.join(rt_group(nm, r['tables'][nm], f'{ch.KEY}-{deck.n}') for nm in tables)
            ml = 'Main table' if len(tables) == 1 else 'Main tables'
            inner = (f'<p class="eyebrow">{eyebrow}</p><div class="qpage"><aside class="qdata"><p class="qlbl"><i>1</i>{ml}</p><div class="ref">{groups}</div></aside>\n'
                     f'<div class="qmain"><p class="qlbl"><i>2</i>Question</p><h2>{inline(t)}{cont}</h2><div class="body">{body}</div></div></div>')
            sid = deck.add(f'{cls} qp', ch.KEY, label, inner)
        else:
            inner = f'<p class="eyebrow">{eyebrow}</p><h2>{inline(t)}{cont}</h2><div class="body">{body}</div>'
            sid = deck.add(cls, ch.KEY, label, inner)
        first = first or sid
    return first


def gap_slide(deck, ch, g, res):
    t = g['t']
    tag = f'<span class="tag lec">{E(g.get("tag", "Deep dive"))}</span>'
    eyebrow = f'Chapter {E(ch.KEY)} · {tag}'
    r = res.get(item_id('gap', t), {})
    return notes_slide(deck, ch, dict(g), {item_id('sec', t): r}, eyebrow, cls='gap')


MCQ_TAG = {'concept': '<span class="tag mcq">Concept</span>', 'output': '<span class="tag mcq">Predict the output</span>',
           'debug': '<span class="tag debug">Debug the code</span>'}


def mcq_slide(deck, ch, k, q, res):
    opts = ''.join(f'<button type="button" class="opt" data-i="{i}"><b>{"ABCD"[i]}</b><span>{inline(o)}</span></button>' for i, o in enumerate(q['o']))
    code = code_fig(q['code'], 'buggy.py' if q.get('kind') == 'debug' else 'question.py', 760)[0] if q.get('code') else ''
    if q.get('code') and q.get('run'):
        r = res.get(item_id('mcq', q['q'][:60], k))
        if not r:
            raise SystemExit(f'No output recorded for MCQ {q["q"][:50]}')
        if q.get('check'):
            if q['check'] not in r['out']:
                raise SystemExit(f'MCQ check failed: {q["check"]!r} not in output of {q["q"][:50]}:\n{r["out"]}')
    grid = 'has-code' if code else ''
    why = f'<p class="why"><b>{"ABCD"[q["a"]]}.</b> {inline(q["why"])}</p>'
    inner = (f'<p class="eyebrow">Chapter {E(ch.KEY)} · MCQ</p><div class="mcq-top">{MCQ_TAG[q.get("kind", "concept")]}<span class="tag score" hidden></span></div>\n'
             f'<div class="body mcq-grid {grid}" data-ans="{q["a"]}">\n<div class="mq"><p class="q">{inline(q["q"])}</p>{code}</div>\n'
             f'<div class="mo"><div class="opts">{opts}</div>\n<div class="reveal" hidden><p class="verdict"></p>{why}</div>\n'
             f'<button type="button" class="show">Show answer</button></div></div>')
    plain = re.sub(r'[`*]', '', q['q'])
    return deck.add('mcq', ch.KEY, plain, inner)


def practice_slides(deck, ch):
    p = ch.PRACTICE
    items = []
    lis = ''.join(f'<li>{inline(x)}</li>' for x in p['kinds'])
    nprob = len(p.get('problems', []))
    note = f'<p class="note-s">{inline(p["note"])}</p>' if p.get('note') else ''
    inner = (f'<p class="eyebrow">Chapter {E(ch.KEY)} · <span class="tag prac">Practice</span></p><h2>What to practise after this chapter</h2><div class="body">\n'
             f'<div class="list big"><ul>{lis}</ul></div>\n{note}</div>')
    items.append({'n': deck.add('practice', ch.KEY, 'What to practise after this chapter', inner), 't': 'What to practise after this chapter', 'k': 'practice'})
    probs = p.get('problems', [])
    for start in range(0, len(probs), 5):
        chunk = probs[start:start + 5]
        lis = ''
        for (num, slug, title, lvl, why) in chunk:
            url = f'https://leetcode.com/problems/{slug}/'
            lis += (f'<li><a href="{url}" target="_blank" rel="noopener"><span class="pn">{num}.</span> {E(title)}</a>\n'
                    f'<span class="meta"><span class="lvl {lvl.lower()}">{lvl}</span><span class="free">Free</span></span><span class="why">{inline(why)}</span></li>')
        title = 'LeetCode problems — solve them in PySpark'
        inner = (f'<p class="eyebrow">Chapter {E(ch.KEY)} · <span class="tag prac">Practice</span></p><h2>{title}</h2>\n'
                 f'<div class="body"><ol class="probs lc" start="{start + 1}">{lis}</ol></div>')
        items.append({'n': deck.add('practice', ch.KEY, title, inner), 't': title, 'k': 'practice'})
    return items


def build_chapter(deck, ch, res):
    items = []
    nsec = len(ch.SECTIONS) + (1 if getattr(ch, 'SETUP', None) else 0)
    nextra = len(getattr(ch, 'EXTRA', []))
    nmcq = len(getattr(ch, 'MCQS', []))
    nprob = len(ch.PRACTICE.get('problems', [])) if getattr(ch, 'PRACTICE', None) else 0
    # opener (page count patched after the chapter is built)
    inside = f'<li class=""><b>{nsec}</b><span>sections with code and real output</span></li>'
    if nextra:
        inside += f'<li class="lec"><b>{nextra}</b><span>deep-dive slides</span></li>'
    if nmcq:
        inside += f'<li class="mcq"><b>{nmcq}</b><span>MCQs, predict-the-output and debug questions</span></li>'
    if nprob:
        inside += f'<li class="prac"><b>{nprob}</b><span>LeetCode problems to solve in PySpark</span></li>'
    tables = f'<p class="tables">Data: {", ".join(f"<code>{E(x)}</code>" for x in ch.DATA)}</p>' if getattr(ch, 'DATA', None) else ''
    inner = (f'<div class="op">\n<div class="op-l"><div class="bignum">{E(ch.KEY)}</div><p class="eyebrow">Chapter {E(ch.KEY)} · {E(ch.WEEK)}</p>\n'
             f'<h1>{E(ch.TITLE)}</h1><div class="rule"></div>{tables}</div>\n'
             f'<div class="op-r"><p class="pgtotal"><b>@@PAGES@@</b> pages in this chapter</p><p class="lbl">In this chapter</p><ul class="inside">{inside}</ul>\n'
             f'<div class="op-progress" data-prog="{E(ch.KEY)}"><span>Your progress</span><div class="bar"><i></i></div></div></div></div>')
    first = deck.add('opener', ch.KEY, ch.TITLE, inner)
    eyebrow = f'Chapter {E(ch.KEY)} · Class notes'
    if getattr(ch, 'SETUP', None):
        s = {'t': ch.SETUP_TITLE, 'code': ch.SETUP, 'expl': ch.SETUP_EXPL, 'tip': getattr(ch, 'SETUP_TIP', None)}
        r = {item_id('sec', ch.SETUP_TITLE): res.get(item_id('setup', ch.SETUP_TITLE), {})}
        items.append({'n': notes_slide(deck, ch, s, r, eyebrow), 't': ch.SETUP_TITLE, 'k': 'notes'})
    for s in ch.SECTIONS:
        if s.get('divider'):
            inner = (f'<div class="div-in"><p class="eyebrow">Chapter {E(ch.KEY)}</p><h1>{inline(s["t"])}</h1>\n'
                     + (f'<p class="lead">{inline(s["lead"])}</p>' if s.get('lead') else '') + '</div>')
            items.append({'n': deck.add('divider', ch.KEY, s['t'], inner), 't': s['t'], 'k': 'divider'})
            continue
        items.append({'n': notes_slide(deck, ch, s, res, eyebrow), 't': s['t'], 'k': 'notes'})
    # points to be noted
    if getattr(ch, 'POINTS', None):
        pt = f'Points to be Noted — Chapter {ch.KEY}'
        for k, part in enumerate(ch.POINTS.split('<!--break-->')):
            start = len(re.findall(r'^\d+\. ', ch.POINTS.split('<!--break-->')[0], re.M)) + 1 if k else 1
            pts = mdblock(part)
            if k:
                pts = pts.replace('<ol>', f'<ol start="{start}">', 1)
            cont = ' <span class="cont">(continued)</span>' if k else ''
            inner = f'<p class="eyebrow">Chapter {E(ch.KEY)} · Summary</p><h2>{pt}{cont}</h2><div class="body"><div class="list">{pts}</div></div>'
            sid = deck.add('points', ch.KEY, pt + (' (continued)' if k else ''), inner)
            if not k:
                items.append({'n': sid, 't': pt, 'k': 'points'})
    extra = getattr(ch, 'EXTRA', [])
    if extra:
        lis = ''.join(f'<li>{inline(g["t"])}</li>' for g in extra)
        inner = (f'<div class="div-in"><p class="eyebrow">Chapter {E(ch.KEY)} · <span class="tag lec">Deep dive</span></p><h1>{E(ch.EXTRA_TITLE)}</h1>\n'
                 f'<p class="lead">{inline(ch.EXTRA_LEAD)}</p><ul class="gap-list">{lis}</ul></div>')
        items.append({'n': deck.add('divider', ch.KEY, ch.EXTRA_TITLE, inner), 't': ch.EXTRA_TITLE, 'k': 'divider'})
        for g in extra:
            items.append({'n': gap_slide(deck, ch, g, res), 't': g['t'], 'k': 'gap'})
    mcqs = getattr(ch, 'MCQS', [])
    if mcqs:
        inner = (f'<div class="div-in"><p class="eyebrow">Chapter {E(ch.KEY)} · <span class="tag mcq">MCQs</span></p><h1>MCQ time — {len(mcqs)} questions</h1>\n'
                 f'<p class="lead">Read the question, pick an option (click it or press <kbd>A</kbd>–<kbd>D</kbd>), and the explanation opens underneath. '
                 f'<b>Predict the output</b> questions were run on real Spark; <b>Debug the code</b> questions show code that fails or gives a wrong answer.</p>\n'
                 f'<button type="button" class="btn rf-start" data-act="rapid" data-ch="{E(ch.KEY)}">Start rapid fire — {len(mcqs)} questions, 2 minutes each</button></div>')
        items.append({'n': deck.add('divider', ch.KEY, 'MCQ time', inner), 't': 'MCQ time', 'k': 'divider'})
        for k, q in enumerate(mcqs):
            mcq_slide(deck, ch, k, q, res)
    if getattr(ch, 'PRACTICE', None):
        items += practice_slides(deck, ch)
    pages = deck.n - first
    deck.slides[first - 1] = deck.slides[first - 1].replace('@@PAGES@@', str(pages))
    deck.menu.append({'key': ch.KEY, 'title': ch.TITLE, 'n': first, 'items': items})
    return first, pages, nsec, nextra, nmcq, nprob


CH_COLORS = {
    '0': ('#334155', '#f1f5f9', '#cbd5e1', '#1e293b'), '1': ('#2563eb', '#eef4ff', '#c7d9fd', '#1d4ed8'),
    '2': ('#0f8a80', '#e9f8f6', '#b5e8e2', '#0b6e66'), '3': ('#b45309', '#fff6e6', '#fbdca6', '#92400e'),
    '4': ('#64748b', '#f3f5f8', '#d3dae3', '#475569'), '5': ('#ca8a04', '#fefbe8', '#f6e3a1', '#a16207'),
    '6': ('#15803d', '#ecf9f0', '#bce8cb', '#166534'), '7': ('#0e7490', '#e8f7fb', '#b3e3ef', '#155e75'),
    '8': ('#a21caf', '#fbeefd', '#f0c4f5', '#86198f'), '9': ('#be185d', '#fdeef4', '#f8c5da', '#9d174d'),
    '10': ('#c2410c', '#fff2ea', '#fed2b8', '#9a3412'), '11': ('#b91c1c', '#fdeeee', '#f6c4c4', '#991b1b'),
    '12': ('#0369a1', '#e9f5fd', '#b9e0f7', '#075985'), '13': ('#7c3aed', '#f3eeff', '#d9c8fb', '#6425d0'),
    '14': ('#4f46e5', '#efeffe', '#cfcdfb', '#3730a3'), '15': ('#047857', '#e8f8f1', '#b0e6cf', '#065f46'),
    '16': ('#4d7c0f', '#f1f8e6', '#d3e9b0', '#3f6212'), '17': ('#475569', '#f1f4f8', '#cfd8e3', '#334155'),
}


def render(chapters, results):
    global FOOT
    FOOT = ('<footer class="sfoot"><span>Satyaki Das — Assistant Professor, Rishihood University x Newton School of Technology</span>'
            '<span class="sf-r"><a class="soc" href="https://github.com/edusatyaki" target="_blank" rel="noopener"><svg aria-hidden="true"><use href="#i-gh"/></svg>GitHub</a>'
            '<a class="soc" href="https://www.linkedin.com/in/satyaki-das-25593714a/" target="_blank" rel="noopener"><svg aria-hidden="true"><use href="#i-li"/></svg>LinkedIn</a>'
            f'<span>{SPARK_LABEL}</span></span></footer>')
    deck = Deck()
    stats = {'sec': 0, 'extra': 0, 'mcq': 0, 'prob': 0}
    # cover + how-to (filled after counting)
    deck.add('cover', '', 'PySpark Class Notes', '@@COVER@@')
    howto = (
        '<h2>How to read each chapter</h2><div class="body">\n<aside class="note"><p>Every code block in these notes was executed on '
        f'<strong>Apache Spark 4.2 (PySpark)</strong> in local mode, and the outputs shown are the <strong>actual results</strong> Spark printed, not typed by hand.</p>\n'
        '<p><strong>How to read each chapter</strong></p>\n<ol>\n<li>The chapter opens with <strong>the data</strong>: the code that builds its DataFrames. Run that first.</li>\n'
        '<li>Then each topic comes as <strong>Question → Code → Output → Explanation</strong>, with the DataFrame it works on shown beside it.</li>\n'
        '<li>Each chapter closes with <strong>Points to be Noted</strong>, deep-dive slides, MCQs and practice problems.</li>\n</ol>\n</aside>\n'
        '<p class="lbl">Set up once</p>\n'
        + code_fig('pip install pyspark            # Spark 4.x needs Java 17 or 21\n'
                   'brew install openjdk@17        # macOS; use your package manager elsewhere\n'
                   'export JAVA_HOME=$(/usr/libexec/java_home -v 17)', 'terminal')[0] +
        '\n<div class="add-grid">\n<div><b><span class="tag lec">Deep dive</span></b><p>Topics beyond the basics — internals, performance and the questions interviews ask — each with code run on Spark.</p></div>\n'
        '<div><b><span class="tag mcq">MCQs</span></b><p>Concept questions, predict-the-output questions run on real Spark, and debug-the-code questions. Click an option to check it.</p></div>\n'
        '<div><b><span class="tag prac">Practice</span></b><p>After every chapter: what to practise, then LeetCode problems to solve with the DataFrame API.</p></div>\n</div></div>')
    deck.add('plain', '', 'How to read each chapter', howto)
    home_cards = []
    for ch in chapters:
        res = results.get(ch.KEY, {})
        first, pages, nsec, nextra, nmcq, nprob = build_chapter(deck, ch, res)
        stats['sec'] += nsec; stats['extra'] += nextra; stats['mcq'] += nmcq; stats['prob'] += nprob
        home_cards.append((ch, first, pages))
    # closing slide
    closing = ('<div class="cover-in"><p class="eyebrow">The end of the notes</p><h1>You can now read a Spark plan<br><span>and write the pipeline behind it</span></h1><div class="rule"></div>\n'
               '<p class="lead">Go back to any chapter from the menu, re-run the rapid-fire quiz on the chapters you missed, and rebuild the ShopKart capstone on your own data: '
               'Bronze, Silver and Gold Delta tables, an SCD2 dimension, quality gates, tests, and the physical plan of the heaviest query in the README.</p>\n'
               '<div class="cover-go"><button type="button" class="btn" data-act="menu">Chapters</button><button type="button" class="btn ghost" data-act="revisit">Revisit list</button>'
               '<button type="button" class="btn ghost" data-act="rapid" data-ch="*">Rapid fire — all chapters</button></div></div>')
    deck.add('cover', '', 'The end', closing)
    nch = len(chapters)
    cover = ('<div class="cover-in">\n<p class="eyebrow">Class notes · Data Engineering</p>\n<h1>PySpark Class Notes<br><span>from Spark theory to a lakehouse pipeline, presented</span></h1>\n<div class="rule"></div>\n'
             '<p class="lead">Every topic as a question, the PySpark code, the real output from Spark and an explanation — with the DataFrame beside every query, '
             'deep dives into Spark internals, MCQs, predict-the-output questions, and the LeetCode problems to solve in PySpark after each chapter.</p>\n'
             f'<div class="cover-stats"><div><b>{nch}</b><span>chapters</span></div><div><b>{stats["sec"]}</b><span>sections with real output</span></div>'
             f'<div><b>{stats["extra"]}</b><span>deep-dive slides</span></div><div><b>{stats["mcq"]}</b><span>MCQs</span></div><div><b>{stats["prob"]}</b><span>practice problems</span></div></div>\n'
             '<div class="cover-go"><button type="button" class="btn" data-act="start">Start reading →</button><button type="button" class="btn ghost" data-act="menu">Chapters</button>'
             '<button type="button" class="btn ghost" data-act="revisit">Revisit list</button><button type="button" class="btn ghost" data-act="rapid" data-ch="*">Rapid fire — all chapters</button></div>\n'
             '<p class="keys">Press <kbd>R</kbd> on any page to mark it for revisit · <kbd>P</kbd> hides outputs so you can predict them · <kbd>?</kbd> shows every shortcut</p>\n</div>')
    deck.slides[0] = deck.slides[0].replace('@@COVER@@', cover)

    # home page
    cards = ''.join(
        f'<button type="button" class="hc" data-ch="{E(ch.KEY)}" data-go="{first}">\n<span class="hc-n">{E(ch.KEY)}</span><span class="hc-t">{E(ch.TITLE)}</span>\n'
        f'<span class="hc-m">{E(ch.WEEK)} · {pages} pages</span>\n<span class="hc-bar"><i></i></span><span class="hc-p">Not started</span></button>\n'
        for ch, first, pages in home_cards)
    social = ('<a class="soc" href="https://github.com/edusatyaki" target="_blank" rel="noopener"><svg aria-hidden="true"><use href="#i-gh"/></svg>GitHub</a>'
              '<a class="soc" href="https://www.linkedin.com/in/satyaki-das-25593714a/" target="_blank" rel="noopener"><svg aria-hidden="true"><use href="#i-li"/></svg>LinkedIn</a>')
    home = (f'<div id="home" hidden>\n<header class="h-top"><b class="h-logo"><span>Py</span> Spark Notes</b><nav class="h-nav">{social}</nav></header>\n'
            '<section class="h-hero">\n<div class="h-copy">\n<p class="h-eyebrow">Data Engineering · Apache Spark 4.2 · Python</p>\n<h1>PySpark<br><span>Class Notes</span></h1>\n'
            '<p class="h-lead">Every topic as a question with the DataFrame beside it, the PySpark code, the real output from Spark and an explanation — plus deep dives into '
            'Spark internals, MCQs, rapid-fire quizzes and LeetCode problems to solve in PySpark after each chapter.</p>\n'
            '<div class="h-cta"><button type="button" class="hb main" data-home="continue">Start reading</button><button type="button" class="hb" data-home="start">From page 1</button>'
            '<button type="button" class="hb" data-home="rapid">Rapid fire</button><button type="button" class="hb" data-home="revisit">Revisit list <b class="h-rv">0</b></button></div>\n'
            f'<ul class="h-stats"><li><b>{len(deck.slides)}</b> pages</li><li><b>{nch}</b> chapters</li><li><b>{stats["mcq"]}</b> MCQs</li><li><b>{stats["prob"]}</b> practice problems</li></ul>\n</div>\n'
            '<figure class="h-term" aria-label="PySpark code from the notes running in the pyspark shell"><figcaption><i></i><i></i><i></i><span>pyspark — local[4]</span></figcaption>'
            '<pre class="h-code"></pre><pre class="h-out"></pre></figure>\n</section>\n'
            f'<section class="h-chs"><h2>Chapters</h2><p class="h-sub">Pick up where you left off — the bar on each card fills as you read.</p><div class="h-grid">{cards}</div></section>\n'
            f'<footer class="h-foot"><span>Satyaki Das — Assistant Professor, Rishihood University x Newton School of Technology</span><span class="sf-r">{social}</span></footer>\n</div>\n')

    demos = []
    for ch in chapters:
        for s in ch.SECTIONS:
            if s.get('home'):
                r = results.get(ch.KEY, {}).get(item_id('sec', s['t']))
                if r:
                    demos.append({'ch': ch.KEY, 'q': s['code'].strip(), 'out': r['out']})
    head = (TPL / 'head.html').read_text()
    head = head.replace('<title>PostgreSQL Class Notes</title>', '<title>PySpark Class Notes</title>')
    head = re.sub(r'<meta name="description" content="[^"]*">',
                  '<meta name="description" content="PySpark class notes: every topic with code run on Apache Spark 4.2, the real output, MCQs, rapid-fire quizzes and LeetCode practice in PySpark.">', head)
    head = head.replace('/* PostgreSQL Class Notes — presentation.', '/* PySpark Class Notes — presentation.')
    css = '\n'.join(f':is(.slide, .hc)[data-ch="{k}"] {{ --cc: {a}; --cc-soft: {b}; --cc-mid: {c}; --cc-deep: {d}; }}' for k, (a, b, c, d) in CH_COLORS.items())
    css += '\n.xl { margin: 0; padding-left: 1.2em; } .xl li { margin: 2px 0; }\n.expl + .expl { margin-top: 6px; }\n'
    head = head.replace('</style>', css + '\n</style>')
    tpl = lambda n: (TPL / f'{n}.html').read_text()
    scripts = (f'<script>const MENU = {json.dumps(deck.menu, ensure_ascii=False)};\nconst HOME_DEMO = {json.dumps(demos, ensure_ascii=False)};\n'
               f'const REFS = {json.dumps(REFS, ensure_ascii=False)};</script>\n')
    page = head + '\n' + tpl('pre') + '\n'.join(deck.slides) + tpl('mid') + home + tpl('tail') + scripts + tpl('engine')
    (ROOT / 'index.html').write_text(page)
    print(f'index.html: {len(deck.slides)} slides, {len(page) // 1024} KB · {stats}')


if __name__ == '__main__':
    chapters = load_chapters()
    if '--run' in sys.argv:
        run_all(chapters)
    results = json.loads(OUT_JSON.read_text()) if OUT_JSON.exists() else {}
    render(chapters, results)
