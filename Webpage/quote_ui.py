QUOTE_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
  <meta charset="utf-8">
  <title>Quotation Lookup</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{ --ink:#0f172a; --muted:#6b7280; --bg:#f7fafc; --hdr:#f8fafc; }
    html,body{ background:var(--bg); color:var(--ink); }
    body{ padding:28px; }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); }
    .summary{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:1rem; }
    .metric{ border:1px solid #e2e8f0; border-radius:12px; padding:1rem; background:#fff; }
    .metric .label{ text-transform:uppercase; font-size:.75rem; letter-spacing:.08em; color:var(--muted); font-weight:600; }
    .metric .value{ font-size:1.3rem; font-weight:700; }
    .table-responsive{ max-height:70vh; overflow:auto; }
    .table thead th{ position:sticky; top:0; z-index:2; background:var(--hdr); }
    .th-projected{ background:#dcfce7 !important; }
    .cell-projected-min{ background:#bbf7d0 !important; font-weight:700; }
    .suggest-head, .suggest-row{
      display:grid;
      grid-template-columns:minmax(220px, 2.2fr) minmax(90px, .8fr) minmax(130px, 1fr) minmax(130px, 1fr);
      gap:.75rem;
      align-items:center;
    }
    .suggest-head{
      padding:.5rem .75rem;
      font-size:.72rem;
      text-transform:uppercase;
      letter-spacing:.06em;
      color:var(--muted);
      background:#f8fafc;
      border:1px solid #dee2e6;
      border-bottom:none;
      border-radius:.5rem .5rem 0 0;
    }
    .suggest-row .col-num{ text-align:right; font-variant-numeric:tabular-nums; }
    .list-group-item.suggest-red{
      background:#fff1f2;
      border-color:#fecdd3;
    }
    .list-group-item.suggest-red:hover{
      background:#ffe4e6;
    }
    .list-group-item.suggest-green{
      background:#f0fdf4;
      border-color:#bbf7d0;
    }
    .list-group-item.suggest-green:hover{
      background:#dcfce7;
    }
    .suggest-empty{ padding:.75rem; color:var(--muted); background:#fff; border:1px solid #dee2e6; border-radius:.5rem; }
    .quote-legend{
      display:flex;
      gap:1rem;
      align-items:center;
      flex-wrap:wrap;
      font-size:.82rem;
      color:var(--muted);
      margin-top:.45rem;
    }
    .quote-legend .swatch{
      width:.8rem;
      height:.8rem;
      border-radius:999px;
      display:inline-block;
      margin-right:.35rem;
      vertical-align:middle;
      border:1px solid rgba(15,23,42,.08);
    }
    .quote-legend .swatch-red{ background:#fecdd3; }
    .quote-legend .swatch-green{ background:#bbf7d0; }
  </style>
</head>
<body>
  <div class="d-flex justify-content-between align-items-center mb-2">
    <div>
      <div class="h3 m-0">Quotation Lookup</div>
      <div class="text-muted small">Loaded {{ loaded_at }}</div>
    </div>
    <div class="d-flex gap-2">
      <a class="btn btn-sm btn-outline-secondary" href="/">Home</a>
    </div>
  </div>

  <form class="row gy-3 gx-4 align-items-end justify-content-start mb-4" method="get">
    <div class="col-12 col-md-9">
      <label class="form-label" for="quote-item">Item (fuzzy search)</label>
      <div style="position:relative;">
        <input id="quote-item" autocomplete="off" class="form-control form-control-lg"
               style="height:60px;font-size:1.05rem"
               name="item"
               placeholder="Type item name or partial code"
               value="{{ item_val or '' }}">
        <div id="quote-suggest-head" class="suggest-head"
             style="position:absolute; top:62px; left:0; right:0; z-index:1001; display:none;">
          <div>Item</div>
          <div class="text-end">Available</div>
          <div class="text-end">ATP (Exclude Unassigned SO)</div>
          <div class="text-end">ATP (All SO)</div>
        </div>
        <div id="quote-suggest" class="list-group"
             style="position:absolute; top:96px; left:0; right:0; z-index:1000; display:none; max-height:280px; overflow:auto;"></div>
      </div>
      <div class="quote-legend">
        <span><span class="swatch swatch-red"></span>red = Max 0</span>
        <span><span class="swatch swatch-green"></span>green = Max 99</span>
      </div>
    </div>
    <div class="col-6 col-md-auto">
      <button class="btn btn-primary px-4 w-100" style="height:52px;font-size:1rem;font-weight:600">Search</button>
    </div>
    <div class="col-6 col-md-auto">
      <a class="btn btn-outline-secondary w-100" style="height:52px;font-size:1rem;font-weight:600" href="/quotation_lookup?reload=1">Reload</a>
    </div>
  </form>

  <div class="summary mb-4">
    <div class="metric">
      <div class="label">Item</div>
      <div class="value">{{ item_val or '-' }}</div>
    </div>
    <div class="metric">
      <div class="label">Opening (On Hand snapshot)</div>
      <div class="value">
        {% if opening_qty is not none %}
          {{ opening_qty }}
        {% else %}
          ---
        {% endif %}
      </div>
    </div>
    <div class="metric">
      <div class="label">Earliest ATP Date (Qty 1)</div>
      <div class="value">
        {% if earliest_atp %}
          {{ earliest_atp }}
        {% else %}
          ---
        {% endif %}
      </div>
    </div>
  </div>

  <div class="card-lite bg-white">
    <div class="card-header fw-bold">Ledger Timeline</div>
    <div class="card-body">
      <div class="table-responsive">
        <table class="table table-sm table-bordered table-hover align-middle">
          <thead class="table-light text-uppercase small text-muted">
            <tr>
              {% for c in ledger_columns %}
                <th class="{% if c == 'Projected_Qty' %}th-projected{% endif %}">{{ 'Projected_OnHand' if c == 'Projected_Qty' else c }}</th>
              {% endfor %}
            </tr>
          </thead>
          <tbody>
            {% if ledger_rows %}
              {% for r in ledger_rows %}
                <tr class="{% if r['Date'] == 'Lead Time Pending' %}table-warning{% elif r['_is_min_nav'] and (not r['Date'].startswith('2099')) %}table-warning{% endif %}">
                  {% for c in ledger_columns %}
                    <td class="{% if c == 'Projected_Qty' and r['_is_min_nav'] %}cell-projected-min{% endif %}">{{ r[c] }}</td>
                  {% endfor %}
                </tr>
              {% endfor %}
            {% else %}
              <tr><td colspan="{{ ledger_columns|length or 1 }}" class="text-center text-muted">No ledger rows for this item.</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
      <div class="text-muted small">Source: public.ledger_analytics and public.item_atp</div>
    </div>
  </div>

  <script>
  (function () {
    var input = document.getElementById('quote-item');
    var list = document.getElementById('quote-suggest');
    var head = document.getElementById('quote-suggest-head');
    var suggestTimer;
    function esc(value){
      if (value === null || value === undefined || value === '') return '---';
      return String(value).replace(/&/g,'&amp;').replace(/</g,'&lt;');
    }
    function hideList(){
      list.style.display = 'none';
      list.innerHTML='';
      if (head) head.style.display = 'none';
    }
    function showList(items){
      if (!items || !items.length) { hideList(); return; }
      list.innerHTML = items.map(function (it){
        var extraClass = it.highlight === 'red' ? ' suggest-red' : (it.highlight === 'green' ? ' suggest-green' : '');
        return '<button type="button" class="list-group-item list-group-item-action' + extraClass + '">' +
               '<div class="suggest-row">' +
               '<div>' + esc(it.item) + '</div>' +
               '<div class="col-num">' + esc(it.available) + '</div>' +
               '<div class="col-num">' + esc(it.min_regular) + '</div>' +
               '<div class="col-num">' + esc(it.min_2099) + '</div>' +
               '</div>' +
               '</button>';
      }).join('');
      if (head) head.style.display = 'grid';
      list.style.display = 'block';
    }
    if (input && list){
      input.addEventListener('input', function(){
        var q = input.value.trim();
        if (suggestTimer) clearTimeout(suggestTimer);
        if (!q){ hideList(); return; }
        suggestTimer = setTimeout(function(){
          fetch('/api/quotation_item_suggest?q=' + encodeURIComponent(q))
            .then(function(r){ return r.json(); })
            .then(function(j){ if (j && j.ok) showList(j.items); else hideList(); })
            .catch(function(){ hideList(); });
        }, 180);
      });
      list.addEventListener('click', function(e){
        var t = e.target.closest('.list-group-item');
        if (!t) return;
        var row = t.querySelector('.suggest-row > div');
        input.value = row ? row.textContent.trim() : t.textContent.trim();
        hideList();
      });
      document.addEventListener('click', function(e){
        if (!e.target.closest || (!e.target.closest('#quote-suggest') && !e.target.closest('#quote-item'))) hideList();
      });
    }
  })();
  </script>
</body>
</html>
"""
