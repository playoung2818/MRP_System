QUOTE_TPL = """
<!doctype html>
<html lang="en">
<head>
  <link rel="icon" href="/static/robot.svg?v=1" type="image/svg+xml">
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Quotation Lookup</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{ --ink:#0f172a; --muted:#6b7280; --bg:#f7fafc; --hdr:#f8fafc; }
    html,body{ background:var(--bg); color:var(--ink); }
    body{ padding:28px; }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); }
    .summary{ display:grid; grid-template-columns:minmax(180px,1fr) minmax(0,3fr); gap:.65rem; }
    .companion-list{ display:flex; flex-wrap:wrap; gap:.35rem 1rem; margin:.35rem 0 0; padding:0; list-style:none; font-size:.85rem; }
    .companion-list a{ overflow-wrap:anywhere; }
    @media (max-width:575.98px){ .summary{ grid-template-columns:1fr; } }
    .metric{ border:1px solid #e2e8f0; border-radius:10px; padding:.65rem .8rem; background:#fff; min-width:0; }
    .metric .label{ text-transform:uppercase; font-size:.75rem; letter-spacing:.08em; color:var(--muted); font-weight:600; }
    .metric .value{ font-size:1rem; font-weight:600; }
    .table-responsive{ max-height:70vh; overflow:auto; }
    .table thead th{ position:sticky; top:0; z-index:2; background:var(--hdr); }
    .th-projected{ background:#dcfce7 !important; }
    .cell-projected-min{ background:#bbf7d0 !important; font-weight:700; }
    .suggest-head, .suggest-row{
      display:grid;
      grid-template-columns:minmax(0, 2.2fr) minmax(90px, .8fr) minmax(90px, .8fr);
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
    .quote-search-form{
      display:grid;
      grid-template-columns:minmax(0,1fr) auto auto;
      align-items:start;
      gap:12px;
      margin:1.5rem 0 .55rem;
    }
    .quote-search-wrap{ position:relative; min-width:0; }
    .quote-search-control{ height:60px; font-size:1.05rem; }
    .quote-search-action{ height:60px; min-width:108px; font-size:1rem; font-weight:600; }
    @media (max-width:767.98px){
      .quote-search-form{ grid-template-columns:1fr 1fr; }
      .quote-search-wrap{ grid-column:1 / -1; }
      .quote-search-action{ width:100%; }
    }
  </style>
  <link rel="stylesheet" href="{{ url_for('static', filename='workspace.css') }}?v=2">
</head>
<body class="workspace-page">
  <a class="workspace-skip" href="#workspace-content">Skip to content</a>
  <nav class="workspace-nav" aria-label="Main navigation"><div class="workspace-nav-inner">
    <a class="workspace-brand" href="/" aria-label="Neousys home"><span class="workspace-brand-mark"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="icon icon-tabler icons-tabler-outline icon-tabler-robot" aria-hidden="true" focusable="false"><path stroke="none" d="M0 0h24v24H0z" fill="none" /><path d="M6 6a2 2 0 0 1 2 -2h8a2 2 0 0 1 2 2v4a2 2 0 0 1 -2 2h-8a2 2 0 0 1 -2 -2l0 -4" /><path d="M12 2v2" /><path d="M9 12v9" /><path d="M15 12v9" /><path d="M5 16l4 -2" /><path d="M15 14l4 2" /><path d="M9 18h6" /><path d="M10 8v.01" /><path d="M14 8v.01" /></svg></span><span><strong>NEOUSYS</strong><small>MRP WORKSPACE</small></span></a>
<a class="workspace-home" href="/">Home</a></div></nav>
  <main id="workspace-content" class="workspace-main">
  <div class="workspace-breadcrumb"><a href="/">Workspace</a><span aria-hidden="true">/</span><span>Quotations</span></div>
  <header class="workspace-header"><div><h1 class="workspace-title">Quotation lookup</h1><p class="workspace-description">Find a part, review its companions, and check upcoming movements.</p></div>
    <div class="workspace-header-actions"><span class="workspace-loaded">Loaded {{ loaded_at }}</span><a class="btn btn-sm btn-outline-secondary" href="/quotation_lookup/peripheral_status">SSD &amp; Memory Status</a></div>
  </header>

  <form class="workspace-search quote-search-form" method="get">
      <div class="quote-search-wrap">
        <label class="visually-hidden" for="quote-item">Item name or part number</label>
        <input id="quote-item" autocomplete="off" class="form-control form-control-lg quote-search-control"
               name="item"
               placeholder="Type item name or partial code"
               value="{{ item_val or '' }}">
        <div id="quote-suggest-head" class="suggest-head"
             style="position:absolute; top:40px; left:0; right:0; z-index:1001; display:none;">
          <div>Item</div>
          <div class="text-end">On Hand</div>
          <div class="text-end">Available</div>
        </div>
        <div id="quote-suggest" class="list-group"
             style="position:absolute; top:74px; left:0; right:0; z-index:1000; display:none; max-height:280px; overflow:auto;"></div>
      </div>
    <button class="btn btn-primary px-4 quote-search-action" type="submit">Search</button>
    <a class="btn btn-outline-secondary px-4 quote-search-action d-flex align-items-center justify-content-center" href="/quotation_lookup?reload=1">Reload</a>
  </form>
  <div class="quote-legend mb-4">
    <span><span class="swatch swatch-red"></span>red = Max 0</span>
    <span><span class="swatch swatch-green"></span>green = Max 99</span>
  </div>

  {% if companion_error %}
    <div class="alert alert-warning" role="alert">{{ companion_error }}</div>
  {% endif %}
  {% if item_cards %}
  <div class="summary mb-3">
    <div class="metric border-primary">
      <div class="label">Searched item</div>
      <div class="value" style="overflow-wrap:anywhere;">{{ item_cards[0].item }}</div>
    </div>
    <div class="metric">
      <div class="label">Top 5 companion items</div>
      <ul class="companion-list">
        {% for card in item_cards[1:6] %}
        <li><a href="{{ url_for('quotation_lookup', item=card.item) }}" class="text-decoration-none">{{ card.item }}</a></li>
        {% else %}
        <li class="text-muted">{{ 'Companion items unavailable.' if companion_error else 'No companion items saved.' }}</li>
        {% endfor %}
      </ul>
    </div>
  </div>
  {% endif %}

  <div class="card-lite bg-white">
    <div class="card-header fw-bold"><span>Ledger timeline</span><span class="workspace-table-note">{{ ledger_rows|length }} movements</span></div>
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
      <div class="text-muted small">Source: public.ledger_analytics</div>
    </div>
  </div>

  </main>

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
               '<div class="col-num">' + esc(it.on_hand) + '</div>' +
               '<div class="col-num">' + esc(it.available) + '</div>' +
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
