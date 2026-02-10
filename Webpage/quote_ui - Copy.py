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
    <div class="col-12 col-md-6">
      <label class="form-label" for="quote-item">Item (fuzzy search)</label>
      <div style="position:relative;">
        <input id="quote-item" autocomplete="off" class="form-control form-control-lg"
               style="height:60px;font-size:1.05rem"
               name="item"
               placeholder="Type item name or partial code"
               value="{{ item_val or '' }}">
        <div id="quote-suggest" class="list-group"
             style="position:absolute; top:62px; left:0; right:0; z-index:1000; display:none; max-height:240px; overflow:auto;"></div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <label class="form-label" for="quote-qty">Requested Qty</label>
      <input id="quote-qty"
             class="form-control form-control-lg"
             style="height:60px;font-size:1.05rem"
             type="number"
             min="1"
             step="1"
             name="qty"
             value="{{ qty_val or 1 }}">
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
      <div class="label">Requested Qty</div>
      <div class="value">{{ qty_val or 1 }}</div>
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
      <div class="label">Earliest ATP (Requested Qty)</div>
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
    var suggestTimer;
    function hideList(){ list.style.display = 'none'; list.innerHTML=''; }
    function showList(items){
      if (!items || !items.length) { hideList(); return; }
      list.innerHTML = items.map(function (it){
        return '<button type="button" class="list-group-item list-group-item-action">' +
               it.replace(/&/g,'&amp;').replace(/</g,'&lt;') + '</button>';
      }).join('');
      list.style.display = 'block';
    }
    if (input && list){
      input.addEventListener('input', function(){
        var q = input.value.trim();
        if (suggestTimer) clearTimeout(suggestTimer);
        if (!q){ hideList(); return; }
        suggestTimer = setTimeout(function(){
          fetch('/api/item_suggest?q=' + encodeURIComponent(q))
            .then(function(r){ return r.json(); })
            .then(function(j){ if (j && j.ok) showList(j.items); else hideList(); })
            .catch(function(){ hideList(); });
        }, 180);
      });
      list.addEventListener('click', function(e){
        var t = e.target.closest('.list-group-item');
        if (!t) return;
        input.value = t.textContent.trim();
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
