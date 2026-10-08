# Webpage/ui.py
ERR_TPL = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Data Error</title>
  <link rel="icon" href="/static/robot.svg?v=1" type="image/svg+xml">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{ --ink:#1f2937; --muted:#64748b; }
    body{ padding:28px; background:#f7fafc; color:var(--ink); }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); }
  </style>
</head>
<body>
  <div class="container">
    <div class="card-lite bg-white p-4">
      <div class="alert alert-danger m-0">
        <div class="fw-bold fs-5">Load Error</div>
        <div class="mt-2">{{ error }}</div>
      </div>
    </div>
  </div>

  
</body>
</html>
"""

INDEX_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/robot.svg?v=1" type="image/svg+xml">
  <meta charset="utf-8">
  <title>LT Check - Dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{
      --ink:#0f172a; --muted:#6b7280; --bg:#f7fafc;
      --ok-bg:#e7f8ed; --ok-fg:#137a2a;
      --warn-bg:#ffecec; --warn-fg:#a61b1b;
      --wait-bg:#fff3cd; --wait-fg:#664d03;
      --hdr:#f8fafc;
    }
    html,body{ background:var(--bg); color:var(--ink); min-height:100%; }
    body{ margin:0; }
    .shell{ display:grid; grid-template-columns:260px minmax(0,1fr); min-height:100vh; }
    .sidebar{
      background:linear-gradient(180deg,#0b1220 0%,#10192d 100%);
      color:#dbe7ff; padding:28px 20px; display:flex; flex-direction:column; gap:26px;
      box-shadow:inset -1px 0 0 rgba(148,163,184,.12);
    }
    .brand-title{ font-size:1.45rem; font-weight:800; letter-spacing:.02em; color:#fff; }
    .brand-sub{ font-size:.88rem; color:#94a3b8; }
    .nav-group{ display:flex; flex-direction:column; gap:8px; }
    .nav-label{ font-size:.72rem; text-transform:uppercase; letter-spacing:.12em; color:#7f8ba3; font-weight:700; }
    .nav-link{
      display:flex; align-items:center; gap:10px; border-radius:14px; padding:11px 13px;
      color:#dbe7ff; text-decoration:none; font-weight:600; transition:.18s ease;
    }
    .nav-link:hover{ background:rgba(255,255,255,.06); color:#fff; }
    .nav-link.active{ background:rgba(13,110,253,.18); color:#fff; box-shadow:0 0 0 1px rgba(13,110,253,.2) inset; }
    .nav-dot{ width:8px; height:8px; border-radius:50%; background:currentColor; opacity:.7; }
    .sidebar-note{
      margin-top:auto; padding:14px; border:1px solid rgba(148,163,184,.14); border-radius:16px;
      color:#9fb0d0; background:rgba(255,255,255,.03); font-size:.88rem;
    }
    .main{ padding:28px; }
    .topbar{ display:flex; justify-content:space-between; align-items:flex-start; gap:16px; margin-bottom:24px; }
    .greeting-wrap{ display:flex; align-items:center; gap:12px; }
    .greeting-image{
      width:112px; height:112px; flex:0 0 112px;
      object-fit:contain; object-position:center;
    }
    .eyebrow{ font-size:.78rem; text-transform:uppercase; letter-spacing:.12em; color:var(--muted); font-weight:700; }
    .hero-title{ font-size:2rem; font-weight:800; letter-spacing:-.03em; margin:.2rem 0; }
    .hero-sub{ color:var(--muted); }
    .topbar-actions{ display:flex; align-items:center; gap:12px; flex-wrap:wrap; justify-content:flex-end; }
    .loaded-badge{
      border:1px solid #dbe4f0; background:#fff; border-radius:999px; padding:.55rem .9rem;
      color:var(--muted); font-size:.88rem;
    }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); }
    .muted{ color:var(--muted); }
    .nowrap{ white-space:nowrap; }
    .clicky a{text-decoration:none}
    .clicky a:hover{text-decoration:underline}
            .table td, .table th{ vertical-align:middle; }
    .table tbody tr:nth-child(odd){ background:#fcfcfe; }
    .table tbody tr:hover{ background:#eef6ff; }
    .table-responsive{ max-height:70vh; overflow:auto; }
    .table thead th{ position:sticky; top:0; z-index:2; background:var(--hdr); white-space:nowrap; }
    .num{ text-align:right; font-variant-numeric: tabular-nums; }
    .neg{ color:var(--warn-fg); background:#fff6f6; }
    .zero{ color:var(--muted); }
    .badge-pill{ display:inline-block; padding:.25rem .6rem; border-radius:999px; font-weight:600; }
    .badge-ok{ background:var(--ok-bg); color:var(--ok-fg); }
    .badge-warn{ background:var(--warn-bg); color:var(--warn-fg); }
    .badge-wait{ background:var(--wait-bg); color:var(--wait-fg); }
    .num-center{ text-align:center; font-variant-numeric: tabular-nums; }
    .blue-cell{ color:#0d6efd; font-weight:600; }
    .num-center{ text-align:center; font-variant-numeric: tabular-nums; }
    .page-title{ text-align:center; font-size:2.7rem; font-weight:700; letter-spacing:.05em; margin-bottom:.35rem; text-transform:uppercase; }
    .page-sub{ text-align:center; color:var(--muted); margin-bottom:1.5rem; }
    .form-section label{ font-weight:600; font-size:.95rem; color:var(--muted); margin-bottom:.4rem; display:block; text-transform:uppercase; letter-spacing:.08em; }
    .summary-card{ border-radius:14px; box-shadow:0 6px 14px rgba(15,23,42,.08); }
    .summary-grid{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:1rem; }
    .summary-field{ border:1px solid #e2e8f0; border-radius:12px; padding:.75rem 1rem; background:#f8fafc; }
    .summary-label{ text-transform:uppercase; font-size:.75rem; letter-spacing:.08em; color:var(--muted); font-weight:600; }
    .summary-value{ font-size:1rem; font-weight:600; color:var(--ink); margin-top:.2rem; }
    .detail-link{ color:#0d6efd; font-weight:600; text-decoration:none; }
    .detail-link:hover{ text-decoration:underline; }
    .detail-panel{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); background:#fff; padding:1.5rem; margin-top:1rem; display:none; }
    .detail-panel h6{ text-transform:uppercase; font-size:.85rem; letter-spacing:.08em; font-weight:600; }
    .detail-panel .subcard{ border:1px solid #e2e8f0; border-radius:12px; padding:1rem; background:#f8fafc; height:100%; }
    .detail-panel .subcard.active{ border-color:#0d6efd; box-shadow:0 0 0 3px rgba(13,110,253,.15); }
    .search-card{ padding:20px; margin-bottom:24px; }
    .search-title{ font-size:1rem; font-weight:700; margin-bottom:.2rem; }
    .search-sub{ color:var(--muted); font-size:.92rem; margin-bottom:1rem; }
    .search-input{
      height:58px; border-radius:16px; font-size:1.02rem; border:1px solid #dbe4f0;
      background:#fbfdff;
    }
    .global-search-wrap{ position:relative; }
    .global-suggest{ position:absolute; left:0; right:0; top:86px; z-index:1000; display:none; max-height:320px; overflow:auto; }
    .global-suggest-row{ display:flex; align-items:center; justify-content:space-between; gap:12px; }
    .global-suggest-meta{ display:flex; align-items:center; gap:8px; flex:0 0 auto; }
    .global-suggest-customer{ color:#475569; font-size:.8rem; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .global-suggest-ship-date{ color:#64748b; font-size:.76rem; font-weight:700; white-space:nowrap; }
    .global-suggest-on-hand{ color:#334155; font-size:.76rem; font-weight:700; white-space:nowrap; }
    .global-suggest-type{ flex:0 0 auto; font-size:.72rem; font-weight:800; color:#0d6efd; background:#e7f0ff; border:1px solid #cfe0ff; border-radius:999px; padding:.14rem .5rem; }
    .section-title{ font-size:1rem; font-weight:700; margin-bottom:12px; }
    .metric-card{ padding:22px; margin-bottom:24px; position:relative; overflow:hidden; }
    .metric-link{ display:block; color:inherit; text-decoration:none; }
    .metric-link:hover{ color:inherit; background:#f8fbff; box-shadow:0 6px 18px rgba(13,110,253,.15); }
    .metric-link:focus-visible{ outline:3px solid #0d6efd; outline-offset:3px; }
    .metric-card::after{
      content:""; position:absolute; inset:auto -40px -40px auto; width:140px; height:140px;
      background:radial-gradient(circle, rgba(13,110,253,.12), rgba(13,110,253,0));
    }
    .metric-value{ font-size:2.5rem; line-height:1; font-weight:800; letter-spacing:-.05em; }
    .metric-label{ margin-top:.55rem; font-size:.95rem; font-weight:700; }
    .metric-note{ margin-top:.35rem; color:var(--muted); font-size:.9rem; }
    .panel-card{ padding:20px; }
    .dash-table thead th{ position:static; background:transparent; color:var(--muted); font-size:.78rem; text-transform:uppercase; letter-spacing:.08em; border-bottom:1px solid #dbe4f0; }
    .dash-table tbody td{ border-color:#eef2f7; }
    .dash-table tbody tr:hover{ background:#f8fbff; }
    .panel-list{ list-style:none; padding:0; margin:0; display:flex; flex-direction:column; gap:10px; }
    .panel-list li{
      border:1px solid #eef2f7; border-radius:14px; padding:12px 14px; background:#fbfdff;
      display:flex; justify-content:space-between; align-items:flex-start; gap:10px;
    }
    .panel-list a{ color:var(--ink); text-decoration:none; font-weight:600; }
    .panel-list a:hover{ color:#0d6efd; }
    .panel-kicker{ color:var(--muted); font-size:.78rem; text-transform:uppercase; letter-spacing:.08em; }
    .page-section{ margin-top:24px; }
    .detail-panel{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); background:#fff; padding:1.5rem; display:none; }
    .detail-panel h6{ text-transform:uppercase; font-size:.85rem; letter-spacing:.08em; font-weight:600; }
    .detail-panel .subcard{ border:1px solid #e2e8f0; border-radius:12px; padding:1rem; background:#f8fafc; height:100%; }
    .detail-panel .subcard.active{ border-color:#0d6efd; box-shadow:0 0 0 3px rgba(13,110,253,.15); }
    @media (max-width:991.98px){
      .shell{ grid-template-columns:1fr; }
      .sidebar{ padding:18px 16px; }
      .main{ padding:18px 16px 88px; }
      .topbar{ flex-direction:column; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside class="sidebar">
      <div>
        <div class="brand-title">NTA MRP System</div>
      </div>
      <div class="nav-group">
        <div class="nav-label">Workspace</div>
        <a class="nav-link active" href="/"><span class="nav-dot"></span><span>Home</span></a>
        <a class="nav-link" href="/quotation_lookup"><span class="nav-dot"></span><span>Quotations</span></a>
        <a class="nav-link" href="/item_info"><span class="nav-dot"></span><span>Item Info</span></a>
        <a class="nav-link" href="/purchase_orders"><span class="nav-dot"></span><span>Purchase Orders</span></a>
      </div>
      <div class="sidebar-note">
        <p><strong>Quotations:</strong> For Inventory status lookup</p>
        <p><strong>Item Info:</strong> Item Details</p>
      </div>
    </aside>

    <main class="main">
      <div class="topbar">
        <div class="greeting-wrap">
          <div id="time-greeting" class="hero-title">Howdy</div>
          <img class="greeting-image" src="/static/snoopy-figure-transparent.png" alt="Snoopy dancing">
        </div>
        <div class="topbar-actions">
          <div class="loaded-badge">Loaded {{ loaded_at }}</div>
          <a class="btn btn-outline-secondary" href="/?reload=1">Reload</a>
        </div>
      </div>

      {% if not so_num and not customer_val %}
        <div class="card-lite search-card">
          <div class="search-title">Global Search</div>
          <form id="global-search-form" class="row g-3 align-items-end" method="get" action="/global_search">
            <div class="col-12 col-xl-10 global-search-wrap">
              <input id="global-search-input" autocomplete="off" class="form-control search-input" name="q" placeholder="SO, item, or customer" value="">
              <div id="global-search-suggest" class="list-group global-suggest"></div>
            </div>
            <div class="col-6 col-xl-1 d-grid">
              <button class="btn btn-primary" style="height:58px;font-weight:700;">Go</button>
            </div>
          </form>
        </div>

        <div class="row g-4">
          <div class="col-12 col-xl-4">
            <div class="card-lite metric-card">
              <div class="metric-value">{{ lt_unassigned_count or 0 }}</div>
              <div class="metric-label">Sales Order Not Assigned LT</div>
              <div class="metric-note">Placeholder ship date: 2099-12-31</div>
            </div>
          </div>
        </div>

        <div class="row g-4 page-section">
          <div class="col-12">
            <div class="card-lite panel-card h-100">
              <div class="section-title">Alerts</div>
              <ul class="panel-list">
                {% for item in alerts %}
                  <li>
                    <div>
                      <div class="panel-kicker">System</div>
                      {% if item.href %}
                        <a class="fw-semibold" href="{{ item.href }}">{{ item.label }}</a>
                      {% else %}
                        <div class="fw-semibold">{{ item.label }}</div>
                      {% endif %}
                    </div>
                  </li>
                {% endfor %}
              </ul>
            </div>
          </div>
        </div>
      {% endif %}

  {% if customer_query is not none %}
  <div class="card-lite bg-white my-4 p-4">
    <div class="d-flex justify-content-between flex-wrap gap-2 align-items-center mb-3">
      <div class="fw-bold">Customer search: "{{ customer_query }}"</div>
      <div class="text-muted small">{{ customer_options|length }} result(s)</div>
    </div>
    {% if customer_options %}
      <div class="list-group">
        {% for opt in customer_options %}
          <a class="list-group-item list-group-item-action d-flex justify-content-between align-items-center"
             href="/?so={{ opt.qb_num | urlencode }}">
            <div>
              <div class="fw-semibold">{{ opt.qb_num }}</div>
              <div class="text-muted small">{{ opt.name or "-" }}</div>
            </div>
            <div class="text-end text-muted small">
              {% if opt.ship_date %}<div>Ship: {{ opt.ship_date }}</div>{% endif %}
              {% if opt.order_date %}<div>Order: {{ opt.order_date }}</div>{% endif %}
            </div>
          </a>
        {% endfor %}
      </div>
      <div class="text-muted small mt-2">Select an SO to view its details.</div>
    {% else %}
      <div class="alert alert-warning mb-0">No SOs found for that customer.</div>
    {% endif %}
  </div>
  {% endif %}

  {% if order_summary %}
  <div class="summary-card bg-white mb-4 p-4">
    <div class="d-flex justify-content-between flex-wrap gap-2 mb-3">
      <div class="fw-bold">SO / QB: {{ order_summary.qb_num }}</div>
      <div class="text-muted small">Rows: {{ order_summary.row_count }}</div>
    </div>
    <div class="summary-grid">
      {% for field in order_summary.fields %}
        <div class="summary-field">
          <div class="summary-label">{{ field.label }}</div>
          <div class="summary-value">{{ field.value or "-" }}</div>
        </div>
      {% endfor %}
    </div>
    {% if order_summary.pdf_url %}
    <div class="mt-3">
      <a class="btn btn-sm btn-outline-primary" href="{{ order_summary.pdf_url }}" target="_blank">
        Open PDF{% if order_summary.pdf_name %} ({{ order_summary.pdf_name }}){% endif %}
      </a>
    </div>
    {% endif %}
  </div>
  {% endif %}

  {% if so_num and rows %}
  <div class="card-lite bg-white">
    <div class="card-header fw-bold">
      SO / QB: {{ so_num }} &nbsp; <span class="text-muted">Rows: {{ count }}</span>
    </div>
    <div class="card-body">
      <div class="table-responsive">
        <table class="table table-sm table-bordered table-hover align-middle">
          <thead class="table-light text-uppercase small text-muted">
            <tr>
              {% for h in headers %}
                <th class="{{ 'text-end' if h in numeric_cols else 'text-center' }}" title="{{ h }}">{{ header_labels.get(h, h) }}</th>
              {% endfor %}
            </tr>
          </thead>
          <tbody>
            {% for r in rows %}
              {% set _status = r.get('Component_Status') %}
              {% if _status == 'Available' %}
                {% set status_badge = 'badge-ok' %}
              {% elif _status == 'Waiting' %}
                {% set status_badge = 'badge-wait' %}
              {% else %}
                {% set status_badge = 'badge-warn' %}
              {% endif %}
              <tr>
              {% for h in headers %}
                {% if h == 'On Sales Order' %}
                  {% set item_val = r.get('Item','') %}
                  <td class="num clicky"><a href="#" class="detail-link" data-item="{{ item_val | e }}" data-focus="so">{{ r.get(h,'') }}</a></td>
                {% elif h == 'On PO' %}
                  {% set item_val = r.get('Item','') %}
                  <td class="num clicky"><a href="#" class="detail-link" data-item="{{ item_val | e }}" data-focus="po">{{ r.get(h,'') }}</a></td>
                {% elif h == 'On Hand - WIP' %}
                  <td class="num blue-cell">{{ r.get('On Hand - WIP', '') }}</td>
                {% elif h == 'Component_Status' %}
                  <td><span class="badge-pill {{ status_badge }}">{{ r.get(h,'') }}</span></td>
                {% elif h == 'Item' %}
                  {% set item_val = r.get('Item','') %}
                  <td class="nowrap clicky">
                    <a href="/item_details?item={{ item_val | urlencode }}">{{ item_val }}</a>
                  </td>
                {% elif h in numeric_cols %}
                  {% set v = r.get(h,'') %}
                  <td class="num-center {% if v is number and v < 0 %}neg{% elif v == 0 %}zero{% endif %}">{{ v }}</td>
                {% else %}
                  <td class="{{ 'nowrap' if h in ['Ship Date'] else '' }}">{{ r.get(h,'') }}</td>
                {% endif %}
              {% endfor %}
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
      <div class="mt-2 text-muted small">Tip: Click "On Sales Order" or "On PO" to drill down for more information</div>
      <div id="item-detail-panel" class="detail-panel"></div>
    </div>
  </div>
  {% elif so_num %}
  <div class="alert alert-warning mt-3">No rows found for "{{ so_num }}".</div>
  {% endif %}
    </main>
  </div>

  <script>
  (function () {
    var greeting = document.getElementById("time-greeting");
    if (!greeting) return;
    var hour = new Date().getHours();
    greeting.textContent = hour < 12
      ? "Good morning"
      : hour < 18
        ? "Good afternoon"
        : "Good evening";
  })();
  </script>
  <script>
  (function () {
    var input = document.getElementById('global-search-input');
    var list = document.getElementById('global-search-suggest');
    var timer;
    if (!input || !list) return;

    function esc(value) {
      return String(value || '').replace(/&/g, '&amp;').replace(/</g, '&lt;');
    }
    function hideList() {
      list.style.display = 'none';
      list.innerHTML = '';
    }
    function showList(items) {
      if (!items || !items.length) { hideList(); return; }
      list.innerHTML = items.map(function (item) {
        var customer = item.type === 'SO' && item.customer
          ? '<span class="global-suggest-customer">' + esc(item.customer) + '</span>'
          : '';
        var shipDate = item.type === 'SO' && item.ship_date
          ? '<span class="global-suggest-ship-date">Ship: ' + esc(item.ship_date) + '</span>'
          : '';
        var onHand = item.type === 'Item' && item.on_hand !== '' && item.on_hand != null
          ? '<span class="global-suggest-on-hand">On Hand: ' + esc(item.on_hand) + '</span>'
          : '';
        return '<a class="list-group-item list-group-item-action" href="' + esc(item.href) + '">' +
          '<span class="global-suggest-row"><span>' + esc(item.label) + '</span>' + customer +
          '<span class="global-suggest-meta">' + shipDate + onHand +
          '<span class="global-suggest-type">' + esc(item.type) + '</span></span></span></a>';
      }).join('');
      list.style.display = 'block';
    }

    input.addEventListener('input', function () {
      var q = input.value.trim();
      if (timer) clearTimeout(timer);
      if (!q) { hideList(); return; }
      timer = setTimeout(function () {
        fetch('/api/global_suggest?q=' + encodeURIComponent(q))
          .then(function (r) { return r.json(); })
          .then(function (j) { if (j && j.ok) showList(j.items); else hideList(); })
          .catch(function () { hideList(); });
      }, 200);
    });

    document.addEventListener('click', function (event) {
      if (!event.target.closest || (!event.target.closest('#global-search-suggest') && !event.target.closest('#global-search-input'))) hideList();
    });
  })();
  </script>
  <script>
  (function () {
    var panel = document.getElementById('item-detail-panel');
    if (!panel) return;

    var cache = {};

    function escapeHtml(value) {
      if (value === null || value === undefined) return '';
      return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function buildTable(columns, rows) {
      var safeCols = Array.isArray(columns) ? columns : [];
      var head = safeCols.map(function (c) { return '<th>' + escapeHtml(c) + '</th>'; }).join('');
      var body = '';
      if (Array.isArray(rows) && rows.length) {
        body = rows.map(function (row) {
          return '<tr>' + safeCols.map(function (col) {
            return '<td>' + escapeHtml(row[col]) + '</td>';
          }).join('') + '</tr>';
        }).join('');
      } else {
        body = '<tr><td colspan="' + (safeCols.length || 1) + '" class="text-center text-muted">No data</td></tr>';
      }
      return [
        '<div class="table-responsive mt-2">',
          '<table class="table table-sm table-bordered table-hover align-middle">',
            '<thead class="table-light text-uppercase small text-muted"><tr>' + head + '</tr></thead>',
            '<tbody>' + body + '</tbody>',
          '</table>',
        '</div>'
      ].join('');
    }

    function buildCard(opts) {
      opts = opts || {};
      var title = opts.title || '';
      var columns = opts.columns || [];
      var rows = Array.isArray(opts.rows) ? opts.rows : [];
      var total = opts.totalText;
      var note = opts.note;
      var active = opts.active ? ' active' : '';
      return [
        '<div class="subcard' + active + '">',
          '<div class="d-flex justify-content-between align-items-center">',
            '<h6 class="m-0">' + escapeHtml(title) + '</h6>',
            '<div class="text-muted small">' + rows.length + ' rows</div>',
          '</div>',
          (total ? '<div class="small fw-semibold text-primary mt-1">' + escapeHtml(total) + '</div>' : ''),
          buildTable(columns, rows),
          (note ? '<div class="text-muted small mt-2">' + escapeHtml(note) + '</div>' : ''),
        '</div>'
      ].join('');
    }

    function renderDetail(data, focus) {
      data = data || {};
      var so = data.so || {};
      var po = data.po || {};
      var itemLabel = data.item || '';
      var onPoLabel = (data.on_po_label !== null && data.on_po_label !== undefined) ? data.on_po_label : '—';

      var card;
      if (focus === 'po') {
        card = {
          title: 'On PO',
          data: po,
          total: so.total_on_po !== null && so.total_on_po !== undefined ? 'On PO (SO_INV): ' + so.total_on_po : null,
          note: 'Source: Taipei SAP',
        };
      } else {
        card = {
          title: 'On Sales Order',
          data: so,
          total: so.total_on_sales !== null && so.total_on_sales !== undefined ? 'On Sales Order: ' + so.total_on_sales : null,
          note: 'Source: NTA Quickbooks',
        };
      }

      var openSection = '';
      if (focus === 'po') {
        var openData = data.open_po || {};
        var openRows = Array.isArray(openData.rows) ? openData.rows : [];
        var openColumns = Array.isArray(openData.columns) ? openData.columns : [];
        openSection = '<hr class="my-3">';
        if (openRows.length) {
          openSection += '<div class="fw-bold small text-muted text-uppercase">Open Purchase Orders</div>' +
            buildTable(openColumns, openRows) +
            '<div class="text-muted small">Source: NTA Quickbooks</div>';
        } else {
          openSection += '<div class="text-muted small">No open purchase orders</div>';
        }
      }

      panel.innerHTML = [
        '<div class="d-flex justify-content-between flex-wrap gap-2 mb-3">',
          '<div>',
            '<h5 class="mb-1">Item — ' + escapeHtml(itemLabel) + '</h5>',
            '<div class="text-muted small">On PO (from SO data): ' + escapeHtml(onPoLabel) + '</div>',
          '</div>',
          '<div class="text-muted small">Data pulled live from cached tables.</div>',
        '</div>',
        '<div class="subcard active">',
          '<div class="d-flex justify-content-between align-items-center">',
            '<h6 class="m-0">' + escapeHtml(card.title) + '</h6>',
            '<div class="text-muted small">' + ((card.data.rows || []).length) + ' rows</div>',
          '</div>',
          (card.total ? '<div class="small fw-semibold text-primary mt-1">' + escapeHtml(card.total) + '</div>' : ''),
          buildTable(card.data.columns, card.data.rows),
          (card.note ? '<div class="text-muted small mt-2">' + escapeHtml(card.note) + '</div>' : ''),
          openSection,
        '</div>'
      ].join('');
      panel.style.display = 'block';
      panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    document.addEventListener('click', function (event) {
      if (!event.target.closest) return;
      var link = event.target.closest('.detail-link');
      if (!link) return;
      event.preventDefault();

      var item = link.getAttribute('data-item') || '';
      if (!item) return;
      var focus = link.getAttribute('data-focus') || 'so';

      if (cache[item]) {
        renderDetail(cache[item], focus);
        return;
      }

      panel.style.display = 'block';
      panel.innerHTML = '<div class="text-muted small">Loading ' + escapeHtml(item) + '…</div>';

      fetch('/api/item_overview?item=' + encodeURIComponent(item))
        .then(function (resp) {
          if (!resp.ok) throw new Error('Server error (' + resp.status + ')');
          return resp.json();
        })
        .then(function (json) {
          if (!json.ok) throw new Error(json.error || 'Failed to load item');
          cache[item] = json;
          renderDetail(json, focus);
        })
        .catch(function (err) {
          panel.innerHTML = '<div class="alert alert-danger mb-0">Error loading ' +
            escapeHtml(item) + ': ' + escapeHtml(err.message) + '</div>';
          panel.style.display = 'block';
        });
    });
  })();
  </script>


</body>
</html>
"""

ITEM_INFO_TPL = """
<!doctype html>
<html lang="en">
<head>
  <link rel="icon" href="/static/robot.svg?v=1" type="image/svg+xml">
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Item Info</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{ --ink:#0f172a; --muted:#6b7280; --bg:#f7fafc; --hdr:#f8fafc; }
    html,body{ background:var(--bg); color:var(--ink); }
    body{ padding:28px; }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); }
    .table-responsive{ max-height:72vh; overflow:auto; }
    .table thead th{ position:sticky; top:0; z-index:2; background:var(--hdr); white-space:nowrap; }
    .table td{ white-space:nowrap; }
    .item-info-suggest{ position:absolute; top:62px; left:0; right:0; z-index:1000; display:none; max-height:280px; overflow:auto; }
    .suggest-item-row{ display:flex; align-items:center; justify-content:space-between; gap:.75rem; }
    .photo-mark{ flex:0 0 auto; font-size:.72rem; font-weight:700; color:#166534; background:#dcfce7; border:1px solid #bbf7d0; border-radius:999px; padding:.12rem .45rem; }
  </style>
  <link rel="stylesheet" href="{{ url_for('static', filename='workspace.css') }}?v=2">
</head>
<body class="workspace-page">
  <a class="workspace-skip" href="#workspace-content">Skip to content</a>
  <nav class="workspace-nav" aria-label="Main navigation"><div class="workspace-nav-inner">
    <a class="workspace-brand" href="/" aria-label="Neousys home"><span class="workspace-brand-mark"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="icon icon-tabler icons-tabler-outline icon-tabler-robot" aria-hidden="true" focusable="false"><path stroke="none" d="M0 0h24v24H0z" fill="none" /><path d="M6 6a2 2 0 0 1 2 -2h8a2 2 0 0 1 2 2v4a2 2 0 0 1 -2 2h-8a2 2 0 0 1 -2 -2l0 -4" /><path d="M12 2v2" /><path d="M9 12v9" /><path d="M15 12v9" /><path d="M5 16l4 -2" /><path d="M15 14l4 2" /><path d="M9 18h6" /><path d="M10 8v.01" /><path d="M14 8v.01" /></svg></span><span><strong>NEOUSYS</strong><small>MANUFACTURING WORKSPACE</small></span></a>
<a class="workspace-home" href="/">Home</a></div></nav>
  <main id="workspace-content" class="workspace-main">
  <div class="workspace-breadcrumb"><a href="/">Workspace</a><span aria-hidden="true">/</span><span>Item Info</span></div>
  <header class="workspace-header"><div><h1 class="workspace-title">Item info</h1><p class="workspace-description">Find part details, vendor information, and product photos.</p></div>
    <div class="workspace-header-actions"><span class="workspace-loaded">Loaded {{ loaded_at }}</span><a class="btn btn-sm btn-outline-secondary" href="/item_info?reload=1">Reload</a></div>
  </header>

  <form class="workspace-search row gy-3 gx-4 align-items-end justify-content-start mb-4" method="get">
    <div class="col-12 col-md-7">
      <label class="form-label" for="item-info-q">Search Item</label>
      <div style="position:relative;">
        <input id="item-info-q" autocomplete="off" class="form-control form-control-lg"
               
               name="q"
               placeholder="Type item name, part name, MPN, vendor, or description"
               value="{{ q_val or '' }}">
        <div id="item-info-suggest" class="list-group item-info-suggest"></div>
      </div>
    </div>
    <div class="col-6 col-md-auto">
      <button class="btn btn-primary px-4 w-100" >Search</button>
    </div>
  </form>

  <div class="card-lite bg-white">
    <div class="card-header fw-bold d-flex justify-content-between align-items-center">
      <span>Results</span>
      <span class="text-muted small">
        {% if q_val %}
          Showing {{ shown }} of {{ count }} matches
        {% else %}
          Enter a search term
        {% endif %}
      </span>
    </div>
    <div class="card-body">
      <div class="table-responsive">
        <table class="table table-sm table-bordered table-hover align-middle">
          <thead class="table-light text-uppercase small text-muted">
            <tr>
              {% for c in columns %}
                <th>{{ c }}</th>
              {% endfor %}
            </tr>
          </thead>
          <tbody>
            {% if rows %}
              {% for row in rows %}
                <tr>
                  {% for c in columns %}
                    <td>
                      {% if c == 'Photo File' and row[c] %}
                        <a href="{{ row['_photo_href'] }}" target="_blank" rel="noopener">{{ row[c] }}</a>
                      {% else %}
                        {{ row[c] }}
                      {% endif %}
                    </td>
                  {% endfor %}
                </tr>
              {% endfor %}
            {% else %}
              <tr><td colspan="{{ columns|length or 1 }}" class="text-center text-muted">
                {% if q_val %}No item info rows found.{% else %}Search to show item info rows.{% endif %}
              </td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
      <div class="text-muted small">Source: public.Item Info</div>
    </div>
  </div>

  </main>

  <script>
  (function () {
    var input = document.getElementById('item-info-q');
    var list = document.getElementById('item-info-suggest');
    var suggestTimer;

    function esc(value) {
      return String(value || '').replace(/&/g, '&amp;').replace(/</g, '&lt;');
    }

    function hideList() {
      if (!list) return;
      list.style.display = 'none';
      list.innerHTML = '';
    }

    function showList(items) {
      if (!items || !items.length) { hideList(); return; }
      list.innerHTML = items.map(function (item) {
        var text = typeof item === 'string' ? item : item.text;
        var photoMark = item && item.has_photo ? '<span class="photo-mark">PHOTO</span>' : '';
        return '<button type="button" class="list-group-item list-group-item-action" data-value="' + esc(text) + '">' +
               '<span class="suggest-item-row"><span>' + esc(text) + '</span>' + photoMark + '</span>' +
               '</button>';
      }).join('');
      list.style.display = 'block';
    }

    if (input && list) {
      input.addEventListener('input', function () {
        var q = input.value.trim();
        if (suggestTimer) clearTimeout(suggestTimer);
        if (!q) { hideList(); return; }
        suggestTimer = setTimeout(function () {
          fetch('/api/item_info_suggest?q=' + encodeURIComponent(q))
            .then(function (r) { return r.json(); })
            .then(function (j) { if (j && j.ok) showList(j.items); else hideList(); })
            .catch(function () { hideList(); });
        }, 180);
      });

      list.addEventListener('click', function (e) {
        var target = e.target.closest('.list-group-item');
        if (!target) return;
        input.value = target.getAttribute('data-value') || target.textContent.trim();
        hideList();
      });

      document.addEventListener('click', function (e) {
        if (!e.target.closest || (!e.target.closest('#item-info-suggest') && !e.target.closest('#item-info-q'))) hideList();
      });
    }
  })();
  </script>
</body>
</html>
"""


SUBPAGE_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/robot.svg?v=1" type="image/svg+xml">
  <meta charset="utf-8">
  <title>{{ title }}</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{ --hdr:#f8fafc; }
    body{ padding:28px; background:#f7fafc; }
    .table td, .table th{ vertical-align:middle; }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); }
    .pill{ display:inline-block; padding:.2rem .65rem; border-radius:999px; background:#eef4ff; border:1px solid #d9e4ff; font-weight:600; margin-left:.5rem; }
    .table-responsive{ max-height:70vh; overflow:auto; }
    .table thead th{ position:sticky; top:0; z-index:2; background:var(--hdr); }
  </style>
</head>
<body>
  <div class="d-flex justify-content-between align-items-center mb-3">
    <h5 class="m-0">{{ title }}</h5>
    <a class="btn btn-sm btn-outline-secondary" href="/">Back</a>
  </div>

  <div class="card-lite bg-white">
    <div class="card-header fw-bold d-flex align-items-center justify-content-between">
      <span>{{ title }}</span>
      {% if on_po is not none %}
        <span class="pill">On PO: {{ on_po }}</span>
      {% endif %}
    </div>
    <div class="card-body">
      <div class="table-responsive">
        <table class="table table-sm table-bordered table-hover align-middle">
          <thead class="table-light text-uppercase small text-muted">
            <tr>
              {% for c in columns %}
                <th>{{ c }}</th>
              {% endfor %}
            </tr>
          </thead>
          <tbody>
            {% if rows %}
              {% for r in rows %}
                <tr>
                  {% for c in columns %}
                    <td>{{ r[c] }}</td>
                  {% endfor %}
                </tr>
              {% endfor %}
            {% else %}
              <tr><td colspan="{{ columns|length }}" class="text-center text-muted">No data</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
      <div class="text-muted small">{{ extra_note }}</div>
      {% if open_po_rows %}
      <hr class="my-4">
      <div class="fw-bold small text-muted text-uppercase">Open Purchase Orders</div>
      <div class="table-responsive mt-2">
        <table class="table table-sm table-bordered table-hover align-middle">
          <thead class="table-light text-uppercase small text-muted">
            <tr>
              {% for c in open_po_columns %}
                <th>{{ c }}</th>
              {% endfor %}
            </tr>
          </thead>
          <tbody>
            {% for r in open_po_rows %}
              <tr>
              {% for c in open_po_columns %}
                <td>{{ r[c] }}</td>
              {% endfor %}
              </tr>
            {% else %}
              <tr><td colspan="{{ open_po_columns|length }}" class="text-center text-muted">No open purchase orders</td></tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
      <div class="text-muted small">{{ extra_note_open_po }}</div>
      {% endif %}
    </div>
  </div>
</body>
</html>
"""

ITEM_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/robot.svg?v=1" type="image/svg+xml">
  <meta charset="utf-8">
  <title>Item Detail — {{ item }}</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{ --hdr:#f8fafc; }
    body{ padding:28px; background:#f7fafc; color:#0f172a; }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); }
    .table td, .table th{ vertical-align:middle; }
    .table-responsive{ max-height:70vh; overflow:auto; }
    .table thead th{ position:sticky; top:0; z-index:2; background:var(--hdr); }
    .pill{ display:inline-block; padding:.2rem .65rem; border-radius:999px; background:#eef4ff; border:1px solid #d9e4ff; font-weight:600; margin-left:.5rem; }
    .muted{ color:#64748b; }
  </style>
</head>
<body>
  <div class="d-flex justify-content-between align-items-start mb-3 flex-wrap gap-3">
    <div>
      <h5 class="m-0">Item Detail — {{ item }}</h5>
      {% if on_po is not none %}
        <div class="text-muted small mt-1">On PO (from SO data): {{ on_po }}</div>
      {% endif %}
    </div>
    <div class="d-flex gap-2">
      <a class="btn btn-sm btn-outline-secondary" href="/">Back</a>
      <a class="btn btn-sm btn-outline-primary" href="/?item={{ item | urlencode }}">Search Again</a>
    </div>
  </div>

  <div class="row g-4">
    <div class="col-12 col-lg-6">
      <div class="card-lite bg-white h-100">
        <div class="card-header fw-bold d-flex justify-content-between align-items-center">
          <span>On Sales Order</span>
          <div class="text-end">
            <div class="text-muted small">{{ so_rows|length }} rows</div>
            {% if so_total_on_sales is not none %}
              <div class="small fw-semibold text-primary">On Sales Order: {{ so_total_on_sales }}</div>
            {% endif %}
          </div>
        </div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-bordered table-hover align-middle">
              <thead class="table-light text-uppercase small text-muted">
                <tr>
                  {% for c in so_columns %}
                    <th>{{ c }}</th>
                  {% endfor %}
                </tr>
              </thead>
              <tbody>
                {% if so_rows %}
                  {% for row in so_rows %}
                    <tr>
                      {% for c in so_columns %}
                        <td>{{ row[c] }}</td>
                      {% endfor %}
                    </tr>
                  {% endfor %}
                {% else %}
                  <tr><td colspan="{{ so_columns|length }}" class="text-center text-muted">No sales order rows for this item.</td></tr>
                {% endif %}
              </tbody>
            </table>
          </div>
          <div class="text-muted small">{{ extra_note_so }}</div>
        </div>
      </div>
    </div>

    <div class="col-12 col-lg-6">
      <div class="card-lite bg-white h-100">
        <div class="card-header fw-bold d-flex justify-content-between align-items-center">
          <span>On PO</span>
          <div class="text-end">
            <div class="text-muted small">{{ po_rows|length }} rows</div>
            {% if so_total_on_po is not none %}
              <div class="small fw-semibold text-primary">On PO (SO_INV): {{ so_total_on_po }}</div>
            {% endif %}
          </div>
        </div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-bordered table-hover align-middle">
              <thead class="table-light text-uppercase small text-muted">
                <tr>
                  {% for c in po_columns %}
                    <th>{{ c }}</th>
                  {% endfor %}
                </tr>
              </thead>
              <tbody>
                {% if po_rows %}
                  {% for row in po_rows %}
                    <tr>
                      {% for c in po_columns %}
                        <td>{{ row[c] }}</td>
                      {% endfor %}
                    </tr>
                  {% endfor %}
                {% else %}
                  <tr><td colspan="{{ po_columns|length }}" class="text-center text-muted">No PO rows for this item.</td></tr>
                {% endif %}
              </tbody>
            </table>
          </div>
          <div class="text-muted small">{{ extra_note_po }}</div>
          {% if open_po_rows %}
          <hr class="my-3">
          <div class="fw-bold small text-muted text-uppercase">Open Purchase Orders</div>
          <div class="table-responsive mt-2">
            <table class="table table-sm table-bordered table-hover align-middle">
              <thead class="table-light text-uppercase small text-muted">
                <tr>
                  {% for c in open_po_columns %}
                    <th>{{ c }}</th>
                  {% endfor %}
                </tr>
              </thead>
              <tbody>
                {% if open_po_rows %}
                  {% for row in open_po_rows %}
                    <tr>
                      {% for c in open_po_columns %}
                        <td>{{ row[c] }}</td>
                      {% endfor %}
                    </tr>
                  {% endfor %}
                {% else %}
                  <tr><td colspan="{{ open_po_columns|length }}" class="text-center text-muted">No open purchase orders</td></tr>
                {% endif %}
              </tbody>
            </table>
          </div>
          <div class="text-muted small">{{ extra_note_open_po }}</div>
          {% endif %}
        </div>
      </div>
    </div>
  </div>
</body>
</html>
"""

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

PERIPHERAL_STATUS_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/robot.svg?v=1" type="image/svg+xml">
  <meta charset="utf-8">
  <title>SSD &amp; Memory Status</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{--ink:#0f172a;--muted:#64748b;--bg:#f7fafc;--hdr:#f8fafc;}
    html,body{background:var(--bg);color:var(--ink)} body{padding:28px}
    .card-lite{border-radius:14px;box-shadow:0 10px 22px rgba(0,0,0,.06)}
    .table-wrap{max-height:72vh;overflow:auto;border:1px solid #e2e8f0;border-radius:10px}
    table{white-space:nowrap;margin-bottom:0!important}
    .table thead th{position:sticky;top:0;z-index:3;background:var(--hdr);vertical-align:bottom}
    .model-recommended{background:#fff2a8!important;font-weight:700}
    .model-do-not-use{background:#f8b4b4!important;font-weight:700}
    .legend-swatch{display:inline-block;width:.85rem;height:.85rem;border:1px solid #cbd5e1;border-radius:3px;margin-right:.35rem;vertical-align:-.08rem}
    .swatch-yellow{background:#fff2a8}.swatch-red{background:#f8b4b4}
    .sheet-pane[hidden]{display:none!important}.muted{color:var(--muted)}
  </style>
</head>
<body>
  <div class="d-flex justify-content-between align-items-start gap-3 mb-3">
    <div>
      <h1 class="h3 mb-1">SSD &amp; Memory Status</h1>
      <div class="small muted">Read-only view of the SSD and DDR workbook sheets{% if loaded_at %} · Loaded {{ loaded_at }}{% endif %}</div>
      {% if workbook_name %}<div class="small muted">Source: {{ workbook_name }}</div>{% endif %}
    </div>
    <div class="d-flex gap-2">
      <a class="btn btn-sm btn-outline-secondary" href="/quotation_lookup">Quotation Lookup</a>
      <a class="btn btn-sm btn-outline-primary" href="/quotation_lookup/peripheral_status?reload=1">Reload workbook</a>
      <a class="btn btn-sm btn-outline-secondary" href="/">Home</a>
    </div>
  </div>

  {% if error %}
    <div class="alert alert-warning"><strong>Workbook unavailable.</strong> {{ error }}</div>
  {% else %}
    {% if warnings %}<div class="alert alert-warning py-2">{{ warnings|join(' ') }}</div>{% endif %}
    <div class="card-lite bg-white p-3">
      <div class="d-flex flex-wrap align-items-end gap-3 mb-3">
        <div>
          <label for="sheet-select" class="form-label small fw-semibold mb-1">Sheet</label>
          <select id="sheet-select" class="form-select">
            {% for sheet in sheets %}<option value="sheet-{{ loop.index0 }}">{{ sheet.label }} ({{ sheet.rows|length }})</option>{% endfor %}
          </select>
        </div>
        <div class="flex-grow-1" style="min-width:260px">
          <label for="table-search" class="form-label small fw-semibold mb-1">Search this sheet</label>
          <input id="table-search" class="form-control" type="search" placeholder="Search model name, status, or any displayed value">
        </div>
        <div class="small muted pb-2">
          <span class="me-3"><span class="legend-swatch swatch-yellow"></span>Recommended to use</span>
          <span><span class="legend-swatch swatch-red"></span>Do not use</span>
        </div>
      </div>

      {% for sheet in sheets %}
      <section id="sheet-{{ loop.index0 }}" class="sheet-pane" {% if not loop.first %}hidden{% endif %}>
        <div class="small muted mb-2"><span class="visible-count">{{ sheet.rows|length }}</span> of {{ sheet.rows|length }} rows shown</div>
        <div class="table-wrap">
          <table class="table table-sm table-bordered table-hover align-middle">
            <thead><tr>{% for header in sheet.headers %}<th>{{ header }}</th>{% endfor %}</tr></thead>
            <tbody>
              {% for row in sheet.rows %}
              <tr data-search="{{ row.search_text }}">
                {% for cell in row.cells %}<td{% if loop.index0 == row.model_display_index and row.model_class %} class="{{ row.model_class }}"{% endif %}>{{ cell }}</td>{% endfor %}
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </section>
      {% endfor %}
    </div>
  {% endif %}

  <script>
  (function(){
    var select=document.getElementById('sheet-select'), search=document.getElementById('table-search');
    if(!select||!search) return;
    function active(){return document.getElementById(select.value)}
    function filter(){
      var pane=active(), q=search.value.trim().toLowerCase(), shown=0;
      pane.querySelectorAll('tbody tr').forEach(function(row){
        var visible=!q || (row.getAttribute('data-search')||'').indexOf(q)!==-1;
        row.hidden=!visible; if(visible) shown++;
      });
      pane.querySelector('.visible-count').textContent=shown;
    }
    select.addEventListener('change',function(){
      document.querySelectorAll('.sheet-pane').forEach(function(p){p.hidden=p.id!==select.value}); filter();
    });
    search.addEventListener('input',filter);
  })();
  </script>
</body>
</html>
"""
