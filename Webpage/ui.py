# Webpage/ui.py
ERR_TPL = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Data Error</title>
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
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
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
  <meta charset="utf-8">
  <title>LT Check — DB</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{
      --ink:#0f172a; --muted:#6b7280; --bg:#f7fafc;
      --ok-bg:#e7f8ed; --ok-fg:#137a2a;
      --warn-bg:#ffecec; --warn-fg:#a61b1b;
      --wait-bg:#fff3cd; --wait-fg:#664d03;
      --hdr:#f8fafc;
    }
    html,body{ background:var(--bg); color:var(--ink); }
    body{ padding:28px; }
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
    /* Inventory Count CTA card */
    .inv-cta{ border-radius:16px; background:#0d6efd; color:#fff; padding:1.25rem 1.5rem; display:flex; align-items:center; justify-content:space-between; gap:1rem; }
    .inv-cta .title{ font-weight:700; letter-spacing:.02em; }
    .inv-cta .sub{ opacity:.9 }
    .inv-cta .btn{ background:#fff; color:#0d6efd; font-weight:700; border:none }
    .detail-panel{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); background:#fff; padding:1.5rem; display:none; }
    .detail-panel h6{ text-transform:uppercase; font-size:.85rem; letter-spacing:.08em; font-weight:600; }
    .detail-panel .subcard{ border:1px solid #e2e8f0; border-radius:12px; padding:1rem; background:#f8fafc; height:100%; }
    .detail-panel .subcard.active{ border-color:#0d6efd; box-shadow:0 0 0 3px rgba(13,110,253,.15); }
    .chat-toggle{
      position:fixed; right:20px; bottom:20px; z-index:1100;
      border:none; border-radius:999px; background:#0d6efd; color:#fff;
      padding:.75rem 1.1rem; font-weight:700; box-shadow:0 10px 24px rgba(13,110,253,.35);
    }
    .chatbox{
      position:fixed; right:20px; bottom:78px; z-index:1100;
      width:min(380px, calc(100vw - 24px)); border-radius:14px;
      background:#fff; border:1px solid #dbe4f0; box-shadow:0 18px 40px rgba(2,6,23,.2);
      display:none; overflow:hidden;
    }
    .chatbox.open{ display:block; }
    .chatbox-head{
      background:#0d6efd; color:#fff; padding:.7rem .9rem;
      display:flex; justify-content:space-between; align-items:center;
      font-weight:700;
    }
    .chatbox-close{ background:transparent; border:none; color:#fff; font-size:1.1rem; line-height:1; }
    .chatbox-body{ max-height:340px; min-height:220px; overflow:auto; background:#f8fbff; padding:.8rem; }
    .chat-msg{ margin-bottom:.6rem; display:flex; }
    .chat-msg.user{ justify-content:flex-end; }
    .chat-bubble{
      max-width:84%; border-radius:12px; padding:.5rem .65rem; font-size:.92rem; white-space:pre-wrap;
      border:1px solid #dbe4f0; background:#fff; color:#0f172a;
    }
    .chat-msg.user .chat-bubble{
      background:#0d6efd; color:#fff; border-color:#0d6efd;
    }
    .chatbox-form{ border-top:1px solid #e2e8f0; padding:.7rem; display:flex; gap:.5rem; background:#fff; }
    .chatbox-input{ flex:1; min-width:0; }
    .chatbox-hint{ padding:0 .75rem .65rem; color:#6b7280; font-size:.77rem; background:#fff; }
    @media (max-width:576px){
      .chat-toggle{ right:10px; bottom:10px; }
      .chatbox{ right:10px; bottom:64px; width:calc(100vw - 20px); }
    }
  </style>
</head>
<body>
  <div class="page-title">LT Check</div>
  <div class="page-sub">Loaded {{ loaded_at }}</div>

  <form class="row gy-3 gx-4 align-items-end justify-content-center mb-5" method="get">
    <div class="col-12 col-md-4 form-section">
      <label for="search-so">By SO</label>
      <input id="search-so" class="form-control form-control-lg" style="height:60px;font-size:1.05rem"
             name="so" placeholder="SO-20251368 or 20251368" value="{{ so_num or '' }}">
    </div>
    <div class="col-12 col-md-4 form-section">
      <label for="search-customer">By Customer</label>
      <input id="search-customer" class="form-control form-control-lg" style="height:60px;font-size:1.05rem"
             name="customer" placeholder="Customer name" value="{{ customer_val or '' }}">
    </div>
    <div class="col-6 col-md-auto text-center">
      <button class="btn btn-primary px-4 w-100" style="height:52px;font-size:1rem;font-weight:600">Search</button>
    </div>
    <div class="col-6 col-md-auto text-center">
      <a class="btn btn-outline-secondary w-100" style="height:52px;font-size:1rem;font-weight:600" href="/?reload=1">Reload</a>
    </div>
    <div class="col-12 d-flex justify-content-between align-items-center">
      <div class="text-muted small">Tip: Search by a specific SO/QB number or enter a customer name to list their SOs.</div>
      <div class="d-flex gap-2"></div>
    </div>
  </form>

  <div class="inv-cta mt-2">
    <div>
      <div class="title">Inventory Count</div>
      <div class="sub">A separate module for quick item stock snapshots</div>
    </div>
    <a class="btn btn-lg" href="/inventory_count">Open</a>
  </div>

  <div class="inv-cta mt-3" style="background:#059669;">
    <div>
      <div class="title">Production Planning</div>
      <div class="sub">Calendar-style view of final sales orders by Lead Time</div>
    </div>
    <a class="btn btn-lg" href="/production_planning">Open</a>
  </div>

  <div class="inv-cta mt-3" style="background:#dc2626;">
    <div>
      <div class="title">Quotation Lookup</div>
      <div class="sub">Ledger + earliest ATP date by item</div>
    </div>
    <a class="btn btn-lg" href="/quotation_lookup">Open</a>
  </div>

  <div class="inv-cta mt-3" style="background:#1f4ed8;">
    <div>
      <div class="title">Phase 1 Board</div>
      <div class="sub">Read-only training view: Demand Queue, Supply Pool, Coverage</div>
    </div>
    <a class="btn btn-lg" href="/phase1">Open</a>
  </div>

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

  <button id="erp-chat-toggle" class="chat-toggle" type="button">Chat</button>
  <div id="erp-chatbox" class="chatbox" aria-hidden="true">
    <div class="chatbox-head">
      <span>ERP Assistant</span>
      <button id="erp-chat-close" class="chatbox-close" type="button" aria-label="Close">x</button>
    </div>
    <div id="erp-chat-body" class="chatbox-body">
      <div class="chat-msg">
        <div class="chat-bubble">Ask me inventory, ATP, ATP date, or SO waiting questions.</div>
      </div>
    </div>
    <form id="erp-chat-form" class="chatbox-form">
      <input id="erp-chat-input" class="form-control form-control-sm chatbox-input" type="text" maxlength="500" placeholder="Type your question...">
      <button id="erp-chat-send" class="btn btn-primary btn-sm" type="submit">Send</button>
    </form>
    <div class="chatbox-hint">Uses your existing LLM parser and DB tools.</div>
  </div>
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
  <script>
  (function () {
    var toggle = document.getElementById("erp-chat-toggle");
    var box = document.getElementById("erp-chatbox");
    var closeBtn = document.getElementById("erp-chat-close");
    var form = document.getElementById("erp-chat-form");
    var input = document.getElementById("erp-chat-input");
    var sendBtn = document.getElementById("erp-chat-send");
    var body = document.getElementById("erp-chat-body");
    if (!toggle || !box || !closeBtn || !form || !input || !sendBtn || !body) return;

    function setOpen(open) {
      box.classList.toggle("open", !!open);
      box.setAttribute("aria-hidden", open ? "false" : "true");
      if (open) input.focus();
    }

    function pushMsg(role, text) {
      var row = document.createElement("div");
      row.className = "chat-msg" + (role === "user" ? " user" : "");
      var bubble = document.createElement("div");
      bubble.className = "chat-bubble";
      bubble.textContent = text || "";
      row.appendChild(bubble);
      body.appendChild(row);
      body.scrollTop = body.scrollHeight;
      return bubble;
    }

    toggle.addEventListener("click", function () {
      setOpen(!box.classList.contains("open"));
    });
    closeBtn.addEventListener("click", function () {
      setOpen(false);
    });

    form.addEventListener("submit", function (event) {
      event.preventDefault();
      var msg = input.value.trim();
      if (!msg) return;

      pushMsg("user", msg);
      input.value = "";
      input.disabled = true;
      sendBtn.disabled = true;
      var pending = pushMsg("assistant", "Thinking...");

      fetch("/api/llm_chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: msg })
      })
        .then(function (resp) {
          return resp.json().then(function (json) {
            return { status: resp.status, ok: resp.ok, json: json };
          });
        })
        .then(function (res) {
          var data = res.json || {};
          if (!res.ok) throw new Error(data.answer || data.error || ("Server error (" + res.status + ")"));
          pending.textContent = data.answer || "No answer.";
        })
        .catch(function (err) {
          pending.textContent = "Error: " + err.message;
        })
        .finally(function () {
          input.disabled = false;
          sendBtn.disabled = false;
          input.focus();
        });
    });
  })();
  </script>

</body>
</html>
"""

PHASE1_TPL = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Phase 1 - Read-only Allocation Board</title>
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=IBM+Plex+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root{
      --bg-1:#f4f8ff; --bg-2:#f3fff6; --ink:#10213a; --muted:#5a6b85;
      --card:#ffffff; --line:#dbe4f2; --accent:#1769ff; --warn:#b42318;
    }
    body{
      font-family:"Space Grotesk",sans-serif;
      color:var(--ink);
      background:radial-gradient(1200px 500px at 5% -10%, #e0edff 0%, transparent 55%),
                 radial-gradient(1100px 500px at 95% -15%, #e5ffef 0%, transparent 50%),
                 linear-gradient(180deg,var(--bg-1),var(--bg-2));
      min-height:100vh;
    }
    .wrap{ padding:24px; max-width:1500px; margin:0 auto; }
    .title{ font-size:2rem; font-weight:700; letter-spacing:.02em; }
    .sub{ color:var(--muted); }
    .badge-ro{ background:#fff1cc; color:#6f4f00; font-weight:700; border-radius:999px; padding:.25rem .7rem; }
    .card-lite{
      border:1px solid var(--line);
      border-radius:16px;
      background:var(--card);
      box-shadow:0 14px 30px rgba(16,33,58,.08);
    }
    .panel-split{
      border:1px solid var(--line);
      border-radius:14px;
      background:#fff;
      padding:1rem;
      height:100%;
    }
    .panel-title{
      font-size:1rem;
      font-weight:700;
      letter-spacing:.02em;
      margin-bottom:.45rem;
    }
    .table-wrap{ max-height:55vh; overflow:auto; border-radius:12px; border:1px solid var(--line); }
    .table{ margin:0; }
    .table thead th{
      position:sticky; top:0; z-index:2; background:#f8fbff; white-space:nowrap;
      text-transform:uppercase; font-size:.75rem; letter-spacing:.06em; color:#4f5f78;
    }
    .mono{ font-family:"IBM Plex Mono",monospace; font-size:.9rem; }
    .num{ text-align:right; font-variant-numeric:tabular-nums; }
    .num.neg{ color:var(--warn); font-weight:700; }
    .pill{
      display:inline-block; border-radius:999px; padding:.2rem .55rem; font-size:.75rem; font-weight:700;
    }
    .pill.wait{ background:#fff3cd; color:#664d03; }
    .pill.short{ background:#ffe2e0; color:#9d1b13; }
    .kpi{ border:1px solid var(--line); border-radius:12px; background:#f9fbff; padding:.65rem .8rem; }
    .kpi .lbl{ color:var(--muted); font-size:.75rem; text-transform:uppercase; letter-spacing:.06em; }
    .kpi .val{ font-weight:700; font-size:1.1rem; }
    .result-section{
      border:1px solid var(--line);
      border-radius:16px;
      background:#fff;
      box-shadow:0 10px 22px rgba(16,33,58,.06);
      overflow:hidden;
    }
    .result-head{
      padding:.7rem .9rem;
      font-weight:700;
      letter-spacing:.03em;
      text-transform:uppercase;
      font-size:.8rem;
    }
    .result-head.demand{ background:#e8f0ff; color:#1148a6; border-bottom:1px solid #cdddfd; }
    .result-head.supply{ background:#e9fbf1; color:#166b46; border-bottom:1px solid #c6eed8; }
    .result-head.cover{ background:#eef3fb; color:#3a4e71; border-bottom:1px solid #d8e2f1; }
    .result-body{ padding:.8rem; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="d-flex justify-content-between align-items-start gap-3 mb-3">
      <div>
        <div class="title">Phase 1: Read-only Allocation Board</div>
        <div class="sub">Training mode for Demand Queue, Supply Pool, and Coverage. Loaded {{ loaded_at }}</div>
      </div>
      <div class="d-flex align-items-center gap-2">
        <span class="badge-ro">READ ONLY</span>
        <a class="btn btn-outline-secondary btn-sm" href="/">Back</a>
      </div>
    </div>

    <div class="card-lite p-3 mb-3">
      <div class="row g-3">
        <div class="col-12 col-lg-6">
          <div class="panel-split">
            <div class="panel-title">Demand</div>
            <div class="row g-2 align-items-end">
              <div class="col-12 col-md-6">
                <label class="form-label mb-1">SO / QB</label>
                <input id="flt-so" class="form-control" placeholder="e.g. SO-2025xxxx">
              </div>
              <div class="col-12 col-md-6">
                <label class="form-label mb-1">Customer contains</label>
                <input id="flt-customer" class="form-control" placeholder="e.g. Applied">
              </div>
              <div class="col-12 col-md-6">
                <label class="form-label mb-1">Status</label>
                <select id="flt-status" class="form-select">
                  <option value="">Waiting + Shortage</option>
                  <option value="Waiting">Waiting</option>
                  <option value="Shortage">Shortage</option>
                </select>
              </div>
              <div class="col-6 col-md-3 d-grid">
                <button id="btn-demand-search" class="btn btn-primary">Search</button>
              </div>
              <div class="col-6 col-md-3 d-grid">
                <button id="btn-demand-load" class="btn btn-outline-secondary">Load</button>
              </div>
            </div>
          </div>
        </div>
        <div class="col-12 col-lg-6">
          <div class="panel-split">
            <div class="panel-title">Supply</div>
            <div class="row g-2 align-items-end">
              <div class="col-12 col-md-9">
                <label class="form-label mb-1">Item contains</label>
                <input id="flt-item" class="form-control" placeholder="e.g. I9-14900">
              </div>
              <div class="col-12 col-md-3 d-grid">
                <button id="btn-supply-search" class="btn btn-primary">Search</button>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="row g-2 mt-2">
        <div class="col-6 col-md-3"><div class="kpi"><div class="lbl">Demand Rows</div><div id="kpi-demand-rows" class="val mono">0</div></div></div>
        <div class="col-6 col-md-3"><div class="kpi"><div class="lbl">Supply Rows</div><div id="kpi-supply-rows" class="val mono">0</div></div></div>
        <div class="col-6 col-md-3"><div class="kpi"><div class="lbl">Coverage Rows</div><div id="kpi-cov-rows" class="val mono">0</div></div></div>
        <div class="col-6 col-md-3"><div class="kpi"><div class="lbl">Items in Deficit</div><div id="kpi-deficit" class="val mono">0</div></div></div>
      </div>
    </div>

    <div class="row g-3">
      <div class="col-12 col-xl-6">
        <div class="result-section h-100">
          <div class="result-head demand">Demand Section</div>
          <div class="result-body">
            <div class="fw-bold mb-2">Demand Queue</div>
            <div class="table-wrap">
              <table class="table table-sm table-hover align-middle">
                <thead><tr><th>SO</th><th>Customer</th><th>Item</th><th class="num">Demand</th><th class="num">Assigned</th><th class="num">Gap</th><th>Status</th><th>Need Date</th></tr></thead>
                <tbody id="tb-demand"><tr><td colspan="8" class="text-center text-muted py-3">Loading...</td></tr></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
      <div class="col-12 col-xl-6">
        <div class="result-section h-100">
          <div class="result-head supply">Supply Section</div>
          <div class="result-body">
            <div class="fw-bold mb-2">Supply Pool</div>
            <div class="table-wrap">
              <table class="table table-sm table-hover align-middle">
                <thead><tr><th>POD</th><th>Item</th><th class="num">Remaining</th><th>ETA</th><th>Vendor</th></tr></thead>
                <tbody id="tb-supply"><tr><td colspan="5" class="text-center text-muted py-3">Loading...</td></tr></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
      <div class="col-12">
        <div class="result-section">
          <div class="result-head cover">Coverage (Demand vs Supply)</div>
          <div class="result-body">
            <div class="fw-bold mb-2">Coverage by Item</div>
            <div class="table-wrap">
              <table class="table table-sm table-hover align-middle">
                <thead><tr><th>Item</th><th class="num">Demand</th><th class="num">Supply</th><th class="num">Gap</th><th class="num">Coverage %</th></tr></thead>
                <tbody id="tb-cover"><tr><td colspan="5" class="text-center text-muted py-3">Loading...</td></tr></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
  (function(){
    const q = (id) => document.getElementById(id);
    const fmtNum = (v) => {
      const n = Number(v || 0);
      return Number.isFinite(n) ? n.toLocaleString(undefined, {maximumFractionDigits: 2}) : "0";
    };
    const esc = (s) => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;");

    function demandRow(r){
      const gap = Number(r.gap_qty || 0);
      const statusClass = String(r.status || "").toLowerCase().startsWith("short") ? "short" : "wait";
      return `<tr>
        <td class="mono">${esc(r.qb_num)}</td>
        <td>${esc(r.customer)}</td>
        <td>${esc(r.item)}</td>
        <td class="num mono">${fmtNum(r.demand_qty)}</td>
        <td class="num mono">${fmtNum(r.assigned_qty)}</td>
        <td class="num mono ${gap>0?'neg':''}">${fmtNum(r.gap_qty)}</td>
        <td><span class="pill ${statusClass}">${esc(r.status)}</span></td>
        <td class="mono">${esc(r.need_date)}</td>
      </tr>`;
    }

    function supplyRow(r){
      return `<tr>
        <td class="mono">${esc(r.pod_no)}</td>
        <td>${esc(r.item)}</td>
        <td class="num mono">${fmtNum(r.remaining_qty)}</td>
        <td class="mono">${esc(r.eta_date)}</td>
        <td>${esc(r.vendor)}</td>
      </tr>`;
    }

    function coverRow(r){
      const gap = Number(r.gap_qty || 0);
      return `<tr>
        <td>${esc(r.item)}</td>
        <td class="num mono">${fmtNum(r.demand_qty)}</td>
        <td class="num mono">${fmtNum(r.supply_qty)}</td>
        <td class="num mono ${gap<0?'neg':''}">${fmtNum(r.gap_qty)}</td>
        <td class="num mono">${fmtNum(r.coverage_pct)}</td>
      </tr>`;
    }

    async function loadBoard(){
      const params = new URLSearchParams({
        item: q("flt-item").value.trim(),
        so: q("flt-so").value.trim(),
        customer: q("flt-customer").value.trim(),
        status: q("flt-status").value.trim()
      });
      const res = await fetch(`/api/phase1/board?${params.toString()}`);
      const data = await res.json();
      if (!data.ok){ throw new Error(data.error || "Load failed"); }

      const d = data.demand_rows || [];
      const s = data.supply_rows || [];
      const c = data.coverage_rows || [];

      q("tb-demand").innerHTML = d.length ? d.map(demandRow).join("") : '<tr><td colspan="8" class="text-center text-muted py-3">No rows</td></tr>';
      q("tb-supply").innerHTML = s.length ? s.map(supplyRow).join("") : '<tr><td colspan="5" class="text-center text-muted py-3">No rows</td></tr>';
      q("tb-cover").innerHTML = c.length ? c.map(coverRow).join("") : '<tr><td colspan="5" class="text-center text-muted py-3">No rows</td></tr>';

      q("kpi-demand-rows").textContent = String(d.length);
      q("kpi-supply-rows").textContent = String(s.length);
      q("kpi-cov-rows").textContent = String(c.length);
      q("kpi-deficit").textContent = String(c.filter(x => Number(x.gap_qty || 0) < 0).length);
    }

    ["btn-demand-search","btn-demand-load","btn-supply-search"].forEach((id) => {
      q(id).addEventListener("click", function(){ loadBoard().catch(alert); });
    });
    ["flt-item","flt-so","flt-customer"].forEach((id) => {
      q(id).addEventListener("keydown", function(e){ if (e.key === "Enter") { e.preventDefault(); loadBoard().catch(alert); }});
    });
    q("flt-status").addEventListener("change", function(){ loadBoard().catch(alert); });

    loadBoard().catch((e) => {
      q("tb-demand").innerHTML = `<tr><td colspan="8" class="text-center text-danger py-3">${esc(e.message)}</td></tr>`;
      q("tb-supply").innerHTML = `<tr><td colspan="5" class="text-center text-danger py-3">${esc(e.message)}</td></tr>`;
      q("tb-cover").innerHTML = `<tr><td colspan="5" class="text-center text-danger py-3">${esc(e.message)}</td></tr>`;
    });
  })();
  </script>
</body>
</html>
"""

INVENTORY_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
  <meta charset="utf-8">
  <title>Inventory Count</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{ --ink:#0f172a; --muted:#6b7280; --bg:#f7fafc; --hdr:#f8fafc; }
    html,body{ background:var(--bg); color:var(--ink); }
    body{ padding:28px; }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); }
    .split-card{ border:1px solid #e2e8f0; border-radius:14px; background:#fff; padding:1rem; height:100%; }
    .split-title{ font-weight:700; letter-spacing:.02em; margin-bottom:.5rem; }
    .summary{ display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:1rem; }
    .metric{ border:1px solid #e2e8f0; border-radius:12px; padding:1rem; background:#fff; }
    .metric .label{ text-transform:uppercase; font-size:.75rem; letter-spacing:.08em; color:var(--muted); font-weight:600; }
    .metric .value{ font-size:1.6rem; font-weight:700; }
    .table-responsive{ max-height:70vh; overflow:auto; }
    .table thead th{ position:sticky; top:0; z-index:2; background:var(--hdr); }
  </style>
  </head>
<body>
  <div class="d-flex justify-content-between align-items-center mb-2">
    <div>
      <div class="h3 m-0">Inventory Count</div>
      <div class="text-muted small">Loaded {{ loaded_at }}</div>
    </div>
    <div class="d-flex gap-2">
      <a class="btn btn-sm btn-outline-secondary" href="/">Home</a>
    </div>
  </div>

  <div class="row g-3 mb-4">
    <div class="col-12 col-lg-6">
      <div class="split-card">
        <div class="split-title">Demand</div>
        <form method="get" class="row gy-3 align-items-end">
          <input type="hidden" name="item" value="{{ item_val or '' }}">
          <div class="col-12">
            <label class="form-label" for="inv-so">Search By SO / QB</label>
            <input id="inv-so" class="form-control form-control-lg" style="height:60px;font-size:1.05rem" name="so" placeholder="SO-20251368 or 20251368" value="{{ so_val or '' }}">
          </div>
          <div class="col-6">
            <button class="btn btn-primary px-4 w-100" style="height:52px;font-size:1rem;font-weight:600">Search</button>
          </div>
          <div class="col-6">
            <a class="btn btn-outline-secondary w-100" style="height:52px;font-size:1rem;font-weight:600" href="/inventory_count?reload=1">Load</a>
          </div>
        </form>
      </div>
    </div>
    <div class="col-12 col-lg-6">
      <div class="split-card">
        <div class="split-title">Supply</div>
        <form method="get" class="row gy-3 align-items-end">
          <input type="hidden" name="so" value="{{ so_val or '' }}">
          <div class="col-12">
            <label class="form-label" for="inv-item">Search By Item</label>
            <div style="position:relative;">
              <input id="inv-item" autocomplete="off" class="form-control form-control-lg" style="height:60px;font-size:1.05rem" name="item" placeholder="Type to search (fuzzy)" value="{{ item_val or '' }}">
              <div id="inv-suggest" class="list-group" style="position:absolute; top:62px; left:0; right:0; z-index:1000; display:none; max-height:240px; overflow:auto;"></div>
            </div>
          </div>
          <div class="col-12">
            <button class="btn btn-primary px-4 w-100" style="height:52px;font-size:1rem;font-weight:600">Search</button>
          </div>
        </form>
      </div>
    </div>
  </div>

  <div class="summary mb-4">
    <div class="metric">
      <div class="label">On Hand</div>
      <div class="value">{{ on_hand if on_hand is not none else '�?"' }}</div>
    </div>
    <div class="metric">
      <div class="label">On Hand - WIP</div>
      <div class="value">{{ on_hand_wip if on_hand_wip is not none else '�?"' }}</div>
    </div>
  </div>

  <div class="card-lite bg-white">
    <div class="card-header fw-bold">On Sales Order</div>
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
              {% for r in so_rows %}
                <tr>
                  {% for c in so_columns %}
                    {% if c == 'Item' %}
                      <td class="nowrap">{{ r[c] }}</td>
                    {% else %}
                      <td>{{ r[c] }}</td>
                    {% endif %}
                  {% endfor %}
                </tr>
              {% endfor %}
            {% else %}
              <tr><td colspan="{{ so_columns|length }}" class="text-center text-muted">No rows</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
      <div class="text-muted small">Source: public.wo_structured</div>
    </div>
  </div>

  <div class="card-lite bg-white mt-3">
    <div class="card-header fw-bold">Open Purchase Orders</div>
    <div class="card-body">
      <div class="table-responsive">
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
              {% for r in open_po_rows %}
                <tr>
                  {% for c in open_po_columns %}
                    <td>{{ r[c] }}</td>
                  {% endfor %}
                </tr>
              {% endfor %}
            {% else %}
              <tr><td colspan="{{ open_po_columns|length }}" class="text-center text-muted">No open purchase orders</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
      <div class="text-muted small">Source: public.Open_Purchase_Orders</div>
    </div>
  </div>

  <script>
  (function () {
    var panel = document.getElementById('inv-item-detail-panel');

    var cache = {};

    // --- fuzzy suggest for item input ---
    var input = document.getElementById('inv-item');
    var list = document.getElementById('inv-suggest');
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
        if (!e.target.closest || (!e.target.closest('#inv-suggest') && !e.target.closest('#inv-item'))) hideList();
      });
    }

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

    function renderSO(item, soData) {
      var totalText = (soData && (soData.total_on_sales !== null && soData.total_on_sales !== undefined))
        ? ('On Sales Order: ' + soData.total_on_sales)
        : '';
      panel.innerHTML = [
        '<div class="d-flex justify-content-between flex-wrap gap-2 mb-2">',
          '<div><h6 class="m-0">On Sales Order — ' + escapeHtml(item) + '</h6></div>',
          (totalText ? '<div class="small fw-semibold text-primary">' + escapeHtml(totalText) + '</div>' : ''),
        '</div>',
        buildTable(soData ? soData.columns : [], soData ? soData.rows : [])
      ].join('');
      panel.style.display = 'block';
      try { panel.scrollIntoView({ behavior: 'smooth', block: 'start' }); } catch (_) {}
    }

    document.addEventListener('click', function (event) {
      var link = event.target.closest('.inv-detail-link');
      if (!link) return;
      event.preventDefault();
      var item = link.getAttribute('data-item') || '';
      if (!item) return;

      if (cache[item]) { renderSO(item, cache[item].so); return; }

      panel.style.display = 'block';
      panel.innerHTML = '<div class="text-muted small">Loading ' + escapeHtml(item) + '…</div>';

      fetch('/api/item_overview?item=' + encodeURIComponent(item))
        .then(function (resp) { if (!resp.ok) throw new Error('Server error (' + resp.status + ')'); return resp.json(); })
        .then(function (json) {
          if (!json.ok) throw new Error(json.error || 'Failed to load item');
          cache[item] = json;
          renderSO(item, json.so);
        })
        .catch(function (err) {
          panel.innerHTML = '<div class="alert alert-danger mb-0">Error loading ' + escapeHtml(item) + ': ' + escapeHtml(err.message) + '</div>';
          panel.style.display = 'block';
        });
    });
  })();
  </script>
</body>
</html>
"""

SUBPAGE_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
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
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
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

PRODUCTION_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
  <meta charset="utf-8">
  <title>Production Planning</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root{ --ink:#0f172a; --muted:#6b7280; --bg:#f7fafc; --card:#ffffff; }
    html,body{ background:var(--bg); color:var(--ink); }
    body{ padding:28px; }
    .card-lite{ border-radius:14px; box-shadow:0 10px 22px rgba(0,0,0,.06); background:var(--card); }
    .day-card{ border-left:4px solid #0d6efd; }
    .day-header{ font-weight:600; font-size:1rem; }
    .order-line{ display:flex; justify-content:space-between; align-items:center; padding:.4rem .6rem; border-radius:10px; }
    .order-line:nth-child(odd){ background:#f9fafb; }
    .order-line:nth-child(even){ background:#eef2ff; }
    .order-main{ font-weight:600; }
    .order-sub{ font-size:.85rem; color:var(--muted); }
    .order-link{ text-decoration:none; color:#0d6efd; font-size:.85rem; }
    .order-link:hover{ text-decoration:underline; }
  </style>
</head>
<body>
  <div class="d-flex justify-content-between align-items-center mb-3">
    <div>
      <div class="h3 m-0">Production Planning</div>
      <div class="text-muted small">Loaded {{ loaded_at }}</div>
    </div>
    <div class="d-flex gap-2">
      <a class="btn btn-sm btn-outline-secondary" href="/">Home</a>
      <a class="btn btn-sm btn-outline-primary" href="/production_planning?reload=1">Reload</a>
    </div>
  </div>

  {% if not date_groups %}
    <div class="alert alert-info">No production data available.</div>
  {% else %}
    <div class="row g-3">
      {% for group in date_groups %}
        <div class="col-12 col-md-6 col-xl-4">
          <div class="card-lite day-card p-3 h-100">
            <div class="day-header mb-2">{{ group.date }}</div>
            <div class="small text-muted mb-2">{{ group.orders|length }} order(s)</div>
            {% if group.orders %}
              <div class="d-flex flex-column gap-1">
                {% for o in group.orders %}
                  <div class="order-line">
                    <div class="me-2">
                      <div class="order-main">{{ o.qb_num }}</div>
                      <div class="order-sub">
                        {{ o.customer or '-' }}{% if o.line %} • {{ o.line }}{% endif %}
                      </div>
                    </div>
                    {% if o.pdf_url %}
                      <a class="order-link" href="{{ o.pdf_url }}" target="_blank">PDF</a>
                    {% endif %}
                  </div>
                {% endfor %}
              </div>
            {% else %}
              <div class="text-muted small">No orders on this date.</div>
            {% endif %}
          </div>
        </div>
      {% endfor %}
    </div>
  {% endif %}
</body>
</html>
"""

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
          ���?"
        {% endif %}
      </div>
    </div>
    <div class="metric">
      <div class="label">Earliest ATP (1 unit)</div>
      <div class="value">
        {% if earliest_atp %}
          {{ earliest_atp }}
        {% else %}
          ���?"
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
                <th>{{ c }}</th>
              {% endfor %}
            </tr>
          </thead>
          <tbody>
            {% if ledger_rows %}
              {% for r in ledger_rows %}
                <tr>
                  {% for c in ledger_columns %}
                    <td>{{ r[c] }}</td>
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
