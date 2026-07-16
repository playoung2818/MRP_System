PERIPHERAL_STATUS_TPL = """
<!doctype html>
<html>
<head>
  <link rel="icon" href="/static/favicon.ico" type="image/x-icon">
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
