/* Homepage item price history: search an item, draw unit cost per POD as an SVG step chart. */
(() => {
  const form = document.getElementById('price-form');
  const input = document.getElementById('price-item');
  const options = document.getElementById('price-item-options');
  const status = document.getElementById('price-status');
  const chart = document.getElementById('price-chart');
  const summary = document.getElementById('price-summary');
  if (!form || !input || !options || !status || !chart || !summary) return;

  const SVG = 'http://www.w3.org/2000/svg';
  const money = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 });
  let suggestTimer;
  let request = 0;

  function el(name, attrs = {}, text) {
    const node = document.createElementNS(SVG, name);
    for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function show(message) {
    status.textContent = message;
    chart.hidden = true;
    summary.hidden = true;
  }

  input.addEventListener('input', () => {
    clearTimeout(suggestTimer);
    const q = input.value.trim();
    if (q.length < 2) return;
    suggestTimer = setTimeout(async () => {
      try {
        const response = await fetch('/api/item_cost/suggest?q=' + encodeURIComponent(q));
        const data = await response.json();
        if (!data.ok) return;
        options.replaceChildren(...data.items.map(row => {
          const option = document.createElement('option');
          option.value = row.item;
          option.label = row.purchases + (row.purchases === 1 ? ' purchase' : ' purchases');
          return option;
        }));
      } catch { /* suggestions are optional */ }
    }, 200);
  });

  form.addEventListener('submit', async event => {
    event.preventDefault();
    const item = input.value.trim();
    if (!item) return;
    const id = ++request;
    show('Loading price history…');
    try {
      const response = await fetch('/api/item_cost?item=' + encodeURIComponent(item));
      const data = await response.json();
      if (id !== request) return;
      if (!data.ok) { show(data.error || 'Price history is unavailable.'); return; }
      if (!data.points.length) { show('No priced purchases recorded for ' + data.item + '.'); return; }
      render(data.item, data.points);
    } catch {
      if (id === request) show('Price history could not be loaded. Try again.');
    }
  });

  function render(item, points) {
    const width = 720, height = 240;
    const pad = { top: 16, right: 16, bottom: 32, left: 64 };
    const times = points.map(p => Date.parse(p.date + 'T00:00:00'));
    const costs = points.map(p => p.unit_cost);
    let t0 = Math.min(...times), t1 = Math.max(...times);
    if (t0 === t1) { t0 -= 15 * 864e5; t1 += 15 * 864e5; }
    let c0 = Math.min(...costs), c1 = Math.max(...costs);
    const span = c1 - c0 || Math.max(c1 * 0.2, 1);
    c0 = Math.max(0, c0 - span * 0.25); c1 = c1 + span * 0.25;
    const x = t => pad.left + (t - t0) / (t1 - t0) * (width - pad.left - pad.right);
    const y = c => pad.top + (1 - (c - c0) / (c1 - c0)) * (height - pad.top - pad.bottom);

    const svg = el('svg', { viewBox: `0 0 ${width} ${height}`, role: 'img', 'aria-labelledby': 'price-chart-title' });
    svg.append(el('title', { id: 'price-chart-title' }, `Unit cost history for ${item}`));
    for (let i = 0; i <= 3; i++) {
      const c = c0 + (c1 - c0) * i / 3;
      svg.append(el('line', { class: 'grid', x1: pad.left, x2: width - pad.right, y1: y(c), y2: y(c) }));
      svg.append(el('text', { class: 'axis', x: pad.left - 8, y: y(c) + 4, 'text-anchor': 'end' }, money.format(c)));
    }
    const dateLabel = t => new Date(t).toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
    svg.append(el('text', { class: 'axis', x: pad.left, y: height - 8 }, dateLabel(Math.min(...times))));
    svg.append(el('text', { class: 'axis', x: width - pad.right, y: height - 8, 'text-anchor': 'end' }, dateLabel(Math.max(...times))));

    let d = '';
    points.forEach((p, i) => {
      const px = x(times[i]), py = y(p.unit_cost);
      d += i === 0 ? `M${px},${py}` : `H${px}V${py}`;
    });
    svg.append(el('path', { class: 'line', d }));
    points.forEach((p, i) => {
      const dot = el('circle', { class: 'dot', cx: x(times[i]), cy: y(p.unit_cost), r: 4 });
      dot.append(el('title', {}, `${p.date}  ${p.pod}\n${money.format(p.unit_cost)} each, qty ${p.qty}${p.vendor ? '\n' + p.vendor : ''}`));
      svg.append(dot);
    });
    chart.replaceChildren(svg);

    const latest = points[points.length - 1];
    const lowest = Math.min(...costs), highest = Math.max(...costs);
    const stat = (label, value, detail) => {
      const block = document.createElement('div');
      const term = document.createElement('span'); term.textContent = label;
      const strong = document.createElement('strong'); strong.textContent = value;
      block.append(term, strong);
      if (detail) { const small = document.createElement('small'); small.textContent = detail; block.append(small); }
      return block;
    };
    summary.replaceChildren(
      stat('Latest', money.format(latest.unit_cost), `${latest.pod}, ${latest.date}`),
      stat('Lowest', money.format(lowest)),
      stat('Highest', money.format(highest)),
      stat('Purchases', String(points.length)),
    );
    status.textContent = item;
    chart.hidden = false;
    summary.hidden = false;
  }
})();
