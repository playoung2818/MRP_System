/* Non-Neousys: QTY-sized serial rows; CPU serial + batch stored as serial_batch. */
(() => {
  const mode = item => {
    const name = String(item || '').trim().toLowerCase();
    return {dust: name.startsWith('dust'), cpu: name.startsWith('i') || name.startsWith('core')};
  };
  const combine = (serial, batch, cpu) => {
    serial = serial.trim(); batch = batch.trim();
    return !serial || serial.toUpperCase() === 'NA' || !cpu ? serial : `${serial}_${batch}`;
  };
  const nextScanTarget = (cpu, isSerial, index, count) => cpu && isSerial
    ? {index, field: 'batch'} : index + 1 < count ? {index: index + 1, field: 'serial'} : null;
  window.PurchaseReceiving = {mode, combine, nextScanTarget};
  const lines = document.getElementById('receipt-lines');
  const template = document.getElementById('receipt-line-template');
  const form = document.getElementById('manual-receiving');
  if (!lines || !form || !template) return;
  const expanded = form.dataset.expanded === 'true';
  function setup(row, saved = {}) {
    const part = row.querySelector('[name=part_number]');
    const qty = row.querySelector('[name=quantity]');
    const text = row.querySelector('[name=serial_number]');
    for (const [key, value] of Object.entries(saved)) {
      const field = row.querySelector(`[name=${key}]`);
      if (field) field.value = value;
    }
    if (!expanded) return;
    const panel = document.createElement('div'); panel.className = 'receipt-serial-panel';
    text.after(panel);
    const entries = document.createElement('div'); panel.append(entries);
    const state = [];
    let current = mode(part.value), active = 0, wasDust = current.dust;
    const savedLines = text.value.split(/\r?\n/).filter(value => value.trim());
    for (const value of savedLines) {
      const split = current.cpu ? value.lastIndexOf('_') : -1;
      state.push({serial: split > 0 ? value.slice(0, split) : value,
                  batch: split > 0 ? value.slice(split + 1) : ''});
    }
    function sync() {
      if (current.dust) return;
      text.value = state.slice(0, active).map(record => combine(record.serial, record.batch, current.cpu))
        .filter(Boolean).join('\n');
      [...entries.children].forEach((line, index) => {
        const record = state[index];
        line.querySelector('[data-batch]').required = index < active && current.cpu &&
          !!record.serial.trim() && record.serial.trim().toUpperCase() !== 'NA';
      });
    }
    function addEntry(index) {
      if (!state[index]) state[index] = {serial: '', batch: ''};
      const line = document.createElement('div'); line.className = 'receipt-serial-line';
      const number = document.createElement('span'); number.textContent = String(index + 1);
      const serial = document.createElement('input'); serial.type = 'text'; serial.className = 'form-control';
      serial.dataset.serial = ''; serial.placeholder = 'Serial number'; serial.maxLength = 255;
      serial.setAttribute('aria-label', `Serial number ${index + 1}`);
      const individualBatch = document.createElement('input'); individualBatch.type = 'text';
      individualBatch.className = 'form-control'; individualBatch.dataset.batch = '';
      individualBatch.placeholder = 'Batch number'; individualBatch.maxLength = 255;
      individualBatch.setAttribute('aria-label', `Batch number ${index + 1}`);
      serial.addEventListener('input', () => {state[index].serial = serial.value; sync();});
      individualBatch.addEventListener('input', () => {state[index].batch = individualBatch.value; sync();});
      serial.addEventListener('paste', event => {
        const values = event.clipboardData.getData('text').split(/\r?\n/).map(value => value.trim()).filter(Boolean);
        if (values.length < 2) return;
        if (values.length > active - index) {
          event.preventDefault(); window.alert('More serials pasted than available QTY rows. Increase QTY first.'); return;
        }
        event.preventDefault();
        values.forEach((value, offset) => {
          state[index + offset].serial = value;
          entries.children[index + offset].querySelector('[data-serial]').value = value;
        }); sync();
      });
      for (const [field, isSerial] of [[serial, true], [individualBatch, false]]) {
        field.addEventListener('keydown', event => {
          if (event.key !== 'Enter') return;
          event.preventDefault();
          const next = nextScanTarget(current.cpu, isSerial, index, active);
          if (next) entries.children[next.index].querySelector(`[data-${next.field}]`).focus();
        });
      }
      line.append(number, serial, individualBatch); entries.append(line);
    }
    function render() {
      current = mode(part.value);
      if (current.dust && !wasDust) text.value = 'NA';
      text.hidden = !current.dust; text.required = current.dust;
      panel.hidden = current.dust; wasDust = current.dust;
      const quantity = Number(qty.value);
      qty.setCustomValidity(!current.dust && (!Number.isInteger(quantity) || quantity < 1 || quantity > 1000)
        ? 'For expanded serial entry, use a whole QTY from 1 to 1000 per part row. Add another part row for more units.' : '');
      active = Math.max(0, Math.min(1000, Math.floor(quantity || 0)));
      if (current.dust) {
        [...entries.querySelectorAll('input')].forEach(input => {input.required = false; input.disabled = true;});
        return;
      }
      while (entries.children.length < active) addEntry(entries.children.length);
      [...entries.children].forEach((line, index) => {
        line.hidden = index >= active; line.classList.toggle('has-batch', current.cpu);
        const serial = line.querySelector('[data-serial]'), individualBatch = line.querySelector('[data-batch]');
        serial.value = state[index].serial; individualBatch.value = state[index].batch;
        serial.disabled = index >= active;
        individualBatch.hidden = !current.cpu; individualBatch.disabled = index >= active || !current.cpu;
      }); sync();
    }
    part.addEventListener('change', render); qty.addEventListener('input', render);
    form.addEventListener('submit', sync);
    render();
  }
  function add(saved) {
    lines.append(template.content.cloneNode(true)); setup(lines.lastElementChild, saved);
  }
  document.getElementById('add-receipt').addEventListener('click', () => add());
  lines.addEventListener('click', event => {
    const button = event.target.closest('[data-remove-receipt]');
    if (button) button.closest('tr').remove();
  });
  const initial = JSON.parse(document.getElementById('receiving-initial').textContent);
  if (initial.length) initial.forEach(add); else add();
})();
