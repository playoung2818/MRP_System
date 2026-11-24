/* Read-only preview: reuse the current dashboard's alerts, not invented counts. */
(() => {
  const greeting = document.getElementById('time-greeting');
  if (!greeting) return;
  const hour = new Date().getHours();
  greeting.textContent = hour < 12 ? 'Good morning' : hour < 18 ? 'Good afternoon' : 'Good evening';
})();
(async function () {
  const target = document.getElementById('attention-content');
  if (!target) return;
  try {
    const response = await fetch('/', { credentials: 'same-origin', cache: 'no-store' });
    if (!response.ok) throw new Error('Dashboard unavailable');
    const page = new DOMParser().parseFromString(await response.text(), 'text/html');
    const source = page.querySelector('[aria-labelledby="home-alerts-title"] .panel-list');
    if (!source) throw new Error('Dashboard alerts unavailable');
    const list = document.createElement('ul');
    list.className = 'alert-list';
    for (const row of source.querySelectorAll('li')) {
      const copy = row.querySelector('.home-alert-copy, .home-alert-empty');
      if (!copy) continue;
      const item = document.createElement('li');
      const link = row.querySelector('a.home-alert-link');
      const label = document.createElement(link ? 'a' : 'span');
      label.textContent = copy.textContent.trim();
      label.className = 'alert-label';
      if (link) {
        const url = new URL(link.getAttribute('href'), window.location.origin);
        if (url.origin !== window.location.origin || !['http:', 'https:'].includes(url.protocol)) {
          throw new Error('Unexpected alert destination');
        }
        label.href = url.pathname + url.search + url.hash;
      }
      item.append(label);
      list.append(item);
    }
    if (!list.children.length) throw new Error('Dashboard alert content unavailable');
    target.replaceChildren(list);
  } catch (error) {
    const message = document.createElement('p');
    message.className = 'status';
    message.append('Alerts could not be loaded. ');
    const link = document.createElement('a');
    link.href = '/';
    link.textContent = 'Open the current dashboard';
    message.append(link);
    target.replaceChildren(message);
  } finally {
    target.setAttribute('aria-busy', 'false');
  }
})();
