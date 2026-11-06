/* Snoopy opens the existing search form; no chatbot or new API required. */
(() => {
  const toggle = document.getElementById('snoopy-search-toggle');
  const bubble = document.getElementById('snoopy-search-bubble');
  const close = document.getElementById('snoopy-search-close');
  const input = document.getElementById('global-search-input');
  if (!toggle || !bubble || !close || !input) return;

  function setOpen(open, restoreFocus = false) {
    bubble.hidden = !open;
    toggle.setAttribute('aria-expanded', String(open));
    if (open) input.focus();
    else {
      const suggestions = document.getElementById('global-search-suggest');
      if (suggestions) suggestions.style.display = 'none';
      if (restoreFocus) toggle.focus();
    }
  }
  toggle.addEventListener('click', () => setOpen(bubble.hidden));
  close.addEventListener('click', () => setOpen(false, true));
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && !bubble.hidden) {
      event.preventDefault();
      setOpen(false, true);
    }
  });
  document.addEventListener('click', event => {
    if (!bubble.hidden && !bubble.contains(event.target) && !toggle.contains(event.target)) {
      setOpen(false);
    }
  });
  document.addEventListener('focusin', event => {
    if (!bubble.hidden && !bubble.contains(event.target) && !toggle.contains(event.target)) {
      setOpen(false);
    }
  });
})();
