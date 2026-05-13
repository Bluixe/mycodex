(() => {
  const interval = Number(document.body.dataset.refreshInterval || 0);
  if (!interval) {
    return;
  }

  const editing = () => {
    const el = document.activeElement;
    if (!el) {
      return false;
    }
    return ["INPUT", "TEXTAREA", "SELECT", "BUTTON"].includes(el.tagName);
  };

  window.setInterval(() => {
    if (!editing()) {
      window.location.reload();
    }
  }, interval);
})();
