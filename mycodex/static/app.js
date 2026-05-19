(() => {
  const chartButtons = Array.from(document.querySelectorAll("[data-chart-toggle]"));
  const chartPanels = Array.from(document.querySelectorAll("[data-chart-panel]"));

  const setChart = (name) => {
    if (!name) {
      return;
    }
    chartButtons.forEach((button) => {
      button.classList.toggle("is-active", button.dataset.chartToggle === name);
    });
    chartPanels.forEach((panel) => {
      panel.classList.toggle("hidden", panel.dataset.chartPanel !== name);
    });
    try {
      window.localStorage.setItem("mycodex-usage-chart", name);
    } catch (_err) {
      // Local storage is optional.
    }
  };

  if (chartButtons.length && chartPanels.length) {
    let preferred = "hourly";
    try {
      preferred = window.localStorage.getItem("mycodex-usage-chart") || preferred;
    } catch (_err) {
      preferred = "hourly";
    }
    if (!chartButtons.some((button) => button.dataset.chartToggle === preferred)) {
      preferred = chartButtons[0].dataset.chartToggle;
    }
    setChart(preferred);
    document.addEventListener("click", (event) => {
      const button = event.target.closest("[data-chart-toggle]");
      if (button) {
        event.preventDefault();
        setChart(button.dataset.chartToggle);
        button.blur();
      }
    });
  }

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
