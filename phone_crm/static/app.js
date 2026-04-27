document.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }

  const actionButton = target.closest(".contact-actions [hx-post]");
  if (actionButton) {
    event.stopPropagation();
    return;
  }

  if (!target.matches("[data-discard-notes]")) {
    return;
  }
  const panel = target.closest("#detail-panel");
  if (!panel) {
    return;
  }
  const textarea = panel.querySelector("textarea");
  if (textarea && textarea instanceof HTMLTextAreaElement) {
    textarea.value = textarea.dataset.savedValue || "";
  }
});
