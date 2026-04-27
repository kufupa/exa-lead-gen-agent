function initNotesPanel(panel) {
  if (!(panel instanceof HTMLElement)) {
    return;
  }

  const textarea = panel.querySelector("#notes");
  const saveButton = panel.querySelector("#save-notes");
  if (!(textarea instanceof HTMLTextAreaElement) || !(saveButton instanceof HTMLButtonElement)) {
    return;
  }

  const syncSaveState = () => {
    const original = textarea.dataset.savedValue || "";
    saveButton.disabled = textarea.value === original;
  };

  textarea.addEventListener("input", syncSaveState);
  syncSaveState();
}

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
    const saveButton = panel.querySelector("#save-notes");
    if (saveButton instanceof HTMLButtonElement) {
      saveButton.disabled = true;
    }
  }
});

document.body.addEventListener("htmx:afterSwap", (event) => {
  const detailPanel = event?.detail?.target;
  if (detailPanel instanceof HTMLElement && detailPanel.id === "detail-panel") {
    initNotesPanel(detailPanel);
  }
});

document.addEventListener("DOMContentLoaded", () => {
  const detailPanel = document.querySelector("#detail-panel");
  if (detailPanel instanceof HTMLElement) {
    initNotesPanel(detailPanel);
  }
});
