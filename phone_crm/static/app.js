function setMirrorValue(target, value) {
  if (!target) return;
  if (target.tagName === "TEXTAREA" || target.tagName === "INPUT") {
    target.value = value;
    return;
  }
  target.textContent = value;
}

function setButtonLoadingState(button, loading) {
  if (!button || button.tagName !== "BUTTON") return;
  if (!button.dataset.defaultText) {
    button.dataset.defaultText = button.textContent;
  }

  if (loading) {
    button.disabled = true;
    button.classList.add("is-loading");
    button.textContent = button.dataset.loadingLabel || "Loading...";
    return;
  }

  button.disabled = false;
  button.classList.remove("is-loading");
  button.textContent = button.dataset.defaultText;
}

function setNotesFormLoading(form, loading, mode = "save") {
  if (!form) return;
  const saveButton = form.querySelector(".notesSaveBtn");
  const discardButton = form.querySelector(".notesDiscardBtn");
  if (mode === "save" || mode === "both") {
    setButtonLoadingState(saveButton, loading);
  }
  if (mode === "discard" || mode === "both") {
    setButtonLoadingState(discardButton, loading);
  }
}

function getNotesFormFromElement(element) {
  if (!element) return null;
  if (element.matches && element.matches(".notes-form")) {
    return element;
  }
  return element.closest ? element.closest(".notes-form") : null;
}

function syncNotesMirrors() {
  const scope = document.querySelector("#contactPanel") || document;
  const notesInput = scope.querySelector("#notesText");
  if (!notesInput) {
    return;
  }
  const sourceValue = notesInput.value || "";
  const mirrors = scope.querySelectorAll("[data-notes-mirror]");
  mirrors.forEach((mirror) => {
    if (mirror === notesInput) return;
    setMirrorValue(mirror, sourceValue);
  });
}

document.addEventListener("submit", (event) => {
  const form = event.target.closest("form");
  if (!form) return;
  const notesForm = getNotesFormFromElement(form);
  if (notesForm) {
    setNotesFormLoading(notesForm, true, "save");
  }

  const contactPanel = form.closest("#contactPanel");
  if (contactPanel) {
    const notesInput = contactPanel.querySelector("#notesText");
    if (notesInput) {
      const currentValue = notesInput.value || "";
      const mirrors = contactPanel.querySelectorAll("[data-notes-mirror]");
      mirrors.forEach((mirror) => {
        if (mirror === notesInput) return;
        setMirrorValue(mirror, currentValue);
      });
      return;
    }
  }

  const notesInput = form.querySelector("#notesText");
  if (!notesInput) return;
  const currentValue = notesInput.value || "";
  const mirrors = form.querySelectorAll("[data-notes-mirror]");
  mirrors.forEach((mirror) => {
    if (mirror === notesInput) return;
    setMirrorValue(mirror, currentValue);
  });
});

document.addEventListener("click", (event) => {
  const discardBtn = event.target.closest("[data-discard-notes]");
  if (!discardBtn) return;
  const root = discardBtn.closest("form") || document;
  const notesInput = root.querySelector("#notesText");
  if (!notesInput) return;
  const notesForm = getNotesFormFromElement(root);
  setNotesFormLoading(notesForm, true, "discard");
  const savedValue = notesInput.dataset.savedValue || "";
  notesInput.value = savedValue;
  syncNotesMirrors();
  window.setTimeout(() => {
    setNotesFormLoading(notesForm, false, "discard");
  }, 120);
});

document.body.addEventListener("htmx:beforeRequest", (event) => {
  const notesForm = getNotesFormFromElement(event.detail && event.detail.elt);
  if (!notesForm) return;
  setNotesFormLoading(notesForm, true, "save");
});

document.body.addEventListener("htmx:afterRequest", (event) => {
  const notesForm = getNotesFormFromElement(event.detail && event.detail.elt);
  if (!notesForm) return;
  setNotesFormLoading(notesForm, false, "both");
});

document.body.addEventListener("htmx:requestError", (event) => {
  const notesForm = getNotesFormFromElement(event.detail && event.detail.elt);
  if (!notesForm) return;
  setNotesFormLoading(notesForm, false, "both");
});

document.body.addEventListener("htmx:afterSwap", () => {
  syncNotesMirrors();
});

document.addEventListener("DOMContentLoaded", () => {
  syncNotesMirrors();
});
