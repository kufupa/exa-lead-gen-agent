function setMirrorValue(target, value) {
  if (!target) return;
  if (target.tagName === "TEXTAREA" || target.tagName === "INPUT") {
    target.value = value;
    return;
  }
  target.textContent = value;
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
  const savedValue = notesInput.dataset.savedValue || "";
  notesInput.value = savedValue;
  syncNotesMirrors();
});

document.body.addEventListener("htmx:afterSwap", () => {
  syncNotesMirrors();
});

document.addEventListener("DOMContentLoaded", () => {
  syncNotesMirrors();
});
