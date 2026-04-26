const DATA_URL = 'data/hotel_leads_samples.json';
const STORAGE_KEY = 'crm-minimal-selected-contact-ids';
const NOTES_STORAGE_KEY = 'crm-minimal-contact-notes';

const state = {
  groups: [],
  activeContactId: null,
  selectedContactIds: new Set(),
  openHotels: new Set(),
  contactNotes: {},
};

const hotelsRoot = document.getElementById('hotelsRoot');
const summaryText = document.getElementById('summaryText');
const emptyState = document.getElementById('emptyState');
const contactPanel = document.getElementById('contactPanel');
const contactName = document.getElementById('contactName');
const contactMeta = document.getElementById('contactMeta');
const contactHotel = document.getElementById('contactHotel');
const contactTitle = document.getElementById('contactTitle');
const linkedinLink = document.getElementById('linkedinLink');
const notesText = document.getElementById('notesText');
const notesSaveBtn = document.getElementById('notesSaveBtn');
const notesDiscardBtn = document.getElementById('notesDiscardBtn');
const primaryHandle = document.getElementById('primaryHandle');
const phones = document.getElementById('phones');
const emails = document.getElementById('emails');
const otherDetail = document.getElementById('otherDetail');
const fitReason = document.getElementById('fitReason');
const evidenceSummary = document.getElementById('evidenceSummary');
const evidenceList = document.getElementById('evidenceList');
const sourcePayload = document.getElementById('sourcePayload');
const markBtn = document.getElementById('markContactBtn');

function t(v) {
  return typeof v === 'string' ? v.trim() : '';
}

function uniq(values) {
  const set = new Set();
  const out = [];
  for (const val of values) {
    const item = t(val);
    if (!item) continue;
    const key = item.toLowerCase();
    if (set.has(key)) continue;
    set.add(key);
    out.push(item);
  }
  return out;
}

function hasPhoneOrEmail(c) {
  return !!(uniq([c.phone, c.phone2]).length || uniq([c.email, c.email2]).length);
}

function primary(c) {
  return (
    uniq([c.phone, c.phone2])[0] ||
    uniq([c.email, c.email2])[0] ||
    t(c.otherContactDetail) ||
    'No contact detail'
  );
}

function hash(value) {
  let h = 0;
  for (let i = 0; i < value.length; i += 1) {
    h = (h * 31 + value.charCodeAt(i)) % 2147483647;
  }
  return String(Math.abs(h));
}

function hotelLink(value) {
  const raw = t(value);
  if (!raw) return '';
  if (/^https?:\/\//i.test(raw)) return raw;
  if (/^mailto:|^tel:/i.test(raw)) return raw;
  if (raw.includes('://')) return raw;
  if (/^[a-z0-9.-]+\.[a-z]{2,}(\/|$)/i.test(raw)) return `https://${raw}`;
  return '';
}

function orderedContactsInGroup(group) {
  return group.contacts
    .map((contact, index) => ({ contact, index }))
    .sort((left, right) => {
      const leftDone = state.selectedContactIds.has(left.contact.id) ? 1 : 0;
      const rightDone = state.selectedContactIds.has(right.contact.id) ? 1 : 0;
      if (leftDone !== rightDone) return leftDone - rightDone;
      return left.index - right.index;
    })
    .map((entry) => entry.contact);
}

function slug(v) {
  return t(v).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '') || 'hotel';
}

function resolveSources(payload) {
  if (!payload || typeof payload !== 'object') return [];
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload.contacts)) return [payload];
  if (Array.isArray(payload.hotels)) return payload.hotels;
  if (Array.isArray(payload.groups)) return payload.groups;
  return [];
}

function hotelNameFrom(source, index) {
  const fromContacts = Array.isArray(source.contacts) ? source.contacts.find((c) => t(c?.company))?.company : '';
  const candidate = t(source.hotel_name || source.hotelName || source.title || source.company || fromContacts);
  if (candidate) return candidate;

  const url = t(source.target_url || source.targetUrl || source.url);
  if (!url) return `Hotel ${index + 1}`;
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch (_e) {
    return url;
  }
}

function normalize(payload) {
  const sources = resolveSources(payload);
  const out = [];

  for (let i = 0; i < sources.length; i += 1) {
    const source = sources[i] || {};
    const contacts = Array.isArray(source.contacts) ? source.contacts : [];
    const name = hotelNameFrom(source, i);
    const seed = `${name}-${i}`;

    const normalizedContacts = contacts
      .map((c, ci) => {
        if (!c || typeof c !== 'object') return null;

        const row = {
          id: `c-${hash(`${seed}|${t(c.full_name)}|${t(c.phone)}|${t(c.email)}|${ci}`)}`,
          fullName: t(c.full_name) || '—',
          title: t(c.title) || '—',
          company: t(c.company) || name,
          hotelName: name,
          hotelUrl: t(c.target_url || source.target_url || source.targetUrl || ''),
          linkedin: t(c.linkedin_url),
          phone: t(c.phone),
          phone2: t(c.phone2),
          email: t(c.email),
          email2: t(c.email2),
          otherContactDetail: t(c.other_contact_detail),
          fitReason: t(c.fit_reason),
          evidenceSummary: t(c.contact_evidence_summary),
          evidence: Array.isArray(c.evidence) ? c.evidence : [],
          raw: c,
          sortIndex: ci,
        };

        row.primary = primary(row);
        return row;
      })
      .filter(Boolean);

    if (!normalizedContacts.length) continue;

    out.push({
      id: `h-${slug(name)}-${hash(seed)}`,
      name,
      sourceUrl: t(source.target_url || source.targetUrl || ''),
      contacts: normalizedContacts,
    });
  }

  return out;
}
function readSelection() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.filter((v) => typeof v === 'string') : [];
  } catch (_e) {
    return [];
  }
}

function saveSelection() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(Array.from(state.selectedContactIds)));
}

function readNotes() {
  const raw = localStorage.getItem(NOTES_STORAGE_KEY);
  if (!raw) return {};
  try {
    const data = JSON.parse(raw);
    return data && typeof data === 'object' ? data : {};
  } catch (_e) {
    return {};
  }
}

function saveNotes() {
  localStorage.setItem(NOTES_STORAGE_KEY, JSON.stringify(state.contactNotes));
}

function loadNotesForActive() {
  if (!state.activeContactId) {
    notesText.value = '';
    return;
  }
  notesText.value = state.contactNotes[state.activeContactId] || '';
}

function withPendingCounts() {
  return state.groups.map((group) => ({
    ...group,
    pendingCount: group.contacts.filter((c) => !state.selectedContactIds.has(c.id) && hasPhoneOrEmail(c)).length,
  }));
}

function sortedGroups() {
  return withPendingCounts().sort((a, b) => {
    if (b.pendingCount !== a.pendingCount) return b.pendingCount - a.pendingCount;
    return a.name.localeCompare(b.name);
  });
}

function contactById(id) {
  for (const group of state.groups) {
    const found = group.contacts.find((c) => c.id === id);
    if (found) return found;
  }
  return null;
}

function renderEvidenceRows(contact) {
  const entries = Array.isArray(contact.evidence) && contact.evidence.length
    ? contact.evidence
    : [];
  if (!entries.length) return ['No evidence entries'];

  return entries.map((entry) => {
    const source = t(entry.source_url);
    const type = t(entry.source_type);
    const quote = t(entry.quote_or_fact);
    if (source && type && quote) return `${quote} — ${source} (${type})`;
    if (source && quote) return `${quote} — ${source}`;
    return source || quote || 'Evidence entry';
  });
}

function renderRight(contactId) {
  const c = contactById(contactId);
  if (!c) {
    emptyState.classList.remove('hidden');
    contactPanel.classList.add('hidden');
    notesText.value = '';
    return;
  }

  emptyState.classList.add('hidden');
  contactPanel.classList.remove('hidden');

  contactName.textContent = c.fullName;
  contactMeta.textContent = `${c.title} · ${c.company}`;
  const hotelValue = c.hotelUrl || c.hotelName;
  const hotelHref = hotelLink(hotelValue);
  contactHotel.textContent = hotelValue || '—';
  if (hotelHref) {
    contactHotel.href = hotelHref;
  } else {
    contactHotel.removeAttribute('href');
  }
  contactTitle.textContent = c.title;
  primaryHandle.textContent = c.primary;
  phones.textContent = uniq([c.phone, c.phone2]).join(' · ') || '—';
  emails.textContent = uniq([c.email, c.email2]).join(' · ') || '—';
  otherDetail.textContent = c.otherContactDetail || '—';
  fitReason.textContent = c.fitReason || '—';
  evidenceSummary.textContent = c.evidenceSummary || '—';

  if (c.linkedin) {
    linkedinLink.href = c.linkedin;
    linkedinLink.textContent = c.linkedin;
  } else {
    linkedinLink.removeAttribute('href');
    linkedinLink.textContent = 'not set';
  }

  evidenceList.innerHTML = '';
  for (const row of renderEvidenceRows(c)) {
    const li = document.createElement('li');
    li.textContent = row;
    evidenceList.appendChild(li);
  }

  sourcePayload.textContent = JSON.stringify(
    {
      full_name: c.fullName,
      title: c.title,
      company: c.company,
      hotel: c.hotelName,
      linkedin_url: c.linkedin || undefined,
      phones: uniq([c.phone, c.phone2]),
      emails: uniq([c.email, c.email2]),
      other_contact_detail: c.otherContactDetail || undefined,
      fit_reason: c.fitReason || undefined,
      contact_evidence_summary: c.evidenceSummary || undefined,
      evidence: c.evidence,
    },
    null,
    2,
  );

  markBtn.textContent = state.selectedContactIds.has(c.id) ? 'Undo done' : 'Mark done';
  loadNotesForActive();
}

function makeContactRow(contact) {
  const isSelected = state.selectedContactIds.has(contact.id);
  const row = document.createElement('button');
  row.type = 'button';
  row.dataset.contactId = contact.id;
  row.className = `contact-row${isSelected ? ' selected' : ''}${state.activeContactId === contact.id ? ' active' : ''}`;
  row.innerHTML = `
    <span class="contact-main">
      <span class="contact-name">${contact.fullName}</span>
      <span class="contact-title-text">${contact.title}</span>
      <span class="contact-handle">${contact.primary}</span>
    </span>
    <button class="mark-toggle" type="button" data-contact-id="${contact.id}">${isSelected ? 'Done' : 'Mark'}</button>
  `;
  return row;
}

function makeGroupBlock(group, open) {
  const details = document.createElement('details');
  details.className = 'hotel-block';
  details.open = open;
  details.dataset.hotelId = group.id;

  const summary = document.createElement('summary');
  summary.innerHTML = `
    <span class="hotel-title">
      <span>${group.name}</span>
      <span>${group.pendingCount} pending</span>
    </span>
    <span class="hotel-meta">
      <span>${group.contacts.length} contacts</span>
      <span>${group.sourceUrl || '—'}</span>
    </span>
  `;

  const body = document.createElement('div');
  body.className = 'contact-list';
  const orderedContacts = orderedContactsInGroup(group);
  for (const contact of orderedContacts) {
    body.appendChild(makeContactRow(contact));
  }

  details.appendChild(summary);
  details.appendChild(body);
  return details;
}

function renderLeft() {
  const groups = sortedGroups();
  if (!groups.length) {
    hotelsRoot.innerHTML = '<p class="muted">No contacts found in sample.</p>';
    summaryText.textContent = 'No data';
    state.activeContactId = null;
    renderRight(null);
    return;
  }

  const total = groups.reduce((sum, g) => sum + g.contacts.length, 0);
  const pending = groups.reduce((sum, g) => sum + g.pendingCount, 0);
  const marked = total - pending;
  summaryText.textContent = `Groups: ${groups.length} · Contacts: ${total} · Pending: ${pending} · Marked: ${marked}`;

  hotelsRoot.innerHTML = '';
  groups.forEach((group, idx) => {
    const open = state.openHotels.has(group.id) || (state.openHotels.size === 0 && idx === 0);
    hotelsRoot.appendChild(makeGroupBlock(group, open));
  });

  if (!state.activeContactId || !contactById(state.activeContactId)) {
    const allContacts = groups.flatMap((g) => g.contacts);
    const fallback = allContacts.find((c) => !state.selectedContactIds.has(c.id) && hasPhoneOrEmail(c)) || allContacts[0];
    state.activeContactId = fallback ? fallback.id : null;
  }

  document.querySelectorAll('.contact-row').forEach((row) => {
    row.classList.toggle('active', row.dataset.contactId === state.activeContactId);
  });

  renderRight(state.activeContactId);
}

function handleNotesSave() {
  if (!state.activeContactId) return;
  state.contactNotes[state.activeContactId] = notesText.value.trim();
  saveNotes();
}

function handleNotesDiscard() {
  loadNotesForActive();
}

function markOrUnmark(contactId) {
  if (state.selectedContactIds.has(contactId)) {
    state.selectedContactIds.delete(contactId);
  } else {
    state.selectedContactIds.add(contactId);
    const nextId = nextActiveAfterMark(contactId);
    if (nextId) {
      state.activeContactId = nextId;
    }
  }
  saveSelection();
  renderLeft();
}

function nextActiveAfterMark(contactId) {
  const groups = sortedGroups();
  for (let gi = 0; gi < groups.length; gi += 1) {
    const group = groups[gi];
    const ordered = orderedContactsInGroup(group);
    const currentIndex = ordered.findIndex((contact) => contact.id === contactId);

    if (currentIndex === -1) {
      continue;
    }

    for (let ci = currentIndex + 1; ci < ordered.length; ci += 1) {
      if (!state.selectedContactIds.has(ordered[ci].id)) {
        return ordered[ci].id;
      }
    }

    for (let gj = gi + 1; gj < groups.length; gj += 1) {
      const candidate = orderedContactsInGroup(groups[gj]).find((contact) => !state.selectedContactIds.has(contact.id));
      if (candidate) {
        return candidate.id;
      }
    }
  }

  const unmarkedFallback = groups
    .flatMap((group) => orderedContactsInGroup(group))
    .find((contact) => !state.selectedContactIds.has(contact.id));

  if (unmarkedFallback) {
    return unmarkedFallback.id;
  }

  return null;
}

function handleLeftClick(event) {
  const mark = event.target.closest('.mark-toggle[data-contact-id]');
  if (mark) {
    event.preventDefault();
    event.stopPropagation();
    markOrUnmark(mark.dataset.contactId);
    return;
  }

  const row = event.target.closest('.contact-row[data-contact-id]');
  if (!row) return;

  state.activeContactId = row.dataset.contactId;
  renderLeft();
}

function handleDetailsToggle(event) {
  const details = event.target.closest('details.hotel-block');
  if (!details || !details.dataset.hotelId) return;
  if (details.open) {
    state.openHotels.add(details.dataset.hotelId);
  } else {
    state.openHotels.delete(details.dataset.hotelId);
  }
}

function fallbackPayload() {
  return {
    target_url: 'https://ampersandhotel.com/',
    contacts: [
      {
        full_name: 'Niels Fr\u00f8strup',
        title: 'Assistant General Manager',
        company: 'The Ampersand Hotel',
        linkedin_url: 'https://uk.linkedin.com/in/niels-fr%C3%B8strup-aa67b862',
        email: null,
        other_contact_detail: '+44 (0)20 7589 5895',
        fit_reason: 'Fallback sample contact from local source.',
        contact_evidence_summary: 'Loaded from fallback source in case primary file has no contacts.',
        evidence: [
          {
            source_url: 'https://ampersandhotel.com/contact/',
            source_type: 'official_site',
            quote_or_fact: 'Fallback source for display only.',
          },
        ],
      },
    ],
  };
}

async function loadSource() {
  const response = await fetch(DATA_URL);
  if (!response.ok) throw new Error('bad fetch');
  return response.json();
}

async function init() {
  state.selectedContactIds = new Set(readSelection());
  state.contactNotes = readNotes();
  try {
    const payload = await loadSource();
    state.groups = normalize(payload);
    if (!state.groups.length) {
      state.groups = normalize(fallbackPayload());
    }
  } catch (_e) {
    state.groups = normalize(fallbackPayload());
  }

  hotelsRoot.addEventListener('click', handleLeftClick);
  hotelsRoot.addEventListener('toggle', handleDetailsToggle, true);
  markBtn.addEventListener('click', () => {
    if (state.activeContactId) markOrUnmark(state.activeContactId);
  });
  notesSaveBtn.addEventListener('click', handleNotesSave);
  notesDiscardBtn.addEventListener('click', handleNotesDiscard);

  renderLeft();
}

init();
