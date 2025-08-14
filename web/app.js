async function loadData() {
  // If you serve /web via GitHub Pages and /data at repo root, this relative path works:
  const res = await fetch('data/latest.json', {cache: 'no-store'});
  if (!res.ok) throw new Error('Failed to load data/latest.json');
  return res.json();
}

function fmtTime(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    return d.toLocaleString('en-AU', { hour: '2-digit', minute: '2-digit', day: 'numeric', month: 'short' });
  } catch { return ''; }
}

function render(items, data) {
  const list = document.getElementById('list');
  list.innerHTML = '';
  items.forEach(it => {
    const div = document.createElement('article');
    div.className = 'card';

    const title = document.createElement('h3');
    const a = document.createElement('a');
    a.href = it.url; a.target = '_blank'; a.rel = 'noopener';
    a.textContent = it.title || '(untitled)';
    title.appendChild(a);

    const meta = document.createElement('div');
    meta.className = 'meta';
    const when = it.published_at ? ` • ${fmtTime(it.published_at)}` : '';
    meta.innerHTML = `<span class="badge">${it.source}</span>${when}`;

    const sum = document.createElement('p');
    sum.className = 'summary';
    sum.textContent = it.summary || '';

    div.appendChild(title);
    div.appendChild(meta);
    if (sum.textContent) div.appendChild(sum);
    list.appendChild(div);
  });

  const summary = document.getElementById('summary');
  summary.innerHTML = `<p><strong>${items.length}</strong> announcements found. Generated at <em>${new Date(data.generated_at).toLocaleString('en-AU')}</em>.</p>`;
}

function populateSources(items) {
  const sel = document.getElementById('sourceFilter');
  const sources = Array.from(new Set(items.map(i => i.source))).sort();
  sources.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s; opt.textContent = s;
    sel.appendChild(opt);
  });
}

(async function init() {
  const data = await loadData();
  const all = data.items || [];

  document.getElementById('subtitle').textContent = `Today: ${data.date} (${data.timezone.replace('tzfile(', '').replace(')', '')})`;

  populateSources(all);

  // Fuse for search (title + summary)
  const fuse = new Fuse(all, { includeScore: false, threshold: 0.3, keys: ['title', 'summary'] });

  const searchEl = document.getElementById('search');
  const sourceEl = document.getElementById('sourceFilter');

  function applyFilters() {
    const q = searchEl.value.trim();
    const s = sourceEl.value;
    let items = q ? fuse.search(q).map(x => x.item) : [...all];
    if (s) items = items.filter(i => i.source === s);
    render(items, data);
  }

  searchEl.addEventListener('input', applyFilters);
  sourceEl.addEventListener('change', applyFilters);

  render(all, data);
})();
