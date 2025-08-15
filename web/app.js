async function loadData() {
  // Since index.html is in /web, latest.json is at /web/data/latest.json
  const res = await fetch('data/latest.json', { cache: 'no-store' });
  if (!res.ok) throw new Error(`Failed to load data/latest.json (${res.status})`);
  return res.json();
}

function fmtTime(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    return d.toLocaleString('en-AU', {
      hour: '2-digit',
      minute: '2-digit',
      day: 'numeric',
      month: 'short'
    });
  } catch {
    return '';
  }
}

function populateSources(items) {
  const sel = document.getElementById('sourceFilter');
  const sources = Array.from(new Set(items.map(i => i.source))).sort();
  sources.forEach(s => {
    const opt = document.createElement('option');
    opt.value = s;
    opt.textContent = s;
    sel.appendChild(opt);
  });
}

function render(items, data) {
  const list = document.getElementById('list');
  list.innerHTML = '';

  // If backend reported errors, show them (collapsible)
  if (Array.isArray(data.errors) && data.errors.length) {
    const box = document.createElement('article');
    box.className = 'card error-card';
    const details = document.createElement('details');
    const summary = document.createElement('summary');
    summary.textContent = `Source/content errors (${data.errors.length})`;
    details.appendChild(summary);

    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(data.errors, null, 2);
    details.appendChild(pre);
    box.appendChild(details);
    list.appendChild(box);
  }

  if (!items || items.length === 0) {
    const empty = document.createElement('article');
    empty.className = 'card';
    empty.innerHTML = `
      <h3>No announcements found</h3>
      <p>Check that <code>data/latest.json</code> has items and that your workflow ran successfully.</p>
    `;
    list.appendChild(empty);
  } else {
    items.forEach(it => {
      const card = document.createElement('article');
      card.className = 'card';

      // Title
      const h3 = document.createElement('h3');
      const a = document.createElement('a');
      a.href = it.url;
      a.target = '_blank';
      a.rel = 'noopener';
      a.textContent = it.title || '(untitled)';
      h3.appendChild(a);
      card.appendChild(h3);

      // Meta
      const meta = document.createElement('div');
      meta.className = 'meta';
      const when = it.published_at ? ` • ${fmtTime(it.published_at)}` : '';
      meta.innerHTML = `<span class="badge">${it.source}</span>${when}`;
      card.appendChild(meta);

      // Content (prefer full content_html, else summary)
      if (it.content_html) {
        const details = document.createElement('details');
        const summary = document.createElement('summary');
        summary.textContent = 'Read here';
        details.appendChild(summary);

        const content = document.createElement('div');
        content.className = 'content';
        // Sanitize HTML before injecting
        content.innerHTML = DOMPurify.sanitize(it.content_html);
        details.appendChild(content);
        card.appendChild(details);
      } else if (it.summary) {
        const p = document.createElement('p');
        p.className = 'summary';
        p.textContent = it.summary;
        card.appendChild(p);
      }

      // Always link to original for attribution/canonical
      const out = document.createElement('p');
      out.innerHTML = `<a href="${it.url}" target="_blank" rel="noopener">Open original</a>`;
      card.appendChild(out);

      list.appendChild(card);
    });
  }

  // Top summary
  const s = document.getElementById('summary');
  const generated = data.generated_at ? new Date(data.generated_at).toLocaleString('en-AU') : 'n/a';
  s.innerHTML = `<p><strong>${items.length}</strong> announcements. Generated at <em>${generated}</em>.</p>`;
}

(async function init() {
  try {
    const data = await loadData();
    const all = Array.isArray(data.items) ? data.items : [];

    // Subtitle
    const tz = (data.timezone || '').toString().replace('tzfile(', '').replace(')', '');
    document.getElementById('subtitle').textContent = `Today: ${data.date}${tz ? ' (' + tz + ')' : ''}`;

    populateSources(all);

    // Build Fuse index including title, summary, and content text (if present)
    const indexable = all.map(item => ({
      ...item,
      // crude text-only version of content for searching (strip tags quickly)
      content_text: item.content_html ? item.content_html.replace(/<[^>]+>/g, ' ') : ''
    }));

    const fuse = new Fuse(indexable, {
      includeScore: false,
      threshold: 0.3,
      keys: ['title', 'summary', 'content_text']
    });

    const searchEl = document.getElementById('search');
    const sourceEl = document.getElementById('sourceFilter');

    function applyFilters() {
      const q = searchEl.value.trim();
      const src = sourceEl.value;
      let items = q ? fuse.search(q).map(r => r.item) : [...indexable];
      if (src) items = items.filter(i => i.source === src);
      render(items, data);
    }

    searchEl.addEventListener('input', applyFilters);
    sourceEl.addEventListener('change', applyFilters);

    render(indexable, data);
  } catch (err) {
    console.error(err);
    const list = document.getElementById('list');
    list.innerHTML = `
      <article class="card error-card">
        <h3>Failed to load data</h3>
        <p>${String(err)}</p>
        <p>Make sure <code>web/data/latest.json</code> exists in the deployed site.</p>
      </article>
    `;
  }
})();
