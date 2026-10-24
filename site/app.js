// Renders the dashboard from the JSON written by `grainflow publish export`.

const pct = (v, digits = 2) =>
  v === null || v === undefined ? "–" : `${v >= 0 ? "+" : "−"}${Math.abs(v * 100).toFixed(digits)}%`;

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

async function load(name) {
  const response = await fetch(`data/${name}.json`, { cache: "no-store" });
  if (!response.ok) throw new Error(`${name}.json: ${response.status}`);
  return response.json();
}

function renderSummary(summary) {
  const s = summary.signal;
  document.getElementById("k-fell").textContent = pct(s.fell_avg);
  document.getElementById("k-fell-n").textContent = `avg next-week return · ${s.fell_weeks} weeks`;
  document.getElementById("k-rose").textContent = pct(s.rose_avg);
  document.getElementById("k-rose-n").textContent = `avg next-week return · ${s.rose_weeks} weeks`;
  document.getElementById("k-corr").textContent = s.correlation?.toFixed(2) ?? "–";
  document.getElementById("k-p").textContent =
    s.p_value === null ? "not enough data" : `t ≈ ${s.t_stat.toFixed(1)}, p ≈ ${s.p_value.toFixed(3)}`;
  document.getElementById("k-weeks").textContent = s.weeks;
  document.getElementById("k-range").textContent = `${s.first_week?.slice(0, 4)}–${s.last_week?.slice(0, 4)} growing seasons`;
  if (s.correlation !== null) document.getElementById("r2").textContent = (s.correlation ** 2).toFixed(2);

  const f = summary.freshness;
  const built = new Date(summary.generated_at);
  document.getElementById("freshness").textContent =
    `Rebuilt ${built.toLocaleString()} · prices through ${f.latest_price_date} · USDA week ending ${f.latest_crop_week}`;

  const quality = document.getElementById("quality");
  const pills = [
    ...Object.entries(summary.volumes)
      .filter(([name]) => name !== "quarantine")
      .map(([name, rows]) => `${name}: ${rows.toLocaleString()} rows`),
    `quarantined: ${summary.volumes.quarantine}`,
  ];
  if (summary.dbt) pills.push(`dbt: ${summary.dbt.models} models, ${summary.dbt.tests_passed}/${summary.dbt.tests} tests passed`);
  quality.replaceChildren(
    ...pills.map((text) => Object.assign(document.createElement("span"), { className: "pill", textContent: text })),
  );

  if (summary.video_url) {
    const embed = summary.video_url.replace("watch?v=", "embed/").replace("youtu.be/", "youtube.com/embed/")
      .replace("loom.com/share/", "loom.com/embed/");
    document.getElementById("video-frame").src = embed;
    document.getElementById("video").classList.remove("hidden");
    document.getElementById("video-link").classList.remove("hidden");
  }
}

function renderPriceChart(weekly) {
  const rows = weekly.filter((w) => w.corn_close !== null);
  new Chart(document.getElementById("price-chart"), {
    data: {
      labels: rows.map((w) => w.week_start),
      datasets: [
        {
          type: "line", label: "Corn close (¢/bu)", data: rows.map((w) => w.corn_close),
          borderColor: css("--corn"), borderWidth: 1.5, pointRadius: 0, yAxisID: "price", tension: 0.2,
        },
        {
          type: "line", label: "Good/excellent %", data: rows.map((w) => w.pct_good_excellent),
          borderColor: css("--leaf"), borderWidth: 1.5, pointRadius: 0, yAxisID: "rating", spanGaps: false,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { labels: { color: css("--muted"), boxWidth: 12 } } },
      scales: {
        x: { ticks: { color: css("--muted"), maxTicksLimit: 8, callback: (_, i) => rows[i].week_start.slice(0, 7) }, grid: { display: false } },
        price: { position: "left", ticks: { color: css("--corn") }, grid: { color: css("--line") } },
        rating: { position: "right", min: 0, max: 100, ticks: { color: css("--leaf") }, grid: { display: false } },
      },
    },
  });
}

function renderSignalChart(summary) {
  const s = summary.signal;
  new Chart(document.getElementById("signal-chart"), {
    type: "bar",
    data: {
      labels: ["Ratings fell", "Ratings rose"],
      datasets: [{
        data: [s.fell_avg * 100, s.rose_avg * 100],
        backgroundColor: [css("--leaf"), css("--red")], borderRadius: 6,
      }],
    },
    options: {
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => `${c.raw.toFixed(2)}%` } } },
      scales: {
        y: { ticks: { color: css("--muted"), callback: (v) => `${v}%` }, grid: { color: css("--line") } },
        x: { ticks: { color: css("--muted") }, grid: { display: false } },
      },
    },
  });
}

function renderStates(states) {
  if (!states.length) return;
  document.getElementById("states-sub").textContent = `USDA report for the week ending ${states[0].week_ending}.`;
  document.getElementById("states").replaceChildren(
    ...states.map((s) => {
      const tr = document.createElement("tr");
      const change = s.good_excellent_change;
      tr.innerHTML = `
        <td>${s.state_alpha}</td>
        <td><div class="bar"><span style="width:${s.pct_good_excellent}%"></span></div></td>
        <td class="num">${s.pct_good_excellent.toFixed(0)}</td>
        <td class="num ${change < 0 ? "down" : change > 0 ? "up" : ""}">${change === null ? "–" : (change > 0 ? "+" : "") + change.toFixed(0)}</td>`;
      return tr;
    }),
  );
}

(async () => {
  try {
    const [summary, weekly, states] = await Promise.all([load("summary"), load("weekly"), load("states")]);
    renderSummary(summary);
    renderPriceChart(weekly);
    renderSignalChart(summary);
    renderStates(states);
  } catch (error) {
    document.getElementById("freshness").textContent = `Could not load data: ${error.message}`;
  }
})();
