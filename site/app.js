// Renders the page from the JSON written by `grainflow publish export`.

const DRAW_MS = 2400;
const reducedMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;

const pct = (v, digits = 2) =>
  v === null || v === undefined ? "–" : `${v >= 0 ? "+" : "−"}${Math.abs(v * 100).toFixed(digits)}%`;

// "Oct 24, 4:12 PM CDT" in the reader's own time zone.
const stamp = (iso) =>
  new Date(iso).toLocaleString([], { month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short" });

// "Oct 23" for a calendar date; parsed as local so it doesn't shift a day west of UTC.
const day = (iso) => {
  if (!iso) return "unknown";
  const [y, m, d] = iso.slice(0, 10).split("-").map(Number);
  return new Date(y, m - 1, d).toLocaleDateString([], { month: "short", day: "numeric" });
};

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const $ = (id) => document.getElementById(id);

async function load(name) {
  const response = await fetch(`data/${name}.json`, { cache: "no-store" });
  if (!response.ok) throw new Error(`${name}.json returned ${response.status}`);
  return response.json();
}

// Run `callback` once, the first time `element` scrolls into view.
function whenVisible(element, callback) {
  if (!("IntersectionObserver" in window)) return callback();
  const observer = new IntersectionObserver((entries) => {
    if (entries.some((e) => e.isIntersecting)) {
      observer.disconnect();
      callback();
    }
  }, { threshold: 0.25 });
  observer.observe(element);
}

// Count a number up from zero; `format` turns the running value into text.
function countUp(element, target, format, delay = 0) {
  if (target === null || target === undefined) return;
  if (reducedMotion) {
    element.textContent = format(target);
    return;
  }
  const duration = 1100;
  setTimeout(() => {
    const start = performance.now();
    const tick = (now) => {
      const t = Math.min(1, (now - start) / duration);
      element.textContent = format(target * (1 - (1 - t) ** 3));
      if (t < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }, delay);
}

function verdict(s) {
  if (s.fell_avg === null || s.rose_avg === null) return ["Not enough data yet", ""];
  const significant = s.p_value !== null && s.p_value < 0.05;
  if (s.fell_avg > s.rose_avg && significant) {
    return ["Yes, modestly.", "Corn tends to gain the week after ratings drop, and the gap is statistically significant."];
  }
  if (s.fell_avg > s.rose_avg) {
    return ["Leaning yes.", "Corn tends to gain after ratings drop, but the gap isn't statistically significant yet."];
  }
  return ["Not in this data.", "Corn hasn't gained more after ratings drops than after rises."];
}

function renderSummary(summary) {
  const s = summary.signal;
  const [headline, note] = verdict(s);
  $("verdict").firstChild.textContent = headline;
  $("verdict-note").textContent = note;

  const delay = reducedMotion ? 0 : DRAW_MS * 0.55;
  countUp($("k-fell"), s.fell_avg, (v) => pct(v), delay);
  countUp($("k-rose"), s.rose_avg, (v) => pct(v), delay + 120);
  countUp($("k-corr"), s.correlation, (v) => `${v < 0 ? "−" : ""}${Math.abs(v).toFixed(2)}`, delay + 240);
  countUp($("k-weeks"), s.weeks, (v) => Math.round(v).toLocaleString(), delay + 360);
  $("k-fell-n").textContent = `avg next-week return, ${s.fell_weeks} weeks`;
  $("k-rose-n").textContent = `avg next-week return, ${s.rose_weeks} weeks`;
  $("k-p").textContent = s.p_value === null ? "not enough data" : `t ≈ ${s.t_stat.toFixed(1)}, p ≈ ${s.p_value.toFixed(3)}`;
  $("k-range").textContent = `${s.first_week?.slice(0, 4)} to ${s.last_week?.slice(0, 4)} growing seasons`;
  if (s.correlation !== null) $("r2").textContent = Math.round(s.correlation ** 2 * 100);

  const f = summary.freshness;
  $("freshness").textContent =
    `Last updated ${stamp(summary.generated_at)} · prices through ${day(f.latest_price_date)} · ` +
    `USDA week ending ${day(f.latest_crop_week)}`;

  const rows = Object.entries(summary.volumes)
    .filter(([name]) => name !== "quarantine")
    .map(([name, count]) => [name.replace("_", " "), `${count.toLocaleString()} rows`]);
  rows.push(["quarantined", summary.volumes.quarantine.toLocaleString()]);
  if (summary.dbt) rows.push(["dbt tests passed", `${summary.dbt.tests_passed} of ${summary.dbt.tests}`]);
  $("quality").replaceChildren(
    ...rows.map(([label, value]) => {
      const li = document.createElement("li");
      li.innerHTML = `<span>${label}</span><span>${value}</span>`;
      if (label === "dbt tests passed" && summary.dbt.tests_passed === summary.dbt.tests) li.lastChild.classList.add("ok");
      return li;
    }),
  );

  if (summary.portfolio_url) {
    $("hub-link").href = summary.portfolio_url;
    $("hub-link").classList.remove("hidden");
  }

  if (summary.video_url) {
    const embed = summary.video_url.replace("watch?v=", "embed/").replace("youtu.be/", "youtube.com/embed/")
      .replace("loom.com/share/", "loom.com/embed/");
    $("video-frame").src = embed;
    $("video").classList.remove("hidden");
  }
}

// The hero: ten years of corn drawn left to right, with the weeks ratings fell
// popping in as the line reaches them.
function renderHero(weekly) {
  const svg = $("hero-svg");
  const figure = $("hero-chart");
  const rows = weekly.filter((w) => w.corn_close !== null);
  if (rows.length < 2) return;

  const draw = (animate) => {
    const { width, height } = svg.getBoundingClientRect();
    const pad = 6;
    const closes = rows.map((w) => w.corn_close);
    const lo = Math.min(...closes);
    const hi = Math.max(...closes);
    const x = (i) => (i / (rows.length - 1)) * width;
    const y = (v) => pad + (1 - (v - lo) / (hi - lo)) * (height - pad * 2 - 18);
    const points = rows.map((w, i) => `${x(i).toFixed(1)},${y(w.corn_close).toFixed(1)}`);
    const floor = height - 18;

    const years = [];
    rows.forEach((w, i) => {
      const year = w.week_start.slice(0, 4);
      if (i > 0 && year !== rows[i - 1].week_start.slice(0, 4) && x(i) < width - 30) years.push([x(i), year]);
    });
    const step = Math.ceil(years.length / (width < 600 ? 4 : 10));

    const fell = rows
      .map((w, i) => [w, i])
      .filter(([w]) => w.good_excellent_change !== null && w.good_excellent_change <= -1.5);

    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.innerHTML = `
      <defs><linearGradient id="kernel-fade" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0" stop-color="#f2b630" stop-opacity=".28"/><stop offset="1" stop-color="#f2b630" stop-opacity="0"/>
      </linearGradient></defs>
      ${years.filter((_, i) => i % step === 0).map(([px, year]) =>
        `<text class="year" x="${px}" y="${height - 2}" text-anchor="middle">${year}</text>`).join("")}
      <polygon class="area" points="0,${floor} ${points.join(" ")} ${width},${floor}"/>
      <polyline class="line" points="${points.join(" ")}"/>
      ${fell.map(([w, i]) => `<circle class="fell" r="3.4" cx="${x(i).toFixed(1)}" cy="${(y(w.corn_close) - 9).toFixed(1)}"
        style="--d:${((i / rows.length) * DRAW_MS).toFixed(0)}ms"/>`).join("")}`;

    const line = svg.querySelector(".line");
    if (!animate || reducedMotion) {
      figure.classList.add("drawn");
      return;
    }
    const length = line.getTotalLength();
    line.style.strokeDasharray = length;
    line.style.strokeDashoffset = length;
    line.getBoundingClientRect();
    line.style.transition = `stroke-dashoffset ${DRAW_MS}ms cubic-bezier(.45, .05, .35, 1)`;
    line.style.strokeDashoffset = 0;
    figure.classList.add("drawn");
  };

  draw(true);
  let timer;
  let lastWidth = svg.getBoundingClientRect().width;
  addEventListener("resize", () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      const width = svg.getBoundingClientRect().width;
      if (width !== lastWidth) {
        lastWidth = width;
        draw(false);
      }
    }, 150);
  });
}

function renderPriceChart(weekly) {
  const rows = weekly.filter((w) => w.corn_close !== null);
  new Chart($("price-chart"), {
    data: {
      labels: rows.map((w) => w.week_start),
      datasets: [
        {
          type: "line", label: "Corn close (¢/bu)", data: rows.map((w) => w.corn_close),
          borderColor: css("--corn"), borderWidth: 1.6, pointRadius: 0, yAxisID: "price", tension: 0.2,
        },
        {
          type: "line", label: "Good/excellent %", data: rows.map((w) => w.pct_good_excellent),
          borderColor: css("--leaf"), backgroundColor: css("--leaf-soft"), fill: "origin",
          borderWidth: 1.6, pointRadius: 0, yAxisID: "rating", spanGaps: false,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      animation: reducedMotion ? false : { duration: 1400, easing: "easeOutQuart" },
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { labels: { color: css("--muted"), boxWidth: 12 } } },
      scales: {
        x: { ticks: { color: css("--muted"), maxTicksLimit: 8, callback: (_, i) => rows[i].week_start.slice(0, 4) }, grid: { display: false } },
        price: { position: "left", ticks: { color: css("--corn") }, grid: { color: css("--line") } },
        rating: { position: "right", min: 0, max: 100, ticks: { color: css("--leaf") }, grid: { display: false } },
      },
    },
  });
}

function renderSignalChart(summary) {
  const s = summary.signal;
  new Chart($("signal-chart"), {
    type: "bar",
    data: {
      labels: [`Ratings fell (${s.fell_weeks} wks)`, `Ratings rose (${s.rose_weeks} wks)`],
      datasets: [{
        data: [s.fell_avg * 100, s.rose_avg * 100],
        backgroundColor: [css("--leaf"), css("--red")], borderRadius: 8, maxBarThickness: 90,
      }],
    },
    options: {
      maintainAspectRatio: false,
      animation: reducedMotion ? false : { duration: 1100, easing: "easeOutBack" },
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
  $("states-sub").textContent =
    `Corn rated good or excellent in the 18 USDA Crop Progress states, week ending ${states[0].week_ending}.`;
  const list = $("states");
  list.replaceChildren(
    ...states.map((s, i) => {
      const li = document.createElement("li");
      const change = s.good_excellent_change;
      const direction = change < 0 ? "down" : change > 0 ? "up" : "";
      li.innerHTML = `
        <b>${s.state_alpha}</b>
        <div class="track" aria-hidden="true"><span style="--w:${s.pct_good_excellent}%;--d:${i * 45}ms"></span></div>
        <span class="num">${s.pct_good_excellent.toFixed(0)}%</span>
        <span class="chg ${direction}">${change === null ? "–" : (change > 0 ? "+" : "") + change.toFixed(0)}</span>`;
      return li;
    }),
  );
  whenVisible(list, () => list.classList.add("grown"));
}

function setUpLightbox() {
  const dialog = $("lightbox");
  const image = dialog.querySelector("img");
  document.querySelectorAll(".shots button").forEach((button) => {
    button.addEventListener("click", () => {
      image.src = button.dataset.full;
      image.alt = button.querySelector("img").alt;
      dialog.showModal();
    });
  });
  dialog.addEventListener("click", () => dialog.close());
}

(async () => {
  setUpLightbox();
  if (window.Chart) Chart.defaults.font.family = '"IBM Plex Sans", system-ui, sans-serif';
  try {
    const [summary, weekly, states] = await Promise.all([load("summary"), load("weekly"), load("states")]);
    renderSummary(summary);
    renderHero(weekly);
    renderStates(states);
    whenVisible($("price-chart"), () => renderPriceChart(weekly));
    whenVisible($("signal-chart"), () => renderSignalChart(summary));
  } catch (error) {
    $("freshness").textContent = `Couldn't load today's data (${error.message}). Reload to try again.`;
  }
})();
