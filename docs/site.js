// KioMQ site — nav, tabs, scrollspy, reveal, code copy + Rust highlighting.

// ponytail: regex highlighter, ~40 lines instead of a syntax-highlighting dep.
// Swap for Prism/Shiki if the code samples ever need real parsing.
const RUST_KEYWORDS =
  /\b(async|await|fn|let|mut|use|pub|struct|enum|impl|for|in|if|else|while|loop|match|return|move|const|static|type|where|dyn|as|crate|self|Self|true|false|Some|None|Ok|Err)\b/g;

function highlight(pre) {
  if (pre.dataset.plain !== undefined) return;
  // Each match is swapped for a private-use codepoint, so later passes cannot
  // re-match text that has already been tokenised.
  const slots = [];
  const stash = (cls, text) =>
    String.fromCharCode(0xe000 + slots.push(`<span class="${cls}">${text}</span>`) - 1);

  const marked = pre.textContent
    .replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c])
    // Order matters: comments and strings first, so keywords inside them stay untouched.
    .replace(/\/\/[^\n]*/g, (m) => stash("tok-cmt", m))
    .replace(/"(?:[^"\\]|\\.)*"/g, (m) => stash("tok-str", m))
    .replace(/#\[[^\]]*\]/g, (m) => stash("tok-mac", m))
    .replace(/\b[a-z_][a-z0-9_]*!/gi, (m) => stash("tok-mac", m))
    .replace(RUST_KEYWORDS, (m) => stash("tok-kw", m))
    .replace(/\b[A-Z][A-Za-z0-9_]*\b/g, (m) => stash("tok-ty", m))
    .replace(/\b\d[\d_]*(?:\.\d+)?(?:u\d+|i\d+|f\d+)?\b/g, (m) => stash("tok-num", m));

  pre.innerHTML = marked.replace(/[\uE000-\uF8FF]/g, (c) => slots[c.charCodeAt(0) - 0xe000]);
}

function addCopy(pre) {
  const wrap = document.createElement("div");
  wrap.className = "codewrap";
  pre.parentNode.insertBefore(wrap, pre);
  wrap.appendChild(pre);

  const source = pre.textContent;
  const btn = document.createElement("button");
  btn.className = "copy";
  btn.type = "button";
  btn.textContent = "Copy";
  btn.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(source);
      btn.textContent = "Copied";
    } catch {
      btn.textContent = "Press Cmd-C";
    }
    setTimeout(() => (btn.textContent = "Copy"), 1600);
  });
  wrap.appendChild(btn);
}

document.querySelectorAll("pre").forEach((pre) => {
  addCopy(pre);
  highlight(pre);
});

// Nav: background on scroll + mobile menu.
const nav = document.querySelector(".nav");
const navLinks = document.querySelector(".nav-links");
const navToggle = document.querySelector(".nav-toggle");
if (nav) {
  const onScroll = () => nav.classList.toggle("scrolled", window.scrollY > 8);
  onScroll();
  addEventListener("scroll", onScroll, { passive: true });
}
if (navToggle && navLinks) {
  navToggle.addEventListener("click", () => {
    const open = navLinks.classList.toggle("open");
    navToggle.setAttribute("aria-expanded", String(open));
  });
  navLinks.addEventListener("click", (e) => {
    if (e.target.tagName === "A") navLinks.classList.remove("open");
  });
}

// Code panel tabs.
document.querySelectorAll(".panel-tabs .tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    const panel = tab.closest(".panel");
    panel.querySelectorAll(".tab").forEach((t) => {
      t.classList.toggle("active", t === tab);
      t.setAttribute("aria-selected", String(t === tab));
    });
    panel.querySelectorAll(".panel-pane").forEach((p) => {
      p.classList.toggle("show", p.id === tab.getAttribute("aria-controls"));
    });
  });
});

// Reveal on scroll.
const reveals = document.querySelectorAll(".reveal");
if (reveals.length) {
  const io = new IntersectionObserver(
    (entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting) {
          e.target.classList.add("in");
          io.unobserve(e.target);
        }
      });
    },
    { rootMargin: "0px 0px -10% 0px" },
  );
  reveals.forEach((el) => io.observe(el));
}

// Docs sidebar scrollspy.
const tocLinks = [...document.querySelectorAll(".toc a")];
if (tocLinks.length) {
  const spy = new IntersectionObserver(
    (entries) => {
      entries.forEach((e) => {
        if (!e.isIntersecting) return;
        tocLinks.forEach((a) =>
          a.classList.toggle("active", a.getAttribute("href") === `#${e.target.id}`),
        );
      });
    },
    { rootMargin: "-96px 0px -70% 0px" },
  );
  tocLinks
    .map((a) => document.querySelector(a.getAttribute("href")))
    .filter(Boolean)
    .forEach((s) => spy.observe(s));
}
