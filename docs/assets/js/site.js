/* KioMQ docs — progressive enhancements. No dependencies. */
(function () {
  "use strict";

  var root = document.documentElement;

  /* ---------------------------------------------------------------- theme */
  var themeBtn = document.querySelector("[data-theme-toggle]");
  if (themeBtn) {
    themeBtn.addEventListener("click", function () {
      var next = root.dataset.theme === "light" ? "dark" : "light";
      root.dataset.theme = next;
      try {
        localStorage.setItem("kiomq-theme", next);
      } catch (e) {
        /* storage disabled — theme still applies for this page */
      }
      themeBtn.setAttribute("aria-label", "Switch to " + (next === "light" ? "dark" : "light") + " theme");
    });
  }

  /* ----------------------------------------------------------- mobile nav */
  var navToggle = document.querySelector("[data-nav-toggle]");
  var nav = document.getElementById("site-nav");
  if (navToggle && nav) {
    var setNav = function (open) {
      nav.dataset.open = open ? "true" : "false";
      navToggle.setAttribute("aria-expanded", open ? "true" : "false");
    };
    navToggle.addEventListener("click", function () {
      setNav(nav.dataset.open !== "true");
    });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && nav.dataset.open === "true") {
        setNav(false);
        navToggle.focus();
      }
    });
    /* Tapping anywhere off the sheet closes it — on a phone the toggle is a
       small target to have to find again. */
    document.addEventListener("click", function (e) {
      if (nav.dataset.open !== "true") return;
      if (nav.contains(e.target) || navToggle.contains(e.target)) return;
      setNav(false);
    });
  }

  /* --------------------------------------------------- collapsible asides
     The docs sidebar and the table of contents ship `open` so that no-JS and
     desktop get them expanded. Here we collapse them at the widths where they
     stack above the article, and leave them alone once the reader has taken
     over the toggle themselves. */
  [
    { sel: ".docs-nav", query: "(max-width: 780px)" },
    { sel: ".docs-toc__disclosure", query: "(max-width: 1180px)" },
  ].forEach(function (spec) {
    var el = document.querySelector(spec.sel);
    if (!el || typeof window.matchMedia !== "function") return;
    var mq = window.matchMedia(spec.query);
    var touched = false;
    var managed = el.open;
    /* `toggle` is queued, not synchronous, so a flag can't tell our own writes
       from the reader's — compare against the last state we set instead. */
    el.addEventListener("toggle", function () {
      if (el.open !== managed) touched = true;
    });
    var apply = function () {
      if (touched) return;
      managed = !mq.matches;
      el.open = managed;
    };
    apply();
    if (typeof mq.addEventListener === "function") mq.addEventListener("change", apply);
    else if (typeof mq.addListener === "function") mq.addListener(apply);
  });

  /* --------------------------------------------------------- copy buttons */
  var COPY_ICON =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
  var DONE_ICON =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 6 9 17l-5-5"/></svg>';

  function attachCopy(el, getText) {
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "code-copy";
    btn.innerHTML = COPY_ICON;
    btn.setAttribute("aria-label", "Copy to clipboard");
    btn.addEventListener("click", function () {
      var text = getText();
      var done = function () {
        btn.innerHTML = DONE_ICON;
        btn.dataset.copied = "true";
        btn.setAttribute("aria-label", "Copied");
        setTimeout(function () {
          btn.innerHTML = COPY_ICON;
          delete btn.dataset.copied;
          btn.setAttribute("aria-label", "Copy to clipboard");
        }, 1600);
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done, function () {});
        return;
      }
      var ta = document.createElement("textarea");
      ta.value = text;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand("copy");
        done();
      } catch (e) {
        /* nothing we can do — leave the icon untouched */
      }
      document.body.removeChild(ta);
    });
    el.appendChild(btn);
  }

  document.querySelectorAll(".highlight").forEach(function (block) {
    attachCopy(block, function () {
      var code = block.querySelector("code");
      return code ? code.innerText.replace(/\n$/, "") : "";
    });
  });

  document.querySelectorAll("[data-copy-text]").forEach(function (el) {
    attachCopy(el, function () {
      return el.dataset.copyText;
    });
  });

  /* ----------------------------------------------------------- code tabs */
  document.querySelectorAll("[data-tabs]").forEach(function (group) {
    var tabs = Array.prototype.slice.call(group.querySelectorAll('[role="tab"]'));
    var select = function (idx) {
      tabs.forEach(function (tab, i) {
        var selected = i === idx;
        tab.setAttribute("aria-selected", selected ? "true" : "false");
        tab.tabIndex = selected ? 0 : -1;
        var panel = document.getElementById(tab.getAttribute("aria-controls"));
        if (panel) panel.hidden = !selected;
      });
    };
    tabs.forEach(function (tab, i) {
      tab.addEventListener("click", function () {
        select(i);
      });
      tab.addEventListener("keydown", function (e) {
        var delta = e.key === "ArrowRight" ? 1 : e.key === "ArrowLeft" ? -1 : 0;
        if (!delta) return;
        e.preventDefault();
        var next = (i + delta + tabs.length) % tabs.length;
        select(next);
        tabs[next].focus();
      });
    });
  });

  /* ------------------------------------------------------- toc scrollspy */
  var tocLinks = Array.prototype.slice.call(document.querySelectorAll(".docs-toc a[href^='#']"));
  if (tocLinks.length && "IntersectionObserver" in window) {
    var byId = {};
    var headings = [];
    tocLinks.forEach(function (link) {
      var id = decodeURIComponent(link.getAttribute("href").slice(1));
      var heading = document.getElementById(id);
      if (heading) {
        byId[id] = link;
        headings.push(heading);
      }
    });
    var visible = new Set();
    var setActive = function () {
      var current = null;
      for (var i = 0; i < headings.length; i++) {
        if (visible.has(headings[i].id)) {
          current = headings[i].id;
          break;
        }
      }
      tocLinks.forEach(function (l) {
        l.classList.remove("is-active");
      });
      if (current && byId[current]) byId[current].classList.add("is-active");
    };
    var observer = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) visible.add(entry.target.id);
          else visible.delete(entry.target.id);
        });
        setActive();
      },
      { rootMargin: "-72px 0px -70% 0px", threshold: 0 }
    );
    headings.forEach(function (h) {
      observer.observe(h);
    });
  }

  /* -------------------------------------------------------------- search */
  var dialog = document.getElementById("search-dialog");
  var openers = document.querySelectorAll("[data-search-open]");
  if (dialog && openers.length && typeof dialog.showModal === "function") {
    var input = dialog.querySelector("input");
    var list = dialog.querySelector(".search-results");
    var empty = dialog.querySelector(".search-empty");
    var index = null;
    var loading = false;
    var cursor = -1;
    var current = [];

    var load = function () {
      if (index || loading) return;
      loading = true;
      fetch(dialog.dataset.searchIndex)
        .then(function (r) {
          return r.json();
        })
        .then(function (data) {
          index = data;
          loading = false;
          if (input.value) render(input.value);
        })
        .catch(function () {
          loading = false;
          empty.textContent = "Search index unavailable.";
          empty.hidden = false;
        });
    };

    var score = function (page, terms) {
      var title = page.title.toLowerCase();
      var section = (page.section || "").toLowerCase();
      var body = page.body.toLowerCase();
      var total = 0;
      for (var i = 0; i < terms.length; i++) {
        var t = terms[i];
        var hit = 0;
        if (title.indexOf(t) === 0) hit += 12;
        else if (title.indexOf(t) > -1) hit += 8;
        if (section.indexOf(t) > -1) hit += 3;
        var at = body.indexOf(t);
        if (at > -1) hit += 2;
        if (!hit) return 0;
        total += hit;
      }
      return total;
    };

    var excerpt = function (page, term) {
      var body = page.body;
      var at = body.toLowerCase().indexOf(term);
      if (at < 0) return page.summary || body.slice(0, 110);
      var start = Math.max(0, at - 40);
      return (start ? "…" : "") + body.slice(start, start + 130).trim() + "…";
    };

    var render = function (query) {
      var terms = query.toLowerCase().split(/\s+/).filter(Boolean);
      list.innerHTML = "";
      cursor = -1;
      current = [];
      if (!terms.length || !index) {
        empty.hidden = !!terms.length;
        empty.textContent = index ? "Type to search the docs." : "Loading index…";
        return;
      }
      var ranked = index
        .map(function (page) {
          return { page: page, score: score(page, terms) };
        })
        .filter(function (r) {
          return r.score > 0;
        })
        .sort(function (a, b) {
          return b.score - a.score;
        })
        .slice(0, 8);

      if (!ranked.length) {
        empty.hidden = false;
        empty.textContent = "No matches for “" + query + "”.";
        return;
      }
      empty.hidden = true;
      ranked.forEach(function (r) {
        var li = document.createElement("li");
        var a = document.createElement("a");
        a.href = r.page.url;
        var strong = document.createElement("strong");
        strong.textContent = r.page.section ? r.page.section + " › " + r.page.title : r.page.title;
        var span = document.createElement("span");
        span.textContent = excerpt(r.page, terms[0]);
        a.appendChild(strong);
        a.appendChild(span);
        li.appendChild(a);
        list.appendChild(li);
        current.push(li);
      });
    };

    var move = function (delta) {
      if (!current.length) return;
      if (cursor > -1) current[cursor].removeAttribute("aria-selected");
      cursor = (cursor + delta + current.length) % current.length;
      current[cursor].setAttribute("aria-selected", "true");
      current[cursor].scrollIntoView({ block: "nearest" });
    };

    var open = function () {
      load();
      dialog.showModal();
      input.value = "";
      render("");
      input.focus();
    };

    openers.forEach(function (btn) {
      btn.addEventListener("click", open);
    });

    document.addEventListener("keydown", function (e) {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        if (dialog.open) dialog.close();
        else open();
        return;
      }
      if (e.key === "/" && !dialog.open) {
        var tag = (document.activeElement && document.activeElement.tagName) || "";
        if (tag === "INPUT" || tag === "TEXTAREA" || document.activeElement.isContentEditable) return;
        e.preventDefault();
        open();
      }
    });

    input.addEventListener("input", function () {
      render(input.value);
    });

    dialog.addEventListener("keydown", function (e) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        move(1);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        move(-1);
      } else if (e.key === "Enter" && cursor > -1) {
        e.preventDefault();
        var link = current[cursor].querySelector("a");
        if (link) window.location.href = link.href;
      }
    });

    dialog.addEventListener("click", function (e) {
      if (e.target === dialog) dialog.close();
    });
  }
})();
