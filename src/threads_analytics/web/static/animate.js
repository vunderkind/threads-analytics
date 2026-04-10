/*
 * threads-analytics motion helpers.
 *
 * - Count-up animation on metric values with [data-countup]
 * - Active-nav-link highlighting based on current path
 * - Intersection-observer-based entrance animations
 */

(function () {
  'use strict';

  const prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  // --------- Active nav link ---------
  function markActiveNav() {
    const path = window.location.pathname;
    document.querySelectorAll('.sidebar-nav a').forEach((a) => {
      const href = a.getAttribute('href');
      if (!href) return;
      // Exact match for "/", prefix match for others
      const isActive =
        (href === '/' && path === '/') ||
        (href !== '/' && path.startsWith(href));
      if (isActive) a.classList.add('active');
    });
  }

  // --------- Count-up on metric values ---------
  // Looks for elements with data-countup="<target>" data-countup-format="pct|multiple|raw"
  // Counts up from 0 to the target over ~800ms.
  function parseValue(raw) {
    if (raw === null || raw === undefined || raw === '—') return null;
    const cleaned = String(raw).replace(/[%×,\s]/g, '');
    const n = parseFloat(cleaned);
    return isNaN(n) ? null : n;
  }

  function formatValue(n, format, originalText) {
    if (format === 'pct') return n.toFixed(1) + '%';
    if (format === 'multiple') return n.toFixed(1) + '×';
    if (Number.isInteger(n)) return String(Math.round(n));
    return n.toFixed(2);
  }

  function countUp(el) {
    if (prefersReduced) return;
    const original = el.textContent.trim();
    const target = parseValue(original);
    if (target === null || target === 0) return;
    const format =
      original.includes('%') ? 'pct' :
      original.includes('×') ? 'multiple' : 'raw';

    const duration = 850;
    const start = performance.now();
    function tick(now) {
      const t = Math.min(1, (now - start) / duration);
      // Ease-out cubic
      const eased = 1 - Math.pow(1 - t, 3);
      el.textContent = formatValue(target * eased, format, original);
      if (t < 1) requestAnimationFrame(tick);
      else el.textContent = original;
    }
    requestAnimationFrame(tick);
  }

  function animateCountups() {
    document.querySelectorAll('.metric-value').forEach(countUp);
    document.querySelectorAll('.verdict-stat-value').forEach(countUp);
  }

  // --------- Entrance observer ---------
  // Add .reveal to any element to have it fade + rise when scrolled into view.
  function setupReveal() {
    if (prefersReduced || !('IntersectionObserver' in window)) return;
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((e) => {
          if (e.isIntersecting) {
            e.target.classList.add('revealed');
            observer.unobserve(e.target);
          }
        });
      },
      { threshold: 0.1, rootMargin: '0px 0px -60px 0px' }
    );
    document.querySelectorAll('.reveal').forEach((el) => observer.observe(el));
  }

  // --------- Boot ---------
  function boot() {
    markActiveNav();
    animateCountups();
    setupReveal();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
