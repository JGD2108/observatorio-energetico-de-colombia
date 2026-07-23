(() => {
  const html = document.documentElement;
  const body = document.body;
  const languageButton = document.querySelector('[data-language-toggle]');
  const presentationButtons = document.querySelectorAll('[data-presentation-toggle]');
  const navToggle = document.querySelector('[data-nav-toggle]');
  const sideNav = document.querySelector('[data-side-nav]');
  const navLinks = [...document.querySelectorAll('[data-nav-link]')];
  const sections = [...document.querySelectorAll('main > section[id]')];
  const currentSectionLabel = document.querySelector('[data-current-section]');
  const backToTop = document.querySelector('[data-back-to-top]');
  const toast = document.querySelector('[data-toast]');
  let presentationIndex = 0;
  let activeSectionId = 'inicio';
  let toastTimer;

  const getSectionTitle = (section) => {
    if (!section) return '';
    const heading = section.querySelector('h2') || section.querySelector('h1');
    return heading?.innerText.trim().replace(/\s+/g, ' ') || section.id;
  };

  const showToast = (message) => {
    if (!toast) return;
    window.clearTimeout(toastTimer);
    toast.textContent = message;
    toast.classList.add('is-visible');
    toastTimer = window.setTimeout(() => toast.classList.remove('is-visible'), 1800);
  };

  const syncCurrentSectionLabel = () => {
    const activeSection = sections.find((section) => section.id === activeSectionId);
    if (currentSectionLabel && activeSection) currentSectionLabel.textContent = getSectionTitle(activeSection);
  };

  const setLanguage = (lang) => {
    html.dataset.lang = lang;
    html.lang = lang;
    localStorage.setItem('oe-language', lang);
    if (languageButton) {
      languageButton.textContent = lang === 'es' ? 'EN' : 'ES';
      languageButton.setAttribute('aria-label', lang === 'es' ? 'Cambiar a inglés' : 'Switch to Spanish');
    }
    navToggle?.setAttribute('aria-label', lang === 'es' ? 'Abrir navegación' : 'Open navigation');
    backToTop?.setAttribute('aria-label', lang === 'es' ? 'Volver al inicio' : 'Back to top');
    syncCurrentSectionLabel();
    if (body.classList.contains('presentation-mode')) updatePresentationMeta();
  };

  const updateProgress = () => {
    const top = window.scrollY;
    const height = document.documentElement.scrollHeight - window.innerHeight;
    const progress = height > 0 ? Math.min(100, (top / height) * 100) : 0;
    const bar = document.getElementById('reading-progress');
    if (bar) bar.style.width = `${progress}%`;
    backToTop?.classList.toggle('is-visible', top > window.innerHeight * 0.85);
  };

  const setActiveSection = (id) => {
    activeSectionId = id;
    navLinks.forEach((link) => {
      const isActive = link.getAttribute('href') === `#${id}`;
      link.classList.toggle('is-active', isActive);
      if (isActive) link.setAttribute('aria-current', 'true');
      else link.removeAttribute('aria-current');
    });
    syncCurrentSectionLabel();
  };

  const setMenuOpen = (open) => {
    sideNav?.classList.toggle('side-nav--open', open);
    navToggle?.setAttribute('aria-expanded', String(open));
    navToggle?.setAttribute(
      'aria-label',
      open
        ? (html.dataset.lang === 'es' ? 'Cerrar navegación' : 'Close navigation')
        : (html.dataset.lang === 'es' ? 'Abrir navegación' : 'Open navigation')
    );
  };

  const updatePresentationMeta = () => {
    const counter = document.querySelector('[data-presentation-counter]');
    const title = document.querySelector('[data-presentation-title]');
    const progress = document.querySelector('[data-presentation-progress]');
    const current = sections[presentationIndex];
    if (counter) counter.textContent = `${presentationIndex + 1} / ${sections.length}`;
    if (title && current) title.textContent = getSectionTitle(current);
    if (progress) progress.style.width = `${((presentationIndex + 1) / sections.length) * 100}%`;
    document.querySelector('[data-presentation-prev]')?.toggleAttribute('disabled', presentationIndex === 0);
    document.querySelector('[data-presentation-next]')?.toggleAttribute('disabled', presentationIndex === sections.length - 1);
  };

  const showPresentationSection = (index) => {
    if (!sections.length) return;
    presentationIndex = Math.max(0, Math.min(index, sections.length - 1));
    sections.forEach((section, i) => section.classList.toggle('presentation-current', i === presentationIndex));
    sections[presentationIndex].scrollIntoView({ behavior: 'auto', block: 'start' });
    updatePresentationMeta();
  };

  const togglePresentation = () => {
    const entering = !body.classList.contains('presentation-mode');
    body.classList.toggle('presentation-mode', entering);
    if (entering) {
      const visibleIndex = sections.findIndex((section) => {
        const rect = section.getBoundingClientRect();
        return rect.top <= window.innerHeight * 0.42 && rect.bottom >= window.innerHeight * 0.42;
      });
      showPresentationSection(visibleIndex >= 0 ? visibleIndex : 0);
      showToast(html.dataset.lang === 'es' ? 'Use ← → para navegar · Esc para salir' : 'Use ← → to navigate · Esc to exit');
    } else {
      const current = sections[presentationIndex];
      sections.forEach((section) => section.classList.remove('presentation-current'));
      current?.scrollIntoView({ behavior: 'auto', block: 'start' });
    }
  };

  setLanguage(localStorage.getItem('oe-language') || 'es');
  updateProgress();
  setActiveSection('inicio');

  const readableCopy = [...document.querySelectorAll('.section-header, .prose, .problem-card, .source-card, .finding, .result-panel')]
    .map((element) => element.textContent || '')
    .join(' ');
  const readingTime = Math.max(8, Math.round(readableCopy.trim().split(/\s+/).length / 210));
  document.querySelectorAll('[data-reading-time]').forEach((element) => { element.textContent = String(readingTime); });

  languageButton?.addEventListener('click', () => setLanguage(html.dataset.lang === 'es' ? 'en' : 'es'));
  window.addEventListener('scroll', updateProgress, { passive: true });
  backToTop?.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));

  const revealObserver = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        entry.target.classList.add('is-visible');
        revealObserver.unobserve(entry.target);
      }
    });
  }, { threshold: 0.09 });
  document.querySelectorAll('.reveal').forEach((element) => revealObserver.observe(element));

  const sectionObserver = new IntersectionObserver((entries) => {
    const visible = entries
      .filter((entry) => entry.isIntersecting)
      .sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];
    if (visible && !body.classList.contains('presentation-mode')) setActiveSection(visible.target.id);
  }, { rootMargin: '-18% 0px -64% 0px', threshold: [0, 0.1, 0.4] });
  sections.forEach((section) => sectionObserver.observe(section));

  document.querySelectorAll('.copy-code').forEach((button) => {
    button.addEventListener('click', async () => {
      const target = document.getElementById(button.dataset.copyTarget);
      const raw = target?.dataset.rawCode || '';
      try {
        await navigator.clipboard.writeText(raw);
        const original = button.innerHTML;
        button.textContent = html.dataset.lang === 'es' ? 'Copiado ✓' : 'Copied ✓';
        showToast(html.dataset.lang === 'es' ? 'Código copiado al portapapeles' : 'Code copied to clipboard');
        window.setTimeout(() => { button.innerHTML = original; }, 1400);
      } catch {
        showToast(html.dataset.lang === 'es' ? 'No fue posible copiar el código' : 'Unable to copy code');
      }
    });
  });

  navToggle?.addEventListener('click', () => setMenuOpen(!sideNav?.classList.contains('side-nav--open')));
  sideNav?.querySelectorAll('a').forEach((link) => link.addEventListener('click', () => setMenuOpen(false)));

  document.querySelectorAll('[data-source-filter]').forEach((button) => {
    button.setAttribute('aria-pressed', String(button.classList.contains('is-active')));
    button.addEventListener('click', () => {
      const filter = button.dataset.sourceFilter;
      document.querySelectorAll('[data-source-filter]').forEach((item) => {
        const isActive = item === button;
        item.classList.toggle('is-active', isActive);
        item.setAttribute('aria-pressed', String(isActive));
      });
      document.querySelectorAll('[data-source-category]').forEach((card) => {
        const shouldShow = filter === 'all' || card.dataset.sourceCategory === filter;
        card.classList.toggle('is-filtered-out', !shouldShow);
      });
      showToast(
        html.dataset.lang === 'es'
          ? `Filtro aplicado: ${button.innerText.trim()}`
          : `Filter applied: ${button.innerText.trim()}`
      );
    });
  });

  presentationButtons.forEach((button) => button.addEventListener('click', togglePresentation));
  document.querySelector('[data-presentation-prev]')?.addEventListener('click', () => showPresentationSection(presentationIndex - 1));
  document.querySelector('[data-presentation-next]')?.addEventListener('click', () => showPresentationSection(presentationIndex + 1));

  document.addEventListener('keydown', (event) => {
    if (!body.classList.contains('presentation-mode')) return;
    if (['ArrowRight', 'ArrowDown', 'PageDown', ' '].includes(event.key)) {
      event.preventDefault();
      showPresentationSection(presentationIndex + 1);
    }
    if (['ArrowLeft', 'ArrowUp', 'PageUp'].includes(event.key)) {
      event.preventDefault();
      showPresentationSection(presentationIndex - 1);
    }
    if (event.key === 'Home') {
      event.preventDefault();
      showPresentationSection(0);
    }
    if (event.key === 'End') {
      event.preventDefault();
      showPresentationSection(sections.length - 1);
    }
    if (event.key === 'Escape') togglePresentation();
  });

})();
