(() => {
  const html = document.documentElement;
  const languageButton = document.querySelector('[data-language-toggle]');
  const presentationButtons = document.querySelectorAll('[data-presentation-toggle]');
  const navToggle = document.querySelector('[data-nav-toggle]');
  const sideNav = document.querySelector('[data-side-nav]');
  const sections = [...document.querySelectorAll('main > section[id]')];
  let presentationIndex = 0;

  const setLanguage = (lang) => {
    html.dataset.lang = lang;
    html.lang = lang;
    localStorage.setItem('oe-language', lang);
    if (languageButton) languageButton.textContent = lang === 'es' ? 'EN' : 'ES';
  };

  setLanguage(localStorage.getItem('oe-language') || 'es');
  languageButton?.addEventListener('click', () => setLanguage(html.dataset.lang === 'es' ? 'en' : 'es'));

  const updateProgress = () => {
    const top = window.scrollY;
    const height = document.documentElement.scrollHeight - window.innerHeight;
    const progress = height > 0 ? Math.min(100, (top / height) * 100) : 0;
    const bar = document.getElementById('reading-progress');
    if (bar) bar.style.width = `${progress}%`;
  };
  updateProgress();
  window.addEventListener('scroll', updateProgress, { passive: true });

  const observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) entry.target.classList.add('is-visible');
    });
  }, { threshold: 0.12 });
  document.querySelectorAll('.reveal').forEach((el) => observer.observe(el));

  document.querySelectorAll('.copy-code').forEach((button) => {
    button.addEventListener('click', async () => {
      const target = document.getElementById(button.dataset.copyTarget);
      const raw = target?.dataset.rawCode || '';
      try {
        await navigator.clipboard.writeText(raw);
        const original = button.innerHTML;
        button.textContent = html.dataset.lang === 'es' ? 'Copiado' : 'Copied';
        setTimeout(() => { button.innerHTML = original; }, 1300);
      } catch {
        button.textContent = 'Error';
      }
    });
  });

  navToggle?.addEventListener('click', () => sideNav?.classList.toggle('side-nav--open'));
  sideNav?.querySelectorAll('a').forEach((link) => link.addEventListener('click', () => sideNav.classList.remove('side-nav--open')));

  const showPresentationSection = (index) => {
    if (!sections.length) return;
    presentationIndex = Math.max(0, Math.min(index, sections.length - 1));
    sections.forEach((section, i) => section.classList.toggle('presentation-current', i === presentationIndex));
    sections[presentationIndex].scrollIntoView({ behavior: 'instant', block: 'start' });
    const counter = document.querySelector('[data-presentation-counter]');
    if (counter) counter.textContent = `${presentationIndex + 1} / ${sections.length}`;
  };

  const togglePresentation = () => {
    const entering = !document.body.classList.contains('presentation-mode');
    document.body.classList.toggle('presentation-mode', entering);
    if (entering) {
      const visibleIndex = sections.findIndex((section) => {
        const rect = section.getBoundingClientRect();
        return rect.top <= window.innerHeight * 0.4 && rect.bottom >= window.innerHeight * 0.4;
      });
      showPresentationSection(visibleIndex >= 0 ? visibleIndex : 0);
    } else {
      sections.forEach((section) => section.classList.remove('presentation-current'));
    }
  };

  presentationButtons.forEach((button) => button.addEventListener('click', togglePresentation));
  document.querySelector('[data-presentation-prev]')?.addEventListener('click', () => showPresentationSection(presentationIndex - 1));
  document.querySelector('[data-presentation-next]')?.addEventListener('click', () => showPresentationSection(presentationIndex + 1));

  document.addEventListener('keydown', (event) => {
    if (!document.body.classList.contains('presentation-mode')) return;
    if (['ArrowRight', 'ArrowDown', 'PageDown', ' '].includes(event.key)) {
      event.preventDefault(); showPresentationSection(presentationIndex + 1);
    }
    if (['ArrowLeft', 'ArrowUp', 'PageUp'].includes(event.key)) {
      event.preventDefault(); showPresentationSection(presentationIndex - 1);
    }
    if (event.key === 'Escape') togglePresentation();
  });

  document.querySelectorAll('.diagram-zoom').forEach((button) => {
    button.addEventListener('click', () => {
      document.getElementById(button.dataset.diagramTarget)?.classList.toggle('mermaid-card--expanded');
    });
  });
})();
