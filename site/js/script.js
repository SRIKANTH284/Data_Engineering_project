// Mobile nav toggle
const navToggle = document.getElementById('nav-toggle');
const mainNav = document.getElementById('main-nav');

navToggle.addEventListener('click', () => {
  const isOpen = mainNav.classList.toggle('open');
  navToggle.classList.toggle('open', isOpen);
  navToggle.setAttribute('aria-expanded', String(isOpen));
});

mainNav.querySelectorAll('a').forEach((link) => {
  link.addEventListener('click', () => {
    mainNav.classList.remove('open');
    navToggle.classList.remove('open');
    navToggle.setAttribute('aria-expanded', 'false');
  });
});

// Scroll-spy: highlight active nav link
const sections = document.querySelectorAll('main section[id]');
const navLinks = mainNav.querySelectorAll('a');

const observer = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        const id = entry.target.getAttribute('id');
        navLinks.forEach((link) => {
          link.classList.toggle('active', link.getAttribute('href') === `#${id}`);
        });
      }
    });
  },
  { rootMargin: '-40% 0px -50% 0px' }
);
sections.forEach((section) => observer.observe(section));

// Code tabs (batch.py / stream.py)
document.querySelectorAll('.code-tab-btn').forEach((btn) => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    btn.parentElement.querySelectorAll('.code-tab-btn').forEach((b) => b.classList.remove('active'));
    btn.classList.add('active');
    const tabsRoot = btn.closest('.code-tabs');
    tabsRoot.querySelectorAll('.code-panel').forEach((panel) => {
      panel.classList.toggle('active', panel.dataset.panel === tab);
    });
  });
});

// Simulate stream.py's real-time console output in the architecture diagram
const streamInputNode = document.getElementById('stream-input-node');
const streamPyNode = document.getElementById('stream-py-node');
const consoleLines = document.getElementById('console-lines');

if (streamInputNode && streamPyNode && consoleLines) {
  const sampleBatches = ["('A', 6)", "('B', 4)", "('C', 3)", "('A', 9)", "('B', 7)", "('C', 5)", "('A', 4)"];
  let batchNum = 0;
  let placeholderCleared = false;

  const runStreamCycle = () => {
    streamInputNode.classList.remove('node-pulse');
    void streamInputNode.offsetWidth;
    streamInputNode.classList.add('node-pulse');

    setTimeout(() => {
      streamPyNode.classList.remove('node-pulse');
      void streamPyNode.offsetWidth;
      streamPyNode.classList.add('node-pulse');
    }, 550);

    setTimeout(() => {
      if (!placeholderCleared) {
        consoleLines.innerHTML = '';
        placeholderCleared = true;
      }
      batchNum += 1;
      const line = document.createElement('div');
      line.className = 'console-line';
      line.textContent = `batch ${batchNum}: ${sampleBatches[(batchNum - 1) % sampleBatches.length]}`;
      consoleLines.appendChild(line);
      while (consoleLines.children.length > 3) {
        consoleLines.removeChild(consoleLines.firstChild);
      }
    }, 1100);
  };

  const architectureSection = document.getElementById('architecture');
  const streamCycleObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          runStreamCycle();
          setInterval(runStreamCycle, 2800);
          streamCycleObserver.unobserve(architectureSection);
        }
      });
    },
    { threshold: 0.3 }
  );
  if (architectureSection) streamCycleObserver.observe(architectureSection);
}

// Workflow timeline: animate once when scrolled into view
const timeline = document.getElementById('timeline');
if (timeline) {
  const timelineObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          timeline.classList.add('in-view');
          timelineObserver.unobserve(timeline);
        }
      });
    },
    { threshold: 0.4 }
  );
  timelineObserver.observe(timeline);
}

// Copy-to-clipboard for code blocks
document.querySelectorAll('.copy-btn').forEach((btn) => {
  btn.addEventListener('click', async () => {
    const code = btn.parentElement.querySelector('code');
    const text = code ? code.innerText : '';
    try {
      await navigator.clipboard.writeText(text);
    } catch (err) {
      const range = document.createRange();
      range.selectNodeContents(code);
      const selection = window.getSelection();
      selection.removeAllRanges();
      selection.addRange(range);
      document.execCommand('copy');
      selection.removeAllRanges();
    }
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(() => {
      btn.textContent = 'Copy';
      btn.classList.remove('copied');
    }, 1500);
  });
});
