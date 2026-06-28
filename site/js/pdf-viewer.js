import * as pdfjsLib from '../vendor/pdfjs/pdf.min.mjs';

pdfjsLib.GlobalWorkerOptions.workerSrc = '../vendor/pdfjs/pdf.worker.min.mjs';

const PDF_URL = 'assets/report.pdf';

const docsLink = document.getElementById('docs-link');
const modal = document.getElementById('pdf-modal');
const body = document.getElementById('pdf-modal-body');
const loading = document.getElementById('pdf-loading');
const closeBtn = document.getElementById('pdf-close');

if (docsLink && modal) {
  let pdfDoc = null;
  let rendering = false;

  const renderAllPages = async () => {
    if (!pdfDoc || rendering) return;
    rendering = true;
    body.querySelectorAll('canvas').forEach((c) => c.remove());
    loading.style.display = 'none';

    const dpr = window.devicePixelRatio || 1;
    const containerWidth = body.clientWidth - 32;

    for (let i = 1; i <= pdfDoc.numPages; i++) {
      const page = await pdfDoc.getPage(i);
      const baseViewport = page.getViewport({ scale: 1 });
      const fitScale = Math.min(containerWidth / baseViewport.width, 1.8);
      const renderViewport = page.getViewport({ scale: fitScale * dpr });

      const canvas = document.createElement('canvas');
      canvas.className = 'pdf-page-canvas';
      canvas.width = renderViewport.width;
      canvas.height = renderViewport.height;
      canvas.style.width = renderViewport.width / dpr + 'px';
      canvas.style.height = renderViewport.height / dpr + 'px';
      body.appendChild(canvas);

      const ctx = canvas.getContext('2d');
      await page.render({ canvasContext: ctx, viewport: renderViewport }).promise;
    }
    rendering = false;
  };

  const loadDocument = async () => {
    if (pdfDoc) {
      await renderAllPages();
      return;
    }
    try {
      pdfDoc = await pdfjsLib.getDocument(PDF_URL).promise;
      await renderAllPages();
    } catch (err) {
      loading.textContent = window.location.protocol === 'file:'
        ? 'This viewer needs to run from a local server, not a double-clicked file. From the site/ folder, run: python3 -m http.server 8000 - then open http://localhost:8000/'
        : 'Could not load the document. Try the download button instead.';
      loading.style.display = 'block';
    }
  };

  const openModal = () => {
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden';
    loadDocument();
  };

  const closeModal = () => {
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = '';
  };

  docsLink.addEventListener('click', (e) => {
    e.preventDefault();
    openModal();
  });
  closeBtn.addEventListener('click', closeModal);
  modal.addEventListener('click', (e) => {
    if (e.target === modal) closeModal();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && modal.classList.contains('open')) closeModal();
  });

  let resizeTimer;
  window.addEventListener('resize', () => {
    if (!modal.classList.contains('open') || !pdfDoc) return;
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(renderAllPages, 250);
  });
}
