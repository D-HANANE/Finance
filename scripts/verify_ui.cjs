/* Browser integration tests against the local, simulated platform. */
const { chromium } = require('playwright');
const fs = require('fs');

(async () => {
  fs.mkdirSync('artifacts/ui', { recursive: true });
  const options = { headless: true };
  if (process.env.BROWSER_EXECUTABLE) options.executablePath = process.env.BROWSER_EXECUTABLE;
  const browser = await chromium.launch(options);
  try {
    const context = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
    const page = await context.newPage();
    const failures = [];
    page.on('pageerror', error => failures.push(error.message));
    await page.goto('http://127.0.0.1:8501', { waitUntil: 'domcontentloaded' });
    await page.getByText('Chandeliers en direct', { exact: true }).waitFor({ timeout: 60000 });
    await page.getByText('Journal des alertes détectées', { exact: true }).waitFor();
    await page.locator('[data-testid="stPlotlyChart"]').waitFor();
    await page.screenshot({ path: 'artifacts/ui/streamlit.png', fullPage: true });
    const text = await page.locator('body').innerText();
    if (text.includes('Traceback') || text.includes('psycopg2.errors')) throw new Error('Streamlit exception displayed');
    if (text.includes('Aucune alerte détectée.')) throw new Error('No alert actually visible in Streamlit');
    await page.getByText('Journal des alertes détectées', { exact: true }).evaluate(el => el.scrollIntoView({ block: 'start', behavior: 'instant' }));
    await page.screenshot({ path: 'artifacts/ui/streamlit-alerts.png' });
    await page.getByText('Corrélations des rendements minute', { exact: true }).waitFor();
    await page.getByText('Gold finalisé', { exact: true }).evaluate(el => el.scrollIntoView({ block: 'start', behavior: 'instant' }));
    await page.locator('[data-testid*="VegaLiteChart"] canvas, [data-testid*="VegaLiteChart"] svg').first().waitFor({ timeout: 30000 });
    await page.waitForTimeout(1500);
    await page.screenshot({ path: 'artifacts/ui/streamlit-gold.png' });
    const login = await context.request.post('http://127.0.0.1:3001/login', {
      data: { user: 'admin', password: process.env.GRAFANA_PASSWORD || 'local_demo_change_me' }
    });
    if (!login.ok()) throw new Error('Grafana login failed: ' + login.status());
    const response = await context.request.get('http://127.0.0.1:3001/api/dashboards/uid/finance-live');
    const data = await response.json();
    if (data.dashboard.refresh !== '5s' || data.dashboard.panels.length !== 3) throw new Error('Wrong dashboard provisioned');
    await page.goto('http://127.0.0.1:3001/d/finance-live', { waitUntil: 'domcontentloaded' });
    await page.getByText('AAPL · cours simulés', { exact: true }).waitFor({ timeout: 60000 });
    await page.getByText('MSFT · cours simulés', { exact: true }).waitFor();
    await page.getByText('NVDA · cours simulés', { exact: true }).waitFor();
    await page.waitForTimeout(7000);
    const grafanaText = await page.locator('body').innerText();
    if (grafanaText.includes('No data') || grafanaText.includes('Query error')) throw new Error('Grafana panels lack data');
    await page.screenshot({ path: 'artifacts/ui/grafana.png', fullPage: true });
    if (failures.length) throw new Error(failures.join('\n'));
    fs.writeFileSync('artifacts/ui/capture.json', JSON.stringify({ capturedAt: new Date().toISOString(),
      streamlit: 'http://127.0.0.1:8501', grafana: 'http://127.0.0.1:3001/d/finance-live' }, null, 2));
    console.log('UI_TEST_OK Streamlit chart and alerts visible; 3 Grafana candle panels, 5s refresh.');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
