const {chromium}=require('playwright');
const fs=require('fs');
(async()=>{
 const b=await chromium.launch({headless:true,executablePath:process.env.BROWSER_EXECUTABLE});
 try{
  const p=await b.newPage({viewport:{width:1600,height:1100}});
  await p.goto('http://localhost:8501',{waitUntil:'domcontentloaded',timeout:120000});
  const errors=[];p.on('pageerror',e=>errors.push(e.message));
  await p.getByText('Couverture des alertes par action',{exact:true}).waitFor({timeout:90000});
  for(const label of ['Daily Return','Daily Volatility','VWAP','MA20','MA50']){
   if(!await p.getByText(label,{exact:true}).count())throw Error('Missing metric '+label);
  }
  const symbols=['AAPL','MSFT','AMZN','GOOGL','META','NVDA','TSLA','NFLX','ADBE','CRM','ORCL','IBM','INTC','AMD','JPM','BAC','V','MA','KO','WMT'];
  for(const s of symbols){
   const action=p.getByTestId('stSelectbox').filter({has:p.getByText('Action',{exact:true})});
   await action.locator('[role=combobox]').fill(s);
   await p.getByRole('option',{name:s,exact:true}).click();
   await p.waitForFunction(()=>document.querySelectorAll('[data-testid="stMetric"]').length===5,{timeout:30000});
   await p.getByText('Action analysée : '+s,{exact:true}).waitFor({timeout:30000});
   const body=await p.locator('body').innerText();
   if(body.includes('Lecture de Gold temporairement indisponible')||body.includes('Traceback'))throw Error(s+' Gold read failed');
   if(!body.includes('PostgreSQL gold_minute / gold_daily'))throw Error(s+' wrong data source');
  }
  const select=p.getByTestId('stSelectbox').filter({has:p.getByText('Filtrer les alertes',{exact:true})});
  await select.locator('[role=combobox]').fill('MSFT');await p.getByRole('option',{name:'MSFT',exact:true}).click();
  await p.waitForTimeout(1000);
  await p.screenshot({path:'artifacts/ui/streamlit-all-symbols.png',fullPage:true});
  if(errors.length)throw Error(errors.join(';'));
  console.log('ALL_SYMBOLS_UI_OK 20 selections, five metric cards, PostgreSQL source and MSFT alert filter');
 }finally{await b.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
