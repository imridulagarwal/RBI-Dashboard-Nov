async function fetchJSON(path){ const r=await fetch(path); if(!r.ok) throw new Error(path); return r.json(); }

async function fetchIndex(){ return fetchJSON('data/index.json'); }
async function fetchBanks(){ return fetchJSON('data/banks.json'); }
async function fetchStats(year,month){
  const path=`data/${year}-${String(month).padStart(2,'0')}.json`;
  return fetchJSON(path); // [{bank_id, year, month, credit/debit/values...}]
}

function populate(select, list, getV, getT){
  list.forEach(item=>{
    const o=document.createElement('option');
    o.value=getV(item); o.textContent=getT(item);
    select.appendChild(o);
  });
}

function uniqueSorted(arr){ return Array.from(new Set(arr)).sort((a,b)=>a-b); }

function buildCharts(stats){
  stats.sort((a,b)=> a.year!==b.year ? a.year-b.year : a.month-b.month);
  const labels=stats.map(s=>`${s.year}-${String(s.month).padStart(2,'0')}`);
  const credit=stats.map(s=>s.credit_cards_outstanding||0);
  const debit=stats.map(s=>s.debit_cards_outstanding||0);
  const ccVal=stats.map(s=>(s.cc_pos_value||0));
  const dcVal=stats.map(s=>(s.dc_pos_value||0));

  if(window.outstandingChart) window.outstandingChart.destroy();
  if(window.transactionChart) window.transactionChart.destroy();

  const ctx1=document.getElementById('outstanding-chart').getContext('2d');
  window.outstandingChart=new Chart(ctx1,{type:'line',data:{
    labels,datasets:[
      {label:'Credit Cards Outstanding',data:credit,fill:true},
      {label:'Debit Cards Outstanding',data:debit,fill:true}
    ]},
    options:{responsive:true,interaction:{mode:'index',intersect:false}}
  });

  const ctx2=document.getElementById('transaction-chart').getContext('2d');
  window.transactionChart=new Chart(ctx2,{type:'bar',data:{
    labels,datasets:[
      {label:"Credit Card Txn Value (Rs '000)",data:ccVal},
      {label:"Debit Card Txn Value (Rs '000)",data:dcVal}
    ]},
    options:{responsive:true,interaction:{mode:'index',intersect:false}}
  });
}

async function init(){
  const bankSel=document.getElementById('bank-select');
  const yearSel=document.getElementById('year-select');

  // load bank list + months index
  const [banks,index]=await Promise.all([fetchBanks(),fetchIndex()]);
  populate(bankSel,banks,b=>b.id,b=>b.name);

  const years=uniqueSorted(index.map(x=>x.year));
  populate(yearSel,years,y=>y,y=>y);

  document.getElementById('load-button').addEventListener('click', async ()=>{
    const bankId = Number(bankSel.value || 0);
    const year   = Number(yearSel.value || 0);
    const months = index.filter(x => !year || x.year===year);

    // fetch all selected months, then optionally filter by bank
    const all = (await Promise.all(months.map(m=>fetchStats(m.year,m.month)))).flat();
    const filtered = bankId ? all.filter(r=>r.bank_id===bankId) : all;
    buildCharts(filtered);
  });
}

document.addEventListener('DOMContentLoaded', init);
