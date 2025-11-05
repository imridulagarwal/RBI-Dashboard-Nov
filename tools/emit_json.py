import json, os, sys, re, calendar
import pandas as pd

# ---- Simple bank-name normalizer ----
def slug(s): return re.sub(r'[^A-Za-z0-9]','', str(s)).upper()

BANK_NAME_MAP = {
  "STATEBANKOFINDIA":"State Bank of India",
  "SBI":"State Bank of India",
  "STATEBANK":"State Bank of India",
  "BANKOFINDIA":"Bank of India",
  "BOI":"Bank of India",
}

def normalize_bank(name:str)->str:
  key = slug(name)
  return BANK_NAME_MAP.get(key, name.strip())

# ---- RBI Excel parser (multi-row header) ----
def parse_excel(path:str):
  raw = pd.read_excel(path, header=None)
  # build header names from rows 2..6, cols 3..28
  headers=[]
  for col in range(3,29):
    parts=[]
    for r in range(2,7):
      v=raw.iloc[r,col]
      if isinstance(v,str) and v.strip(): parts.append(v.strip())
    base = re.sub(r'[^A-Za-z0-9]+','_', ' '.join(parts).lower()).strip('_')
    # ensure uniqueness
    cand=base; i=1
    while cand in headers: cand=f"{base}_{i}"; i+=1
    headers.append(cand)
  # find start row (serial no appears in col 1)
  start=None
  for i,v in enumerate(raw[1]):
    if isinstance(v,(int,float)) and pd.notna(v): start=i; break
  if start is None: raise RuntimeError("Cannot find data start")
  # find end row (blank bank name or contains 'total')
  end=len(raw)
  for i in range(start,len(raw)):
    bank=raw.iloc[i,2]
    if pd.isna(bank) or (isinstance(bank,str) and 'total' in bank.lower()):
      end=i; break
  df = raw.iloc[start:end, 1:29]
  df.columns = ["serial_no","bank_name"]+headers
  for h in headers: df[h] = pd.to_numeric(df[h], errors="coerce")
  return df

def emit_month(xlsx_path:str, year:int, month:int):
  out_dir = os.path.join('docs','data')
  os.makedirs(out_dir, exist_ok=True)

  df = parse_excel(xlsx_path)

  # load/update banks
  banks_path = os.path.join(out_dir,'banks.json')
  banks = json.load(open(banks_path)) if os.path.exists(banks_path) else []
  name_to_id = {b['name']: b['id'] for b in banks}
  next_id = max([b['id'] for b in banks], default=0)+1

  rows=[]
  for _,r in df.iterrows():
    canon = normalize_bank(r['bank_name'])
    if canon not in name_to_id:
      name_to_id[canon]=next_id; banks.append({'id':next_id,'name':canon}); next_id+=1
    bank_id = name_to_id[canon]
    # combine CC value columns and DC value columns as “total values” used by the chart
    cc_val = float(r.get('value_in_rs_000',0) or 0) + float(r.get('value_in_rs_000_1',0) or 0) + float(r.get('value_in_rs_000_2',0) or 0)
    dc_val = float(r.get('value_in_rs_000_4',0) or 0) + float(r.get('value_in_rs_000_5',0) or 0) + float(r.get('value_in_rs_000_6',0) or 0)
    rows.append({
      'bank_id': bank_id,
      'year': year, 'month': month,
      'credit_cards_outstanding': float(r.get('credit_cards',0) or 0),
      'debit_cards_outstanding':  float(r.get('debit_cards',0) or 0),
      'cc_pos_value': cc_val,
      'dc_pos_value': dc_val,
    })

  # write month
  fn = f'{year}-{str(month).zfill(2)}.json'
  json.dump(rows, open(os.path.join(out_dir,fn),'w'))

  # write banks
  banks.sort(key=lambda x:x['name'])
  json.dump(banks, open(banks_path,'w'))

  # update index
  idx_path = os.path.join(out_dir,'index.json')
  index = json.load(open(idx_path)) if os.path.exists(idx_path) else []
  path=f'data/{fn}'
  if not any(x['path']==path for x in index):
    index.append({'year':year,'month':month,'path':path})
  index.sort(key=lambda x:(x['year'],x['month']))
  json.dump(index, open(idx_path,'w'))

if __name__=='__main__':
  # usage: python tools/emit_json.py <xlsx_path> <year> <month>
  emit_month(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
