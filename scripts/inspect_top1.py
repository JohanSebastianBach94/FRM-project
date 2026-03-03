import pandas as pd
from pathlib import Path
OUT=Path('analysis_outputs')
N=OUT/'bis_matches_normalized.csv'
TOP1=OUT/'bis_matches_top1_per_catalog.csv'
if not N.exists():
    print('MISSING_NORMALIZED', N)
    raise SystemExit(1)

df=pd.read_csv(N,dtype=str).fillna('')
# ensure score numeric
if 'score' not in df.columns:
    print('MISSING_SCORE_COLUMN')
    df['score']=0.0

df['score_num']=pd.to_numeric(df['score'],errors='coerce').fillna(0.0)
# top1 per catalog
top1 = df.sort_values(['catalog_series','score_num'],ascending=[True,False]).groupby('catalog_series',as_index=False).first()
TOP1.parent.mkdir(parents=True,exist_ok=True)
top1.to_csv(TOP1,index=False)

total_catalogs=top1['catalog_series'].nunique()
c_ge25 = (top1['score_num']>=2.5).sum()
c_ge20 = (top1['score_num']>=2.0).sum()
c_ge15 = (top1['score_num']>=1.5).sum()
c_lt15 = (top1['score_num']<1.5).sum()

print('TOTAL_CATALOGS',total_catalogs)
print('TOP1_GE_2.5',c_ge25)
print('TOP1_GE_2.0',c_ge20)
print('TOP1_GE_1.5',c_ge15)
print('TOP1_LT_1.5',c_lt15)

print('\nSAMPLE_LOW (top1 score < 2.0)')
low = top1[top1['score_num']<2.0].sort_values('score_num')
print(low[['catalog_series','catalog_entity','bis_series','score_num']].head(12).to_string(index=False))

print('\nSAMPLE_HIGH (top1 score >= 2.5)')
high = top1[top1['score_num']>=2.5].sort_values('score_num',ascending=False)
print(high[['catalog_series','catalog_entity','bis_series','score_num']].head(12).to_string(index=False))

# Write review files
low.to_csv(OUT/'bis_matches_review_low_top1.csv',index=False)
high.to_csv(OUT/'bis_matches_review_high_top1.csv',index=False)
print('\nWrote review CSVs:', OUT/'bis_matches_review_low_top1.csv', OUT/'bis_matches_review_high_top1.csv')
