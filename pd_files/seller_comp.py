import pandas as pd
import pd_files.informed as informed
import cgi

form = cgi.FieldStorage()
file3 = form.getvalue('csv_file3')
comp = pd.read_csv(file3)
df_comp = pd.DataFrame(comp)

form2 = cgi.FieldStorage()
file4 = form.getvalue('csv_file4')
category = pd.read_csv(file4)
df_category = pd.DataFrame(category)

df_w_comp = pd.merge(informed.final, df_comp, left_on='BUYBOX_SELLER', right_on='COMP_SELLER_ID', how='left')
df_w_comp = pd.merge(df_category, df_w_comp, left_on = 'Category ID', right_on='CategoryID', how='right')
count = df_w_comp.groupby(['BUYBOX_SELLER']).size().reset_index(name='BUY_BOX_WINS')
#df_w_comp.loc[df_w_comp['BUYBOX_SELLER'] == df_w_comp['COMP_SELLER_ID'], 'TOTAL_WINS_HERE'] = 
#print(buy_box_winners)
df_w_comp = pd.merge(df_w_comp, count, left_on='BUYBOX_SELLER', right_on='BUYBOX_SELLER', how='left' )
#print(count)
#print(count['COMP_SELLER_ID'])
df_w_comp.drop(columns=['COMP_SELLER_ID','SELLER DOMAIN', 'CITY', 'STATE', 'ZIP', 'ADDRESS', 'COUNTRY'])

df_w_comp.loc[(df_w_comp['DeckFinalPrice'] - df_w_comp['STA'] - df_w_comp['EMO']) <= df_w_comp['Cost'], 'FINAL_COST'] = (df_w_comp['DeckFinalPrice'] - df_w_comp['STA'] - df_w_comp['EMO'])
df_w_comp.loc[(df_w_comp['DeckFinalPrice'] - df_w_comp['STA'] - df_w_comp['EMO']) > df_w_comp['Cost'], 'FINAL_COST'] = df_w_comp['Cost']
df_w_comp['COST'] = df_w_comp['FINAL_COST']
df_w_comp.loc[df_w_comp['Cost'] <= df_w_comp['FINAL_COST'], 'COST'] = df_w_comp['Cost']
df_w_comp.loc[df_w_comp['COST'] == 0, 'COST'] = df_w_comp['Cost']
df_w_comp.loc[df_w_comp['STRATEGY_ID'] == 0, 'STRATEGY_ID'] = ''
# updating strategy id
#df_w_comp.loc[df_w_comp['Brand'] == 'GE' & df_w_comp['STRATEGY_ID ' == ''], 'STRATEGY_ID'] = 
df_w_comp.drop(columns=['FINAL_COST', 'Cost', 'DeckFinalPrice', 'EMO', 'STA'])
df_w_comp = df_w_comp[['SKU','LISTING_TYPE','MARKETPLACE_ID','CURRENCY', 'MIN_PRICE', 'MAX_PRICE', 'CURRENT_PRICE',	'MANUAL_PRICE', 'ORIGINAL_PRICE', 
'MAP_PRICE','STRATEGY_ID',	'BUYBOX_PRICE',	'BUYBOX_SELLER','BUYBOX_WINNER', 'YOUR_SELLER_ID',	'DAYS_SINCE_BUYBOX','DATE_OF_LAST_SALE','Business Name', 'BUY_BOX_WINS', 'COST', 'PhysicalStock', 'Brand', 'Category Path', 'MAIN CATEGORY'
, 'ItemType' ]]
df_w_comp.to_csv('test.csv', index=False)