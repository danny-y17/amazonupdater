import pandas as pd
import pd_files.foo as foo
import cgi

form = cgi.FieldStorage
file2 = form.getvalue('csv_file2')
buy_box = pd.read_csv(file2, encoding='latin1')
df_buy_box = pd.DataFrame(buy_box)

# add remote for buybox
df_buy_box[['RemoteId', 'shipping']] = df_buy_box['SKU'].str.split('-', 1, expand=True)

df_buy_box.columns = df_buy_box.columns.str.replace(' ','_')
foo.df['RemoteId'] = foo.df['RemoteId'].astype('string')
df_buy_box['RemoteId'] = df_buy_box['RemoteId'].astype('string')
new_df = pd.merge(foo.df, df_buy_box, left_on='RemoteId', right_on='RemoteId', how='inner')
new_df.loc[new_df['ground'] != 'LTL', 'MIN_PRICE'] = new_df['ground']
new_df.loc[new_df['local'] != 'ground', 'MIN_PRICE'] = new_df['local']
new_df.loc[new_df['green'] != 'ground', 'MIN_PRICE'] = new_df['green']
new_df.loc[new_df['east'] != 'ground', 'MIN_PRICE'] = new_df['east']
new_df.loc[new_df['west'] != 'ground', 'MIN_PRICE'] = new_df['west']

new_df.loc[new_df['ground'] != 'LTL', 'MAX_PRICE'] = round( (1.2 * (new_df['Cost'] + new_df['DimensionWeight'] + 15)) / 10) * 10 - 0.01
new_df.loc[new_df['local'] != 'ground', 'MAX_PRICE'] = round( (1.2 * (new_df['Cost'] + 95)) / 10 ) * 10 - 0.01
new_df.loc[new_df['green'] != 'ground', 'MAX_PRICE'] = round( (1.2 * (new_df['Cost'] + 175)) / 10 ) * 10 - 0.01
new_df.loc[new_df['east'] != 'ground', 'MAX_PRICE'] = round( (1.2 * (new_df['Cost'] + 195)) / 10 ) * 10 - 0.01
new_df.loc[new_df['west'] != 'ground', 'MAX_PRICE'] = round( (1.2 * (new_df['Cost'] + 225)) / 10 ) * 10 - 0.01
new_df = new_df[new_df['STRATEGY_ID'] != 50941]
new_df = new_df[new_df['SKU'].str.match('\d*-temp|\d*-TEMP|\d*-Temp') != True ]
final = new_df.drop(columns=['COST', 'MapPrice', 'Weight', 'DimensionWeight', 'DMI Exclusive', 'ground',
'local', 'east', 'green', 'west', 'FEATURED', 'TARGET_VELOCITY', 'CALC_TARGET_VELOCITY', 'Stock', 'STOCK_y', 'VAT', 'CURRENT_FEES', 'COMP_PRICE',	'COMP_SELLER_ID', 'COMP_LISTING_TYPE','MANAGED', 'SALES_RANK', 'SALES_RANK_CATEGORY',
'shipping', 'ITEM_ID', 'TITLE', 'MEMO', 'CURRENT_VELOCITY', 'CALC_MIN_PRICE', 'CALC_MAX_PRICE', 'CURRENT_SHIPPING', 'VAT_PERCENTAGE', 'STOCK_COST_VALUE', 'DATE_ADDED'])
#print(new_df)
final = final.rename(columns={"STOCK_x":"PhysicalStock", "MapPrice": "MAP_PRICE"})
#print(final)
