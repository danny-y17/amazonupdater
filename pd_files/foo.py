import pandas as pd
import cgi

form = cgi.FieldStorage()
file1 = form.getvalue('csv_file1')
bulk_item = pd.read_csv(file1, encoding='latin1')
df = pd.DataFrame(bulk_item, columns=['RemoteId',"Cost","MapPrice", "Weight", "DimensionWeight", 'Stock', 'DMI Exclusive', 'DeckFinalPrice', 
'EMO', 'STA', 'ItemType', 'CategoryID', 'Brand'])
df = df[df['Brand'] != 'Whitehall Products']
df.dropna(inplace=True)

# calculate min prices
df.loc[df['Weight'] <= 70, 'ground'] = round( (1.12 * (df['Cost'] + df['DimensionWeight'] + 15)) / 10) * 10 - 0.01
df.loc[df['Weight'] <= 70, 'local'] = 'ground'
df.loc[df['Weight'] <= 70, 'east'] = 'ground'
df.loc[df['Weight'] <= 70, 'green'] = 'ground'
df.loc[df['Weight'] <= 70, 'west'] = 'ground'

df.loc[df['Weight'] > 70, 'local'] = round( (1.12 * (df['Cost'] + 95)) / 10 ) * 10 - 0.01
df.loc[df['Weight'] > 70, 'east'] = round( (1.12 * (df['Cost'] + 175)) / 10 ) * 10 - 0.01
df.loc[df['Weight'] > 70, 'green'] = round( (1.12 * (df['Cost'] + 195)) / 10 ) * 10 - 0.01
df.loc[df['Weight'] > 70, 'west'] = round( (1.12 * (df['Cost'] + 225)) / 10 ) * 10 - 0.01
df.loc[df['Weight'] > 70, 'ground'] = 'LTL'

df['STOCK'] = df['Stock'] + df['DMI Exclusive']
df = df.fillna(0)
#print(df)
#df.to_csv('updated_bulk_upload.csv', index=False)
