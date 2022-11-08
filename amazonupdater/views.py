from django.shortcuts import render
from django.shortcuts import HttpResponse
from django.contrib import messages
from django.forms import ValidationError
from django.http import HttpResponseRedirect
from django.shortcuts import render
from django.urls import reverse
#from pd_files import seller_comp
from django.http import StreamingHttpResponse
import logging
#from pd_files import foo, informed
import pandas as pd

def upload_csv(request):
    data = {}
    # if get 
    if "GET" == request.method:
        return render(request, "amazonupdater/upload_csv.html", data)
    
    # if not get, proceed
    try:
        csv_file1 = request.FILES['csv_file1']
        csv_file2 = request.FILES['csv_file2']
        csv_file3 = request.FILES['csv_file3']
        csv_file4 = request.FILES['csv_file4']
        # if file is not csv ext
        if not csv_file1.name.endswith('.csv') or not csv_file2.name.endswith('.csv') or not csv_file3.name.endswith('.csv') or not csv_file4.name.endswith('.csv'):
            messages.error(request, "File is not CSV type")
            return HttpResponseRedirect(reverse("upload_csv"))
        # if file is too large, return
        if csv_file1.multiple_chunks() or csv_file2.multiple_chunks() or csv_file3.multiple_chunks() or csv_file4.multiple_chunks():
            messages.error(request, "Uploaded file is too big")
            return HttpResponseRedirect(reverse("upload_csv"))

        # bulk item sectiona
        bulk_item = pd.read_csv(csv_file1.name, encoding='latin1')
        print(bulk_item)
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

        # buy box section
        buy_box = pd.read_csv(csv_file2.name, encoding = 'latin1')
        df_buy_box = pd.DataFrame(buy_box)

        # add remote for buybox
        df_buy_box[['RemoteId', 'shipping']] = df_buy_box['SKU'].str.split('-', 1, expand=True)

        df_buy_box.columns = df_buy_box.columns.str.replace(' ','_')
        df['RemoteId'] = df['RemoteId'].astype('string')
        df_buy_box['RemoteId'] = df_buy_box['RemoteId'].astype('string')
        new_df = pd.merge(df, df_buy_box, left_on='RemoteId', right_on='RemoteId', how='inner')
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

        # seller comp section
        comp = pd.read_csv(csv_file3)
        df_comp = pd.DataFrame(comp)

        category = pd.read_csv(csv_file4)
        df_category = pd.DataFrame(category)

        df_w_comp = pd.merge(final, df_comp, left_on='BUYBOX_SELLER', right_on='COMP_SELLER_ID', how='left')
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


        output_df = df_w_comp
        response = HttpResponse(content_type= 'text/csv')
        response['Content-Disposition'] = 'attachment; filename=export.csv'
        #'tableview/static/csv/20_Startups.csv' is the django 
        # directory where csv file exist.
        # Manipulate DataFrame using to_html() function
        output_df.to_csv(path_or_buf=response)
        return response

    except Exception as e:
        logging.getLogger("error_logger").error("Unable to upload file. "+repr(e))
        messages.error(request,"Unable to upload file. "+repr(e))
        
    return HttpResponseRedirect(reverse("upload_csv"))

'''
def Table(request):
    df = seller_comp.df_w_comp
    response = HttpResponse(content_type= 'text/csv')
    response['Content-Disposition'] = 'attachment; filename=export.csv'
    #'tableview/static/csv/20_Startups.csv' is the django 
    # directory where csv file exist.
    # Manipulate DataFrame using to_html() function
    geeks_object = df.to_csv(path_or_buf=response)
    return response
'''
