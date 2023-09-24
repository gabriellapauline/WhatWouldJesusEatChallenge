#%%
#%%
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import datetime as dt
import time
import numpy as np
import re
import os
import json
from oauth2client.service_account import ServiceAccountCredentials
import requests
import urllib3

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (dt.date, dt.datetime)):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)

def toGSheetEpochDate(date):
    return (date - dt.datetime(1899, 12, 30)) / pd.to_timedelta(1, unit='D')

def next_available_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return str(len(str_list)+1)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#%%
TOKEN = "6134931404:AAECB-KTv_tw0Y0y1CKn9sWaHW35vCqOWIY"
CHATID = -1001831146822
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

base_url = "https://api.telegram.org/bot"
valid_tags_points = {'#HealthyMeal':2,'#OMAD':7}
for hr in range(12,21):
    valid_tags_points[f'#IF{hr}'] = (hr - 8) / 2.0
#%% Get today's message from telegram
challenge_entries = pd.DataFrame()
max_attempt = 3
for attempt in range(max_attempt):
    res = requests.get(f'{base_url}{TOKEN}/getUpdates').json()

    if res['ok']:
        updates = res['result']
        if len(updates)>0:
            updates_df = pd.DataFrame.from_records([update['message'] for update in updates if 'message' in update.keys()]) 
            updates_df = updates_df[updates_df.chat.apply(lambda x: x['id'] == CHATID)]
            updates_df['date'] = updates_df['date'].apply(pd.Timestamp.utcfromtimestamp)
            updates_df['user'] = updates_df['from'].apply(lambda x: x['first_name'])
            updates_df['message'] = updates_df.apply(lambda x: x['caption'] if 'caption' in updates_df.columns and x['caption']==x['caption'] else x['text'], axis=1)
            challenge_entries = updates_df[~updates_df.message.isna()].copy()
            challenge_entries['tagInPost'] = challenge_entries['message'].apply(lambda x: [tag for tag in valid_tags_points.keys() if tag.lower() in x.lower()])
            challenge_entries = challenge_entries[challenge_entries.apply(lambda x: len(x['tagInPost'])>0, axis=1)]
        else:
            print('No updates')
    
    if len(challenge_entries)>0:
        break
    elif attempt<max_attempt-1:
        # Wait 5 mins before retrying
        print('Will retry in 30 seconds')
        time.sleep(30)
                
                

# %% Insert today's entry in GSheet
# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('D:\Gereja\GPO\KP\BPH 2022-2023\src\kp-gpo-af40b372b053.json', scope)
# authorize the clientsheet 
client = gspread.authorize(creds)

sheet = client.open('What Would Jesus Eat Challenge 2023')
records_sheet = sheet.get_worksheet_by_id(0)
leaderboard_sheet = sheet.get_worksheet_by_id(1683826479)
current_records = pd.DataFrame.from_records(records_sheet.get_all_records())

new_records = challenge_entries[~challenge_entries['message_id'].isin(current_records['MessageId'])]
for _,update in new_records.iterrows():
    next_row = next_available_row(records_sheet)
    records_sheet.update(f'A{next_row}:F{next_row}', 
                          [[toGSheetEpochDate(update['date'] + pd.offsets.Hour(8)), 
                            update['message_id'], 
                            update['from']['username'] if 'username' in update['from'].keys() else 'Unknown Username', 
                            update['from']['first_name'],
                            ', '.join(update['tagInPost']),
                            sum([valid_tags_points[tag] for tag in update['tagInPost']])]],
                          raw=False)
#%% Compute ranking
current_records = pd.DataFrame.from_records(records_sheet.get_all_records())
leaderboard_df = current_records.groupby('FirstName').agg(TotalPoints = ('Points','sum')).sort_values('TotalPoints',ascending=False)
leaderboard_df['Rank'] = leaderboard_df['TotalPoints'].rank(method='dense', ascending=False)
set_with_dataframe(worksheet=leaderboard_sheet, dataframe=leaderboard_df, include_index=True,
include_column_header=True, resize=False)

# %% Craft message for top 3 leaderboard
leaderboard_text = f"""
<b>Leaderboard {dt.datetime.today():%d %b %Y}</b>
"""
top3 = leaderboard_df[leaderboard_df['Rank']<=3]
for _, winner in top3.iterrows():
    leaderboard_text += f"""\n{winner.Rank:.0f}. {winner.name} ({winner.TotalPoints} points)"""
    
top_scorer_today = current_records[pd.to_datetime(current_records['TimestampSGT']) >= dt.datetime.combine(dt.datetime.now(), dt.datetime.min.time())].groupby('FirstName')['Points'].sum()

leaderboard_text += f"""\n\nToday's top scorer: {top_scorer_today.idxmax()} ({top_scorer_today.max()} points)"""

#%% Post Leaderboard in telegram
res = requests.post(f'{base_url}{TOKEN}/sendMessage?chat_id={CHATID}&text={leaderboard_text}&parse_mode=HTML')


# %%
