import requests
import json
import pandas as pd
from tqdm import tqdm
import os
from pathlib import Path
import time
import numpy as np
import sys
import traceback

# go to: https://developers.facebook.com/tools/explorer/
# to get token
# this token is good till August 17th
token=os.getenv("TOKEN", "EAACccrMmWa0BADfzJo29BrO461qM6gfPvoWjez5iIjoqUYf6qC2lnaFqC5Ko8pWh5Ila8QmLfWPzrrZCpqbu548TatirmZBdYQlLclOLFrDLjrrcGteHUG1NaNjJIKzUgDSQxxMoWmZCTBOstFIaCsAfiZCWlm6Jpt7OjYkggwZDZD")
file_name = os.getenv("file_name") 
sleep = os.getenv("sleep", 1800)
block_size = os.getenv("block_size", 100)


def get_media_metadata(ig_user_name, token, pages=1):
    params = {
        "ig_username": ig_user_name,
        "access_token": token,
        "endpoint_base":"https://graph.facebook.com/v17.0/",
        "instagram_account_id":"17841400224190654"
    }
    biz_discovery_first_page = '){username,ig_id,id,profile_picture_url,biography,follows_count,followers_count,media_count, media{comments_count, like_count, caption, timestamp, media_type}}' 

    endpointParams = dict() # parameter to send to the endpoint
    
    endpointParams['fields'] = 'business_discovery.username(' + params['ig_username'] + biz_discovery_first_page
    
    # string of fields to get back with the request for the account
    endpointParams['access_token'] = params['access_token'] # access token

    url = params['endpoint_base'] + params['instagram_account_id'] # endpoint url
    
    data = requests.get(url, endpointParams)
    if data.ok:
        dict_ = json.loads(data.content)
    else:
        return data.reason
    try:
        df = pd.DataFrame(dict_['business_discovery']['media']['data'])
    except:
        print(f"{ig_user_name} has bad data")
        return dict_
    
    pages -=1
    while pages>0:
        #d1 = json.loads(data.content)
        try:
            cursor = dict_['business_discovery']['media']['paging']['cursors']['after']
        except:
            return "no cursor"
        biz_discovery_following_page = f"){{username,website,name,ig_id,id,profile_picture_url,biography,follows_count,followers_count,media_count, media.after({cursor}){{comments_count, like_count, caption, timestamp, media_type}}}}"
        endpointParams['fields'] = 'business_discovery.username(' + params['ig_username'] + biz_discovery_following_page
        url = params['endpoint_base'] + params['instagram_account_id'] # endpoint url
        data = requests.get(url, endpointParams)
        dict_ = json.loads(data.content)
        df = pd.concat([df, pd.DataFrame(dict_['business_discovery']['media']['data'])])
                       
        pages -=1
        
    
    return df

def get_biz_discovery_e2e(usernames, dest_folder, scraped_users, sleep_sec=3600): 
    bad_users = []
    for ig_username in tqdm(usernames.tolist()):
        if ig_username in scraped_users:
            continue
            
        res = get_media_metadata(ig_username, token, 1)
        while type(res) is str and res == "Forbidden":
            print(f"Possibly reach rate limit, sleep for {sleep} seconds and retry.")
            time.sleep(int(sleep_sec))
            res = get_media_metadata(ig_username, token, 1)
            
        # at this point rule out the rate limit issue.
        if type(res) in [str, dict]:
            bad_users.append({ig_username: res})
        elif isinstance(res, pd.DataFrame):
            res.to_parquet(f"feature/{dest_folder}/{ig_username}.parquet")
    
    return bad_users


def get_df(file_name):
    df = pd.read_csv(os.path.join("data", file_name), sep="\t")
    return df

def get_scraped_users(dest_folder):
    scraped_users = []
    for entry in Path(f"feature/{dest_folder}").iterdir():
        # check if it a file
        if entry.is_file():
            scraped_users.append(entry.stem)
    return scraped_users

def main(file_name, sleep, block_size):
    # read in file
    df = get_df(file_name)
    dest_folder = file_name.split(".")[0]
    try:
        os.mkdir(f"feature/{dest_folder}")
    except:
        pass

    #usernames = df[df['Openness'].isna()]['username']
    usernames = df['username']
    num_chunk = len(usernames)//block_size
    print(f"total of usernames: {len(usernames)}")
    print(f"divide in to {num_chunk} chunks")
    
    for i, chunk in enumerate(tqdm(np.array_split(usernames, num_chunk))):
        scraped_users = set(get_scraped_users(dest_folder))
        bad_users = get_biz_discovery_e2e(chunk, dest_folder, scraped_users, sleep)
        print(f"Iteration {i}/{num_chunk} has {len(bad_users)} bad users, {len(scraped_users)} already scraped users.")
        with open(f"feature/{dest_folder}/bad_users_idx.json", "a") as outfile:
            json.dump(bad_users, outfile)
if __name__ == "__main__":
    try:
        main(file_name, sleep, block_size)
    except Exception as err:
        traceback.print_exc()
        sys.exit(1)