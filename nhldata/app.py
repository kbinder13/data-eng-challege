'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import boto3
import requests
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse
import sys


logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f'{self.SCHEMA_HOST}/{self.VERSION_PREFIX}'


    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        ''' 
        returns a dict tree structure that is like
            "dates": [ 
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})

    def boxscore(self, game_id):
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            "person": {
                                "id": $int,
                                "fullName": $string,
                                #-- other info
                                "currentTeam": {
                                    "name": $string,
                                    #-- other info
                                },
                                "stats": {
                                    "skaterStats": {
                                        "assists": $int,
                                        "goals": $int,
                                        #-- other status
                                    }
                                    #-- ignore "goalieStats"
                                }
                            }
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home" 
                }
            }

            See tests/resources/boxscore.json for a real example response
        '''
        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    def _get(self, url, params=None):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
        except requests.exceptions.HTTPError as ehtp:
            print ("Http Error: ",ehtp)
            raise SystemExit(ehtp)
        except requests.exceptions.ConnectionError as econ:
            print ("Connection Error: ",econ)
            raise SystemExit(econ)
        except requests.exceptions.Timeout as etim:
            print ("Timeout Error: ",etim)
            raise SystemExit(etim)
        except requests.exceptions.RequestException as ex:
            print ("Error in API Request: ",ex)
            raise SystemExit(ex)
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

@dataclass
class StorageKey():
    def __init__(self, gameid, gamedate):
        self._gameid = gameid
        self._gamedate = gamedate.strftime('%Y%m%d')

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        return f'{self._gamedate}_{self._gameid}.csv'

class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        return True

class Crawler():
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage

    def crawl(self, startdate: datetime, enddate: datetime) -> None:
        '''
            Crawl for player scoring stats.
            Writes CSV files to S3 Bucket for NHL Games in date range specified (inclusive).
            Files partitioned by Date and Game ID.
        '''
        schedule = self.api.schedule(startdate, enddate)
        
        ##iterate over game dates to properly partition
        if schedule is not None:
            for day in schedule.get("dates"):
                gamedate = datetime.strptime(day.get("date"),'%Y-%m-%d')

                logging.info(f'Processing games for {gamedate}')
                games_df = pd.DataFrame()
                games_df = games_df.append(pd.json_normalize(day.get("games")), ignore_index = True)
            
                column_names = ["player_person_id", "player_person_currentTeam_name", "player_person_fullName", "player_stats_skaterStats_assists", "player_stats_skaterStats_goals", "side"]

                for index, row in games_df.iterrows():
                    gameid = row["gamePk"]

                    stats = self.api.boxscore(gameid)
                    stats_df = pd.DataFrame()

                    for side in ('home','away'):
                        teamname = stats.get("teams").get(side).get("team")["name"]
                        players = stats.get("teams").get(side).get("players").keys()

                        for p in players:
                            if stats.get("teams").get(side).get("players").get(f'{p}').get("stats").get("skaterStats") is not None:
                                playername = stats.get("teams").get(side).get("players").get(f'{p}').get("person").get("fullName")
                                goals,assists = [stats.get("teams").get(side).get("players").get(f'{p}').get("stats").get("skaterStats").get(k) for k in ["goals","assists"]]
                                
                                playerstats = pd.Series([p.replace('ID',''),teamname,playername,assists,goals,side], index=column_names)
                                stats_df = stats_df.append(playerstats, ignore_index=True)
                    
                    s3Key = StorageKey(gameid,gamedate)
                    
                    logging.info(f'Writing file: {s3Key.key()}')

                    self.storage.store_game(s3Key, stats_df[column_names].to_csv(index=False))
        else:
            logging.info(f'No games found for date range {startdate} - {enddate}')

                 
def main():
    import os
    import argparse

    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    parser.add_argument('--start_date',
                            required=True,
                            type=str,
                            help='Set start date to begin to retrieve data (inclusive). Format: yyyymmdd')
    parser.add_argument('--end_date',
                        required=True,
                        type=str,
                        help='Set end date to stop retrieving data (inclusive). Format: yyyymmdd')
    args = vars(parser.parse_args())

    dest_bucket = os.environ.get('DEST_BUCKET', 'output')
    start_date = dateparse(args['start_date'])
    end_date = dateparse(args['end_date'])

    api = NHLApi()
    s3client = boto3.client('s3', config=Config(signature_version='s3v4'), endpoint_url=os.environ.get('S3_ENDPOINT_URL'))
    storage = Storage(dest_bucket, s3client)
    crawler = Crawler(api, storage)
    crawler.crawl(start_date, end_date)

if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        print(f'exception: {ex}')
        sys.exit(1)

