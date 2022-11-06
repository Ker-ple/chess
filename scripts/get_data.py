import asyncio
import httpx
import pandas as pd
import random
from datetime import datetime
import re
import itertools

pd.options.mode.chained_assignment = None  # default='warn'

get_column = lambda df, col, dtype='object': df.get(col, pd.Series(index=df.index, name=col, dtype=dtype))   

async def gather_player_stats(users):
    async with httpx.AsyncClient() as client:
        tasks = []
        for user in users:
            tasks.append(asyncio.ensure_future(get_player_stats(client, user)))

        player_stats = await asyncio.gather(*tasks)
        player_stats = pd.concat(player_stats, axis=0, ignore_index=True)
        return clean_player_stats(player_stats)

async def get_player_stats(client, user: str):
    try:
        now = int(datetime.now().timestamp())
        resp = await client.get(f'https://api.chess.com/pub/player/{user.lower()}/stats', headers=headers)
        df = pd.json_normalize(resp.json(), sep='_')
        df['scraped_datetime'] = now
        df['username'] = user
        return df
    except Exception:
        pass

def clean_player_stats(df):
    #keep_cols = ['chess_daily_last_rating','chess_blitz_last_rating','chess_bullet_last_rating','chess_rapid_last_rating']
    keep_cols = ['username','chess_daily_best_rating','chess_daily_last_rating','chess_blitz_best_rating','chess_blitz_last_rating',\
                    'chess_bullet_last_rating','chess_bullet_best_rating','chess_rapid_best_rating','chess_rapid_last_rating']
    for col in keep_cols:
        df[col] = get_column(df, col)

    return df[keep_cols]

async def gather_game_archive(users, year, month):
    async with httpx.AsyncClient() as client:
        tasks = []
        for user in users:
            tasks.append(asyncio.ensure_future(get_game_archive(client, user, year, month)))

        game_archive = await asyncio.gather(*tasks)
        if len(game_archive) == 1:
            return clean_game_archive(game_archive[0])
        else:
            game_archive = pd.concat(game_archive, axis=0, ignore_index=True)
            return clean_game_archive(game_archive)

async def get_game_archive(client, user: str, YYYY: int, MM: int):
    YYYY, MM = str(YYYY), str(MM).zfill(2)
    try:
        now = int(datetime.now().timestamp())        
        resp = await client.get(f'https://api.chess.com/pub/player/{user.lower()}/games/{YYYY}/{MM}', headers=headers)
        df = pd.json_normalize(resp.json()['games'], sep='_')
        df['scraped_datetime'] = now
        return df
    except Exception:
        return None

def clean_game_archive(df):
    drop_cols = ['tcn', 'uuid', 'tournament',
            'initial_setup', 'fen', 
            'white_@id','white_uuid', 
            'black_@id', 'black_uuid',
            'verified']

    df = df.drop(columns=drop_cols, errors='ignore')
    df['start_time'] = get_column(df, 'start_time').astype('Int64')
    #df['start_time'] = pd.to_datetime(df['start_time'], unit='s', errors='coerce')
    df['end_time'] = get_column(df, 'end_time').astype('Int64')
    #df['end_time'] = pd.to_datetime(df['end_time'], unit='s', errors='coerce')
    df = df.drop(index=df[df['rules'] != 'chess'].index, errors='ignore')
    #df['tournament'] = get_column(df, 'tournament')
    #df['tournament'] = df['tournament'].apply(lambda x: False if pd.isnull(x) else True)
    #df['white_result'] = df.where(df['white_result'] == 'win', 1)

    #Can we make the following row faster by using str.contains('win') for vectorization?
    df['result'] = df.apply(lambda row: convert_results(row), axis=1)
    df['game_id'] = df['url'].str.split('/').str.get(-1).astype('Int64')
    df['moves'] = df['pgn'].apply(extract_moves)
    df['eco_code'] = df['pgn'].apply(extract_opening)
    df = df.drop(columns=['pgn','match','url', 'white_result', 'black_result', 'rules'], errors='ignore')
    #df = df.drop(columns=['match','url', 'white_result', 'black_result', 'rules'], errors='ignore')

    return df

async def gather_membership_data(users):
    async with httpx.AsyncClient() as client:

        tasks = []
        for user in users:
            tasks.append(asyncio.ensure_future(get_membership_data(client, user)))

        membership_data = await asyncio.gather(*tasks)
        membership_data = pd.concat(membership_data, axis=0, ignore_index=True)
        return clean_membership_data(membership_data)
    
async def get_membership_data(client, user: str):
    try:
        now = int(datetime.now().timestamp()) 
        resp = await client.get(f'https://www.chess.com/callback/user/popup/{user.lower()}', headers=headers)
        df = pd.json_normalize(resp.json(), sep='_')
        df['scraped_datetime'] = now
        df['username'] = user
        return df
    except:
        return None

def clean_membership_data(df):
    keep_cols = ['membership_code','username']
    return df[keep_cols]

async def gather_account_data(users):
    async with httpx.AsyncClient() as client:

        tasks = []
        for user in users:
            tasks.append(asyncio.ensure_future(get_account_data(client, user)))

        account_data = await asyncio.gather(*tasks)
        account_data = pd.concat(account_data, axis=0, ignore_index=True)
        return clean_account_data(account_data)

async def get_account_data(client, user: str):
            try:
                now = int(datetime.now().timestamp())
                resp = await client.get(f'https://api.chess.com/pub/player/{user.lower()}', headers=headers)
                df = pd.json_normalize(resp.json(), sep='_')
                df['scraped_datetime'] = now
                return df
            except Exception:
                pass

def clean_account_data(df):
    keep_cols = ['player_id','country','last_online','joined','title', 'scraped_datetime','username']

    df['country'] = get_column(df, 'country')
    df['title'] = get_column(df, 'title')
    df = df[keep_cols]
    df.loc[:,'country'] = df.loc[:,'country'].str.split('/').str[-1]
    #df['last_online'] = pd.to_datetime(df['last_online'], unit='s')
    #df['joined'] = pd.to_datetime(df['joined'], unit='s').dt.floor('d')    
    return df 

def get_player_list_from_games(games):
    return pd.concat([games['white_username'],games['black_username']], axis=0).unique()

def extract_moves(pgn: str):
    game = re.split('\n\n', pgn)[1]
    game = re.sub('clk', '', game)
    game = re.findall('[a-zA-Z]+-?O?-?O?\d?x?\w\d?', game)
    
    moves = str()
    for j in range(0, len(game), 2):
        try:
            moves += game[j]+game[j+1]+','
        except IndexError:
            moves += game[j]
    return moves

def extract_opening(pgn: str):
    try:
        game = re.search('ECO "[\w\d-]*"', pgn).group()
        game = re.sub('ECO "', '', game)
        game = re.sub("\"", "", game)
        return game

    except:
        return None

headers = {'Accept-Encoding': 'gzip'}
    
def convert_results(row):
    if row['white_result'] == 'win':
        return 'white_win'
    elif row['black_result'] == 'win':
        return 'black_win'
    else:
        return 'draw'    

def tunnel(seed_user:str, steps:int, begin_year: str, begin_month: str, end_year: str, end_month: str, init_year: str = '2022', init_month: str = '08'):
    ym_range = make_ym_range(begin_year, begin_month, end_year, end_month)
    user = seed_user
    year = init_year
    month = init_month
    account_data_all = list()
    player_stats_all = list()
    metadata_all = list()
    for iter in range(0, steps):
        try:
            games = asyncio.run(gather_game_archive([user], year, month))
            users = get_player_list_from_games(games)

            account_data_present = asyncio.run(gather_account_data(users))
            membership_data_present = asyncio.run(gather_membership_data(users))

            account_data_present = account_data_present.join(membership_data_present.set_index('username'), on='username')    

            player_stats_present = asyncio.run(gather_player_stats(users))

            metadata_present = pd.DataFrame({'username': [user], 'year_querying': [year], 'month_querying': [month], 'iteration': [iter]})
            print(iter, year, month, user)
            user = random.choice(users)
            player_stats_all.append(player_stats_present)
            account_data_all.append(account_data_present)
        except:
            metadata_present = pd.DataFrame({'username': ['encountered error'], 'year_querying': [year], 'month_querying': [month], 'iteration': [iter]})
            print(iter, year, month, 'encountered error')
        
        metadata_all.append(metadata_present)
        year, month = get_new_ym(ym_range)

    metadata_all = pd.concat(metadata_all, axis=0, ignore_index=True)
    player_stats_all = pd.concat(player_stats_all, axis=0, ignore_index=True)
    account_data_all = pd.concat(account_data_all, axis=0, ignore_index=True)

    return account_data_all, player_stats_all, metadata_all

def make_ym_range(begin_year: str, begin_month: str, end_year: str, end_month: str):
    begin_year, begin_month = int(begin_year), int(begin_month)
    end_year, end_month = int(end_year), int(end_month)
    if (begin_year < end_year) & (begin_month in range(1,13)) & (end_month in range(1,13)):
        years = [year for year in range(begin_year, end_year+1)]
        months = range(1, 13)
        years_months = [element for element in itertools.product(*[years,months])]
        years_months = years_months[begin_month-1:-(12-end_month)]
        return years_months
    else:
        raise ValueError('invalid years and/or months.')

def get_new_ym(ym_range: list):
    year, month = random.choice(ym_range)
    return str(year), str(month).zfill(2)