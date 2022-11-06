from operator import not_
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime



def plot_joined_date(account_data, filename):
    #plt.hist((pd.to_datetime(player_data['joined'], unit='s').dt.date), bins=30)
    #ax.set_xticks(ticks, rotation=70)
    #plt.show()
    data = pd.to_datetime(account_data['joined'], unit='s').dt.year
    bins = data.nunique()

    fig, ax = plt.subplots()

    #sns.displot(data=data)
    plt.hist(data, bins=bins)
    ax.set_xticks(np.linspace(2008.5, 2020.5, 15), labels=range(2008, 2023), rotation=30)
    plt.title('strong account creation in 2020 and 2021')
    plt.ylabel('number of accounts created')
    plt.xlabel('year')
    plt.savefig(filename, dpi='figure')
    plt.close()

def get_days_active(account_data):
    return ((pd.to_datetime(account_data['last_online'], unit='s').dt.date) - (pd.to_datetime(account_data['joined'], unit='s').dt.date)).dt.days

def plot_days_active_line(account_data, days_lim, filename):
    days_active = get_days_active(account_data)

    proportions = list()
    for day in range(days_lim):
        not_created_yet = days_active > day
        proportion = not_created_yet.value_counts(normalize=True)
        proportion_not_created = proportion.iat[0]
        proportions.append(proportion_not_created)

    plt.plot(range(days_lim), proportions)
    plt.title('58% of accounts created since pandemic start')
    plt.ylabel('proportion of accounts created since present')
    plt.xlabel('days before present')
    plt.vlines(950, ymin=0, ymax=.7, colors='r', label='March 6, 2020')
    plt.legend()
    plt.savefig(filename, dpi='figure')
    plt.close()


def plot_days_active_bar(account_data, filename):
    days_active = get_days_active(account_data)
    plt.hist(days_active, bins=20)
    plt.xlabel('days active')
    plt.ylabel('count')
    plt.title('account lifespan peaks near 600-800 days')
    plt.savefig(filename, dpi='figure')
    plt.close()

def get_days_since_active(account_data):
    return (pd.to_datetime(int(datetime.now().timestamp()), unit='s') - pd.to_datetime(account_data['last_online'], unit='s')).dt.days

def plot_days_since_active_line(account_data, days_lim, filename):
    days_since_active = get_days_active(account_data)

    proportions = list()
    for day in range(days_lim):
        more_than_week_off = days_since_active > day
        proportion = more_than_week_off.value_counts(normalize=True)
        proportion_away = proportion.iat[0]
        proportions.append(proportion_away)
        
    plt.plot(range(days_lim), proportions)
    plt.title('63% of accounts active within last 5 days')
    plt.ylabel('proportion of accounts active')
    plt.xlabel('days since online')
    plt.savefig(filename, dpi='figure')
    plt.close()

def get_max_elo(player_stats):
    return player_stats.loc[:, player_stats.columns != 'username'].max(axis=1)

def get_mean_elo(player_stats):
    return player_stats.loc[:, player_stats.columns != 'username'].mean(axis=1)

def plot_rolling_elo_line(mode: str, player_stats, window: int, filename):
    if mode == 'max':
        elo_measure = get_max_elo(player_stats)
    elif mode == 'mean':
        elo_measure = get_mean_elo(player_stats)
    sns.lineplot(elo_measure.rolling(window).mean())
    plt.title(f'rolling {window}-user {mode} elo line')
    plt.savefig(filename, dpi='figure')
    plt.close()