from get_data import *
from plot_data import *

account_data, player_stats, metadata = tunnel('Kerple', steps=20, begin_year='2019', begin_month='03', end_year='2022', end_month='10', init_year='2021', init_month='04')
print(account_data)
print(player_stats)
print(metadata)

plot_joined_date(account_data, 'joined date')

plot_days_active_line(account_data, 1000, 'days active line')

plot_days_active_bar(account_data, 'days active bar')

plot_days_since_active_line(account_data, 100, 'days since active')

plot_rolling_elo_line('max', player_stats, 15, 'rolling 15-player max elo')

plot_rolling_elo_line('mean', player_stats, 15, 'rolling 15-player mean elo')