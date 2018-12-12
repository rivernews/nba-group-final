from visualization_csv_basic import *

import pandas as pd
import numpy as np

def glue_pd_series(series_a, series_b):
    return pd.concat([series_a, series_b], axis=1)

def calculate_year_weighed_height(per_year_df, year):
    year_all_pts_avg = per_year_df['PTS'].mean()
    per_year_df['weighed_height_by_pts'] = per_year_df.apply(
        lambda d : (d['PTS'] / year_all_pts_avg) * d['height'], 
        axis=1
    )
    year_all_orb_drb_sum_avg = per_year_df['orb_drb_sum'].mean()
    per_year_df['weighed_height_by_orb_drb_sum'] = per_year_df.apply(
        lambda d : (d['orb_drb_sum'] / year_all_orb_drb_sum_avg) * d['height'], 
        axis=1
    )
    return {
        'by_pts' : per_year_df['weighed_height_by_pts'].mean(),
        'by_orb_drb_sum': per_year_df['weighed_height_by_orb_drb_sum'].mean(),
    }

def calculate_year_height_data(per_year_df, year):
    year_all_height_avg = per_year_df['height'].mean()
    year_weighed_height_data = calculate_year_weighed_height(per_year_df, year)
    year_all_weighed_height_by_pts_avg = year_weighed_height_data['by_pts']
    year_all_weighed_height_by_orb_drb_sum_avg = year_weighed_height_data['by_orb_drb_sum']

    return {
        'height': year_all_height_avg,
        'weighed_height_by_pts': year_all_weighed_height_by_pts_avg,
        'weighed_height_by_orb_drb_sum': year_all_weighed_height_by_orb_drb_sum_avg
    }

def cal_weighed_height_data(__stat_player_joined_df):
    stat_player_joined_df = __stat_player_joined_df[[
        'Year', 'name', 'height', 'PTS', 'orb_drb_sum', 'AST', 'BLK'
    ]]

    weighed_by_list = ['PTS', 'orb_drb_sum', 'AST', 'BLK']
    print('INFO: Calculating weighed value for {}...'.format(weighed_by_list))

    # calculate aggregated avg first
    yearly_wed_agg_avgs = stat_player_joined_df.groupby('Year').agg({
        weighed_by : 'mean' for weighed_by in weighed_by_list
    })
    yearly_h_agg_avgs = stat_player_joined_df.groupby('Year').agg({
        'height': 'mean'
    })
    # calculate each data
    def weighed_h_by_each(player):
        def cal_weighed_value(weighed_by):
            avg = yearly_wed_agg_avgs.loc[
                player['Year'], weighed_by
            ]
            return (player[weighed_by] / avg) * player['height']
        return pd.Series([
            cal_weighed_value(weighed_by) for weighed_by in weighed_by_list
        ])
    stat_player_joined_df[[
        f'weighed_height_by_{weighed_by}' for weighed_by in weighed_by_list
    ]] = stat_player_joined_df.apply(
        weighed_h_by_each, axis=1
    )

    print("INFO: Aggregating wed heights to yearly...")
    yearly_avgs_df = stat_player_joined_df.groupby('Year').agg({ **{
        'height': 'mean'
    }, **{
        f'weighed_height_by_{weighed_by}' : 'mean' for weighed_by in weighed_by_list
    }}).rename(columns={ **{
        'height': 'height_avg'
    }, **{
        f'weighed_height_by_{weighed_by}' : f'wed_h_by_{weighed_by}_avg' for weighed_by in weighed_by_list
    }})

    return yearly_avgs_df

def generate_positions_weighed_height_data(stat_player_joined_df):
    position_unique_set = stat_player_joined_df['normalized-position'].unique()
    position_dfs = []
    for position in position_unique_set:
        # filter
        print(f"INFO: Filtering data for position {position}...")
        position_filtered_stat_player_joined_df = stat_player_joined_df[
            stat_player_joined_df['normalized-position'] == position
        ]
        print(f"INFO: Calculating weighed height data for position {position}...")
        position_filtered_years_height_df = cal_weighed_height_data(position_filtered_stat_player_joined_df)

        # append position info
        print("INFO: Appending position info to dataframe...")
        position_filtered_years_height_df = position_filtered_years_height_df.assign(
            position=pd.Series(
                [position] * len(position_filtered_years_height_df)
            ).values
        )
        position_dfs.append(position_filtered_years_height_df)
    print("INFO: Appending each position df into a single df...")
    positions_years_height_df = position_dfs[0].append(
        position_dfs[1:],
        ignore_index=True # re-index so it's unique
    )
    
    # filter by normalized-position
    print("INFO: Writing position-wise df to file...")
    positions_years_height_df = positions_years_height_df.rename_axis("id")
    positions_years_height_df_csv_file_manager = CSVFileManager('positions_years_height_df')
    positions_years_height_df.to_csv(
        positions_years_height_df_csv_file_manager.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH
    )
    

def generate_all_positions_weighed_height_data(stat_player_joined_df):
    print("INFO: Calculating weighed height data for all positions...")
    all_position_years_height_df = cal_weighed_height_data(stat_player_joined_df)
    all_positions_years_height_df_csv_file_manager = CSVFileManager('all_positions_years_height_df')
    print("INFO: Writing all-position df to file...")
    all_position_years_height_df = all_position_years_height_df.rename_axis("id") # give index field a name in csv 
    all_position_years_height_df.to_csv(
        all_positions_years_height_df_csv_file_manager.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH
    )

    

def combine_array_df(df_list):
    return df_list[0].append(
        df_list[1:],
        ignore_index=True
    )

def generate_odrb_data(stat_player_joined_df):
    # split height into several bands
    height_bands = {
        'ticks': [1.8, 1.9, 2.0, 2.1, 2.2],
        'labels': ['1.8-1.9', '1.9-2.0', '2.0-2.1', '2.1-2.2']
    }
    stat_player_joined_df['height_band'] = pd.cut(
        stat_player_joined_df['height'],
        height_bands['ticks'],
        labels=height_bands['labels']
    )

    height_banded_df_list = []
    for height_band in height_bands['labels']:
        # filter
        height_banded_df = stat_player_joined_df[
            stat_player_joined_df['height_band'] == height_band
        ]
        # group by year
        yearly_df = height_banded_df.groupby('Year')
        year_list = []
        odrb_avg_list = []
        for year, year_df in yearly_df:
            odrb_avg_list.append(
                year_df['orb_drb_sum'].mean()
            )
            year_list.append(year)
        # re-generate our dataframe
        yearly_avgs_df = pd.DataFrame(data={
            'year': year_list,
            'orb_drb_avg': odrb_avg_list,
            'height_band': [height_band] * len(year_list)
        }).rename_axis("id")
        height_banded_df_list.append(yearly_avgs_df)
    height_bands_df = combine_array_df(height_banded_df_list)
    
    print("INFO: Writing height banded df to file...")
    height_bands_df = height_bands_df.rename_axis("id")
    csv_file_manager = CSVFileManager('height_banded_odrb_df')
    height_bands_df.to_csv(
        csv_file_manager.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH
    )