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
    per_year_df['orb_drb_sum'] = per_year_df.apply(
        lambda d: d['ORB'] + d['DRB'], axis=1
    )
    year_all_orb_drb_sum_avg = per_year_df['orb_drb_sum'].mean()
    per_year_df['weighed_height_by_orb_drb_sum'] = per_year_df.apply(
        lambda d : (d['orb_drb_sum'] / year_all_orb_drb_sum_avg) * d['height'], 
        axis=1
    )
    # import ipdb; ipdb.set_trace()
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

if __name__ == "__main__":
    '''
        Load CSV in Python Data Structure
    '''
    player_data_viz = Vizualization(
        data_csvfilename='player_data',
    )

    draft_78_viz = Vizualization(
        data_csvfilename='draft78',
        fields_required=[
            'Pick', 'Player'
        ]
    )

    seasons_stats_viz = Vizualization(
        data_csvfilename='Seasons_Stats',
        fields_required=[
            'PTS', 'Player'
        ]
    )

    '''
        Pandas DataFrame
    '''
    player_data_df = pd.DataFrame(
        player_data_viz.processed_data,
        columns=player_data_viz.field_names
    )

    draft_78_df = pd.DataFrame(
        draft_78_viz.processed_data,
        columns=draft_78_viz.field_names
    )

    seasons_stats_df = pd.DataFrame(
        seasons_stats_viz.processed_data,
        columns=seasons_stats_viz.field_names
    )

    joined_df_csv_file_manager = CSVFileManager('joined_df')
    
    # careerpick_wh_joined_df = pd.merge(
    #     player_data_df,
    #     draft_78_df,
    #     left_on='name',
    #     right_on='Player',
    #     # how='outer',
    # )

    pts_wh_joined_df = pd.merge(
        player_data_df,
        seasons_stats_df,
        left_on='name',
        right_on='Player'
    )

    # clean data: if required fields N/A, drop that row
    numeric_fields = ['PTS', 'height', 'ORB', 'DRB']
    numeric_fields_df = pts_wh_joined_df[numeric_fields]
    numeric_fields_df = numeric_fields_df.apply(
        pd.to_numeric, errors='coerce'
    )
    for nf in numeric_fields:
        pts_wh_joined_df[nf] = numeric_fields_df[nf]
    pts_wh_joined_df = pts_wh_joined_df.dropna(subset=numeric_fields)

    # write data
    (pts_wh_joined_df
        .sort_values(
            by='Year',
            ascending=True
        )
        .to_csv(
            joined_df_csv_file_manager.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH
        )
    )

    pts_wh_joined_gby_year = pts_wh_joined_df.groupby('Year')
    field_year_data_list = []
    field_height_data_list = []
    field_weighed_height_by_pts_data_list = []
    field_weighed_height_by_orb_drb_sum_data_list = []
    for year, per_year_df in pts_wh_joined_gby_year:
        height_data = calculate_year_height_data(per_year_df, year)
        field_year_data_list.append(year)
        field_height_data_list.append(height_data['height'])
        field_weighed_height_by_pts_data_list.append(height_data['weighed_height_by_pts'])
        field_weighed_height_by_orb_drb_sum_data_list.append(height_data['weighed_height_by_orb_drb_sum'])
    
    years_height_df = pd.DataFrame(data={
        'year': field_year_data_list,
        'height': field_height_data_list,
        'weighed_height_by_pts': field_weighed_height_by_pts_data_list,
        'weighed_height_by_orb_drb_sum': field_weighed_height_by_orb_drb_sum_data_list
    })

    print(years_height_df)
    years_height_df_csv_file_manager = CSVFileManager('years_height_df')
    years_height_df.to_csv(
        years_height_df_csv_file_manager.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH
    )