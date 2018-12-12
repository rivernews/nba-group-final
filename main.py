from visualization_csv_basic import *
from height_performance_data import *

import pandas as pd
import numpy as np

if __name__ == "__main__":
    '''
        Load CSV in Python Data Structure
    '''
    print("INFO: Forming player_data data...")
    player_data_viz = Vizualization(
        data_csvfilename='player_data',
        fields_required=[
            # 'weight', 
            'height', 
            'position', 
            # 'year_start', 
            # 'year_end'
        ]
    )

    # draft_78_viz = Vizualization(
    #     data_csvfilename='draft78',
    #     fields_required=[
    #         'Pick', 'Player'
    #     ]
    # )

    print("INFO: Forming Seasons_Stats data...")
    seasons_stats_viz = Vizualization(
        data_csvfilename='Seasons_Stats',
        fields_required=[
            'PTS', 'Player', 'ORB', 'DRB', 'AST', 'BLK'
        ]
    )

    '''
        Pandas DataFrame
    '''
    print("INFO: Preparing joined stat_player_joined_df...")
    player_data_df = pd.DataFrame(
        player_data_viz.processed_data,
        columns=player_data_viz.field_names
    )

    # draft_78_df = pd.DataFrame(
    #     draft_78_viz.processed_data,
    #     columns=draft_78_viz.field_names
    # )

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

    stat_player_joined_df = pd.merge(
        player_data_df,
        seasons_stats_df,
        left_on='name',
        right_on='Player'
    )

    # clean data: if required fields N/A, drop that row
    numeric_fields = ['PTS', 'height', 'ORB', 'DRB', 'AST', 'BLK']
    numeric_fields_df = stat_player_joined_df[numeric_fields]
    numeric_fields_df = numeric_fields_df.apply(
        pd.to_numeric, errors='coerce'
    )
    for nf in numeric_fields:
        stat_player_joined_df[nf] = numeric_fields_df[nf]
    stat_player_joined_df = stat_player_joined_df.dropna(subset=numeric_fields)
    stat_player_joined_df['orb_drb_sum'] = stat_player_joined_df.apply(
        lambda d: d['ORB'] + d['DRB'], axis=1
    )

    # write data
    (stat_player_joined_df
        .sort_values(
            by='Year',
            ascending=True
        )
        .to_csv(
            joined_df_csv_file_manager.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH
        )
    )

    # part 2-1
    generate_all_positions_weighed_height_data(stat_player_joined_df)

    # part 2-2
    # generate_odrb_data(stat_player_joined_df)