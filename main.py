from visualization_csv_basic import *

import pandas as pd
import numpy as np

if __name__ == "__main__":
    player_data_viz = Vizualization(
        data_csvfilename='player_data'
    )

    draft_78_viz = Vizualization(
        data_csvfilename='draft78',
        fields_required=[
            'Pick', 'Player'
        ]
    )

    player_data_df = pd.DataFrame(
        player_data_viz.processed_data,
        columns=player_data_viz.field_names
    )

    draft_78_df = pd.DataFrame(
        draft_78_viz.processed_data,
        columns=draft_78_viz.field_names
    )


    joined_df_csv_file_manager = CSVFileManager('joined_df')
    (pd.merge(
        player_data_df,
        draft_78_df,
        left_on='name',
        right_on='Player',
        how='outer',
    )
    .sort_values(
        by='Pick',
        ascending=True
    )
    .to_csv(
        joined_df_csv_file_manager.CSV_DATA_OUTPUT_PROCESSED_FILE_PATH
    ))
