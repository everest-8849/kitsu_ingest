import os
import pandas as pd
from datetime import datetime
import logging
from ..utils.validation import sort_dataframe


class CsvProcessor:
    def __init__(self, csv_path, sequence, output_dir=None):
        self.csv_path = csv_path
        self.sequence = sequence
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_dir = output_dir or self._create_output_dir()
        self.df = None
        self.processed_csv_path = None


    def _create_output_dir(self):
        processed_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'processed', self.timestamp)
        os.makedirs(processed_dir, exist_ok=True)
        return processed_dir

    def process(self):
        self.df = pd.read_csv(self.csv_path)
        self.df = sort_dataframe(self.df)
        self.df.loc[:, 'final_shot_name'] = self.df['SHOT'].apply(
            lambda shot: '_'.join(str(shot).split('_')[:2])
        )

        df = self.df.copy(deep=True)
        rename_mapping = {
            'final_shot_name': 'Name',
            'FRAME IN': 'Frame In',
            'FRAME OUT': 'Frame Out',
            'FRAME DURATION': 'Nb Frames',
            'Clip Name': 'Description'
        }

        missing_cols = [col for col in rename_mapping if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns for rename: {missing_cols}")

        df.rename(columns=rename_mapping, inplace=True)
        df['Sequence'] = self.sequence
        columns_to_keep = ['Sequence'] + list(rename_mapping.values()) + ['FPS']
        df = df[columns_to_keep]

        input_filename = os.path.splitext(os.path.basename(self.csv_path))[0]
        output_filename = f"{input_filename}_kitsu_{self.timestamp}.csv"
        output_path = os.path.join(self.output_dir, output_filename)
        df.to_csv(output_path, index=False)
        self.processed_csv_path = output_path

        logging.info(f"CSV processed and saved to: {output_path}")
        return self.processed_csv_path