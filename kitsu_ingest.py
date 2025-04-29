from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import argparse
import ffmpeg
import gazu
import os

from ffmpeg import output


def extract_shots(df):
    df = df[['SHOT', 'FRAME DURATION', 'FPS']]
    df.columns = ['shot', 'frame_length', 'fps']

    df['frame_length'] = pd.to_numeric(df['frame_length'], errors='coerce')
    df['fps'] = pd.to_numeric(df['fps'], errors='coerce')

    df.dropna(subset=['shot', 'frame_length', 'fps'], inplace=True)

    shot_info = {
        row['shot']: (row['frame_length'], row['fps'])
        for _, row in df.iterrows()
    }

    return shot_info

def sort_dataframe(df):
    df['SHOT'] = df['SHOT'].astype(str)

    # Split the shot into parts if needed
    # Example: 'SHOT_0030_A006C012_241206VG' → ['SHOT', '0030', 'A006C012', '241206VG']
    split_cols = df['SHOT'].str.split('_', expand=True)

    # Add temporary columns for sorting
    df['sort_prefix'] = split_cols[0]  # e.g., 'SHOT'
    df['sort_number'] = pd.to_numeric(split_cols[1], errors='coerce')  # e.g., 30 (numeric)

    # Sort first by prefix alphabetically, then by number ascending
    df = df.sort_values(by=['sort_prefix', 'sort_number'], ascending=[True, True])

    # Drop the helper columns
    df = df.drop(columns=['sort_prefix', 'sort_number'])

    return df


class KitsuIngest:
    def __init__(self, csv_path=None, video_path=None, project_name=None):
        # INPUTS
        self.csv_path = csv_path
        self.video_path = video_path
        self.kitsu_project = project_name

        # OPEN CSV, SORT BY SHOT NAME COLUMN
        self.df_obj = pd.read_csv(self.csv_path)
        self.df_obj = sort_dataframe(self.df_obj)

        self.output_dir = ""

        # PROCESSED
        self.processed_csv_path = None

        if self.csv_path:
            self.process_csv()
        if self.csv_path and self.video_path:
            self.process_video()
        if self.kitsu_project:
            self.push_to_kitsu()

    def process_csv(self):
        df = self.df_obj.copy()

        rename_mapping = {
            'SHOT': 'Name',
            'FRAME IN': 'Frame In',
            'FRAME OUT': 'Frame Out',
            'FRAME DURATION': 'Nb Frames',
            'Clip Name': 'Description'
        }

        df.rename(columns=rename_mapping, inplace=True)
        df['Sequence'] = 'SQ01'
        columns_to_keep = ['Sequence'] + list(rename_mapping.values()) + ['FPS']
        df = df[columns_to_keep]

        processed_dir = os.path.join(os.path.dirname(__file__), 'processed')
        os.makedirs(processed_dir, exist_ok=True)
        self.output_dir = processed_dir

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        input_filename = os.path.splitext(os.path.basename(self.csv_path))[0]
        output_filename = f"{input_filename}_kitsu_{timestamp}.csv"
        output_path = os.path.join(processed_dir, output_filename)
        df.to_csv(output_path, index=False)
        self.processed_csv_path = output_path

    def process_video(self):
        df = self.df_obj.copy()
        shots = extract_shots(df)

        input = ffmpeg.input(self.video_path)

        last_frame = 0

        for idx, (shot, (length, fps)) in enumerate(shots.items()):
            start_frame = last_frame
            end_frame = last_frame + length
            final_shot_name = '_'.join(shot.split('_')[:2])

            trimmed = (
                input.video
                .trim(start_frame=start_frame, end_frame=end_frame)
                .setpts('PTS-STARTPTS')  # Reset timestamps
            )

            os.makedirs(self.output_dir, exist_ok=True)
            output_path = os.path.join(self.output_dir, f"{final_shot_name}.mp4")

            try:
                (
                    ffmpeg
                    .output(
                        trimmed,
                        output_path,
                        vcodec='libx264',  # H.264 codec
                        pix_fmt='yuv420p',  # Standard 8-bit pixel format
                        crf=18  # Quality level: lower = better (18 is visually lossless)
                    )
                    .overwrite_output()
                    .run(quiet=True)
                )
                print(f"Exported: {output_path}")
            except ffmpeg.Error as e:
                print(f"Failed to export {shot}: {e.stderr.decode()}")

            last_frame = end_frame

    def push_to_kitsu(self):
        load_dotenv()

        kitsu_server = os.getenv('KITSU_SERVER')
        kitsu_email = os.getenv('KITSU_EMAIL')
        kitsu_password = os.getenv('KITSU_PASSWORD')


    def close(self):
        info = "Ingestion complete."
        if self.csv_path:
            info += f" Processed CSV file."
        if self.video_path:
            info += f" Processed video file."
        print(info)


def main():
    parser = argparse.ArgumentParser(description='Kitsu Ingest Tool')
    parser.add_argument('--csv', required=True, help='Path to CSV file')
    parser.add_argument('-v', '--video', required=False, help='Path to video file')
    parser.add_argument('--push', required=True, type=str, help='Project name to push to Kitsu')

    args = parser.parse_args()

    ingest = KitsuIngest(args.csv, args.video)
    ingest.close()


if __name__ == "__main__":
    main()