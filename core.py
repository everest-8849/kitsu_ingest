import os
from datetime import datetime
import argparse

import pandas as pd
import ffmpeg
import gazu
from dotenv import load_dotenv

# USES PYTHON 3.9 AS AYON
# NEED THE DEPENDENCIES:
# pip install ffmpeg-python pandas gazu python-dotenv argparse
# NEEDS .env FILE WITH KITSU_SERVER, KITSU_EMAIL, KITSU_PASSWORD

TASK_TYPE_NAME = "From EVEREST"
TASK_STATUS_NAME = "Done"


def extract_shots(df):
    required_cols = ['final_shot_name', 'FRAME DURATION', 'FPS']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in CSV: {', '.join(missing)}")

    df = df[['final_shot_name', 'FRAME DURATION', 'FPS']].copy()
    df.columns = ['final_shot_name', 'frame_length', 'fps']

    df['frame_length'] = pd.to_numeric(df['frame_length'], errors='coerce')
    df['fps'] = pd.to_numeric(df['fps'], errors='coerce')

    df.dropna(subset=['final_shot_name', 'frame_length', 'fps'], inplace=True)

    shot_info = {
        row['final_shot_name']: (row['frame_length'], row['fps'])
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

    return df.copy()


class KitsuIngest:
    def __init__(self, csv_path=None, video_path=None, project_name=None, new_version=None, sequence=None):
        # INPUTS
        self.csv_path = csv_path
        self.video_path = video_path
        self.kitsu_project = project_name
        self.new_version = new_version
        self.sequence = sequence

        # OPEN CSV, SORT BY SHOT NAME COLUMN
        self.df_obj = pd.read_csv(self.csv_path)
        self.df_obj = sort_dataframe(self.df_obj)
        # RENAME SHOTS FROM CSV
        self.df_obj.loc[:, 'final_shot_name'] = self.df_obj['SHOT'].apply(
            lambda shot: '_'.join(str(shot).split('_')[:2])
        )

        # INIT
        self.output_dir = ""
        self.processed_csv_path = ""

        # PROCESSING
        if self.csv_path:
            self.process_csv()
        if self.csv_path and self.video_path:
            self.process_video()
        # PUSHING WORKFLOW
        if self.kitsu_project and self.new_version:
            self.push_to_kitsu()
        elif self.kitsu_project:
            self.push_to_kitsu()

    def process_csv(self):
        df = self.df_obj.copy(deep=True)

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

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        processed_dir = os.path.join(os.path.dirname(__file__), 'processed')
        os.makedirs(processed_dir, exist_ok=True)
        processed_dir_timestamp = os.path.join(processed_dir, timestamp)
        os.makedirs(processed_dir_timestamp, exist_ok=True)
        self.output_dir = processed_dir_timestamp

        input_filename = os.path.splitext(os.path.basename(self.csv_path))[0]
        output_filename = f"{input_filename}_kitsu_{timestamp}.csv"
        output_path = os.path.join(processed_dir_timestamp, output_filename)
        df.to_csv(output_path, index=False)
        self.processed_csv_path = output_path

    def process_video(self):
        df = self.df_obj.copy(deep=True)
        shots = extract_shots(df)

        input_stream  = ffmpeg.input(self.video_path)

        last_frame = 0

        for idx, (final_shot_name, (length, fps)) in enumerate(shots.items()):
            start_frame = last_frame
            end_frame = last_frame + length

            trimmed = (
                input_stream.video
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
                print(f"Failed to export {final_shot_name}: {e.stderr.decode()}")

            last_frame = end_frame

    def push_to_kitsu(self):
        load_dotenv()
        kitsu_server = os.getenv('KITSU_SERVER')
        kitsu_email = os.getenv('KITSU_EMAIL')
        kitsu_password = os.getenv('KITSU_PASSWORD')

        if not all([kitsu_server, kitsu_email, kitsu_password]):
            raise EnvironmentError("Missing one of KITSU_SERVER, KITSU_EMAIL, or KITSU_PASSWORD environment variables.")

        print(f"Connecting to Kitsu server: {kitsu_server}")
        try:
            gazu.set_host(kitsu_server)
            gazu.log_in(kitsu_email, kitsu_password)
        except Exception as e:
            raise RuntimeError("Kitsu login failed. Check your credentials.") from e

        try:
            project = gazu.project.get_project_by_name(self.kitsu_project)
        except gazu.exception.RouteNotFoundException as e:
            print(f"Project '{self.kitsu_project}' not found: {e}")
            return

        print("Importing shots into Kitsu...")
        gazu.shot.import_shots_with_csv(project, self.processed_csv_path)

        print("Fetching tasks...")
        task_type = gazu.task.get_task_type_by_name(TASK_TYPE_NAME)
        task_status = gazu.task.get_task_status_by_name(TASK_STATUS_NAME)
        from_everest_tasks = gazu.task.all_tasks_for_project(project, task_type)

        shot_task_map = {}
        for task in from_everest_tasks:
            entity_id = task.get("entity_id")
            if not entity_id:
                continue

            entity = gazu.entity.get_entity(entity_id)
            if not entity:
                continue

            shot_name = entity["name"]

            shot_task_map[shot_name] = task

        print(f"Found {len(shot_task_map)} tasks matching shots.")

        for file_name in os.listdir(self.output_dir):
            if file_name.endswith(".mp4"):
                shot_base_name = os.path.splitext(file_name)[0] # remove ext
                task = shot_task_map.get(shot_base_name)

                if task:
                    video_path = os.path.join(self.output_dir, file_name)

                    try:
                        print(f"Publishing preview for shot '{shot_base_name}'...")

                        comment = gazu.task.add_comment(
                            task=task,
                            task_status=task_status,
                            comment="Auto-published preview."
                        )

                        preview = gazu.task.add_preview(
                            task=task,
                            comment=comment,
                            preview_file_path=video_path,
                            normalize_movie=True
                        )

                        gazu.task.set_main_preview(preview)

                    except Exception as e:
                        print(f"Warning: Failed to publish preview for {shot_base_name}: {e}")
                else:
                    print(f"Warning: No matching task found for {shot_base_name}")

    def close(self):
        info = "Ingestion complete."
        if self.csv_path:
            info += f" Processed CSV file."
        if self.video_path:
            info += f" Processed video file."
        if self.kitsu_project:
            info += f" Pushed to Kitsu project '{self.kitsu_project}'."
        print(info)


def main():
    parser = argparse.ArgumentParser(description='Kitsu Ingest Tool')

    parser.add_argument('--csv', required=True, help='Path to the breakdown CSV file')
    parser.add_argument('-v', '--video', help='Path to the breakdown video file')
    parser.add_argument('-p', '--push', metavar='PROJECT', help='Project name to push to Kitsu')
    parser.add_argument('-nv', '--new_version', action='store_true',
                        help='Push as a new version (requires --push)')
    parser.add_argument('--sequence', default='SQ01', help='Sequence name to assign to all shots')

    args = parser.parse_args()

    if args.new_version and not args.push:
        parser.error("--new_version requires --push to be specified")

    ingest = KitsuIngest(args.csv, args.video, args.push, args.new_version, args.sequence)
    ingest.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        exit(1)
