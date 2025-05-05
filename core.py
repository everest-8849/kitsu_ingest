from datetime import datetime
import argparse
import os

from dotenv import load_dotenv
import pandas as pd
import logging
import ffmpeg
import gazu

# USES PYTHON 3.9 AS AYON
# NEED THE DEPENDENCIES:
# pip install ffmpeg-python pandas gazu python-dotenv argparse
# NEEDS .env FILE WITH KITSU_SERVER, KITSU_EMAIL, KITSU_PASSWORD

TASK_TYPE_NAME = "From EVEREST"
TASK_STATUS_NAME = "Done"

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)


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


# Fetch the last modified CSV file from a directory
def fetch_csv_from_folder(path):
    if os.path.isdir(path):
        csv_files = sorted(
            (os.path.join(path, f) for f in os.listdir(path)
             if f.lower().endswith('.csv') and os.path.isfile(os.path.join(path, f))),
            key=os.path.getmtime,
            reverse=True
        )

        if not csv_files:
            raise FileNotFoundError(f"[fetch_csv_from_folder] No .csv files found in directory: {path}")

        return os.path.join(path, csv_files[0])

    else:
        raise FileNotFoundError(f"[fetch_csv_from_folder] Path does not exist: {path}")


def kitsu_login():
    load_dotenv()
    kitsu_server = os.getenv('KITSU_SERVER')
    kitsu_email = os.getenv('KITSU_EMAIL')
    kitsu_password = os.getenv('KITSU_PASSWORD')

    if not all([kitsu_server, kitsu_email, kitsu_password]):
        raise EnvironmentError("Missing one of KITSU_SERVER, KITSU_EMAIL, or KITSU_PASSWORD in .env")

    logging.info(f"Connecting to Kitsu server: {kitsu_server}")
    try:
        gazu.set_host(kitsu_server)
        gazu.log_in(kitsu_email, kitsu_password)
    except Exception as e:
        raise RuntimeError("Kitsu login failed. Check your credentials or host address.") from e


def fetch_shot_name_from_tasks(all_tasks):
    shot_task_map = {}
    for task in all_tasks:
        entity_id = task.get("entity_id")
        if not entity_id:
            continue
        entity = gazu.entity.get_entity(entity_id)
        if entity:
            shot_task_map[entity["name"]] = task
    return shot_task_map


def safety_check_kitsu_vs_local_mp4(kitsu_data, mp4_files):
    shots_names_from_sequence = set(kitsu_data.keys())
    mp4_shot_names = {os.path.splitext(f)[0] for f in mp4_files}

    matching = shots_names_from_sequence & mp4_shot_names
    missing_in_files = shots_names_from_sequence - mp4_shot_names
    extra_in_files = mp4_shot_names - shots_names_from_sequence

    logging.info(f"Total shots in Kitsu: {len(shots_names_from_sequence)}")
    logging.info(f"Total MP4 files: {len(mp4_shot_names)}")
    logging.info(f"Matching shot names: {len(matching)}")

    if missing_in_files:
        logging.warning(f"Shots missing corresponding MP4s: {sorted(missing_in_files)}")
    if extra_in_files:
        logging.warning(f"Extra MP4 files without matching shots: {sorted(extra_in_files)}")

    if missing_in_files or extra_in_files:
        ask_user_input()


def safety_check_matching_metadata(kitsu_data, local_data):
    mismatches = {}

    for shot_name, csv_data in local_data.items():
        kitsu_shot = kitsu_data.get(shot_name)
        if not kitsu_shot:
            mismatches[shot_name] = {"error": "Missing in Kitsu shots"}
            continue

        # Compare each metadata field between CSV and Kitsu
        differences = {}
        for field_name, csv_value in csv_data.items():
            kitsu_value = kitsu_shot[field_name]

            if isinstance(csv_value, float):
                if abs(csv_value - kitsu_value) >= 1e-3:
                    differences[field_name] = {"csv": csv_value, "kitsu": kitsu_value}

            elif csv_value != kitsu_value:
                differences[field_name] = {"csv": csv_value, "kitsu": kitsu_value}

        if differences:
            mismatches[shot_name] = differences

    for name in kitsu_data:
        if name not in local_data:
            mismatches[name] = {"error": "Missing in CSV shots"}

    # Summary
    if mismatches:
        logging.warning("Metadata mismatches detected:")
        for name, diff in mismatches.items():
            print(f"\nShot: {name}")
            for k, v in diff.items():
                if isinstance(v, dict):
                    print(f"  {k}: CSV={v['csv']} | Kitsu={v['kitsu']}")
                else:
                    print(f"  {k}: {v}")
        ask_user_input()
    else:
        logging.info("All metadata matches between CSV and Kitsu.")


def ask_user_input():
    response = input("continue? (y/N): ").strip().lower()
    if response not in ("y", "yes"):
        logging.info("Ingest aborted by user.")
        exit(0)


class KitsuIngest:
    def __init__(self, csv_path=None, video_path=None, project_name=None, push_only=None, sequence=None):
        # INPUTS
        self.csv_path = csv_path
        self.video_path = video_path
        self.kitsu_project = project_name
        self.sequence = sequence
        self.push_only = push_only

        # INIT
        self.output_dir = ""
        self.processed_csv_path = ""
        self.timestamp = ""
        self.kitsu_data = None
        self.local_data = None

        # PROCESSING
        if self.push_only:
           self.processed_csv_path = fetch_csv_from_folder(self.push_only)
           self.output_dir = self.push_only
           print(f"[push_only] Using CSV: {self.processed_csv_path}")
        else:
            # OPEN CSV, SORT BY SHOT NAME COLUMN
            self.df_obj = pd.read_csv(self.csv_path)
            self.df_obj = sort_dataframe(self.df_obj)
            # RENAME SHOTS FROM CSV
            self.df_obj.loc[:, 'final_shot_name'] = self.df_obj['SHOT'].apply(
                lambda shot: '_'.join(str(shot).split('_')[:2])
            )
            self.build_output_dir()
        if self.csv_path:
            self.process_csv()
        if self.video_path and self.csv_path:
            self.process_video()

        # PUSHING WORKFLOW
        if self.kitsu_project:
            self.push_to_kitsu()

    def build_output_dir(self):
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        processed_dir = os.path.join(os.path.dirname(__file__), 'processed')
        os.makedirs(processed_dir, exist_ok=True)
        processed_dir_timestamp = os.path.join(processed_dir, self.timestamp)
        os.makedirs(processed_dir_timestamp, exist_ok=True)
        self.output_dir = processed_dir_timestamp

    def build_data_dicts(self, shots_from_sequence, processed_csv_path):
        self.kitsu_data = {
            shot["name"]: {
                "frame_in": int(shot["data"].get("frame_in", 0)),
                "frame_out": int(shot["data"].get("frame_out", 0)),
                "nb_frames": shot.get("nb_frames", 0),
                "fps": float(shot["data"].get("fps", 0.0)),
                "description": shot.get("description", "")
            }
            for shot in shots_from_sequence
        }

        df = pd.read_csv(processed_csv_path)
        self.local_data = {
            row["Name"]: {
                "frame_in": int(row["Frame In"]),
                "frame_out": int(row["Frame Out"]),
                "nb_frames": int(row["Nb Frames"]),
                "fps": float(row["FPS"]),
                "description": row["Description"]
            }
            for _, row in df.iterrows()
        }


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

        input_filename = os.path.splitext(os.path.basename(self.csv_path))[0]
        output_filename = f"{input_filename}_kitsu_{self.timestamp}.csv"
        output_path = os.path.join(self.output_dir, output_filename)
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
                logging.info(f"Exported: {output_path}")
            except ffmpeg.Error as e:
                logging.warning(f"Failed to export {final_shot_name}: {e.stderr.decode()}")

            last_frame = end_frame

    def push_to_kitsu(self):
        kitsu_login()

        try:
            project = gazu.project.get_project_by_name(self.kitsu_project)
        except gazu.exception.RouteNotFoundException as e:
            logging.warning(f"Project '{self.kitsu_project}' not found: {e}")
            return

        logging.info("Importing shots from CSV...")
        gazu.shot.import_shots_with_csv(project, self.processed_csv_path)

        logging.info(f"Fetching data from Kitsu...")
        task_type = gazu.task.get_task_type_by_name(TASK_TYPE_NAME)
        task_status = gazu.task.get_task_status_by_name(TASK_STATUS_NAME)
        sequence = gazu.shot.get_sequence_by_name(project, self.sequence)
        shots_from_sequence = gazu.shot.all_shots_for_sequence(sequence)
        all_tasks = gazu.task.all_tasks_for_project(project, task_type)
        shot_task_map = fetch_shot_name_from_tasks(all_tasks)

        logging.info(f"Fetching data from local folder...")
        mp4_files = [f for f in os.listdir(self.output_dir) if f.endswith(".mp4")]

        # Build data dictionaries
        self.build_data_dicts(shots_from_sequence, self.processed_csv_path)

        # Safety check
        safety_check_kitsu_vs_local_mp4(self.kitsu_data, mp4_files)
        safety_check_matching_metadata(self.kitsu_data, self.local_data)
        #####
        # DO SHOT NAME CHECK
        ####

        unmatched_count = 0
        failed_count = 0
        for file_name in mp4_files:
            shot_name = os.path.splitext(file_name)[0]
            task = shot_task_map.get(shot_name)

            if not task:
                logging.warning(f"No matching Kitsu task found for shot: {shot_name}")
                unmatched_count += 1
                continue

            video_path = os.path.join(self.output_dir, file_name)
            if not os.path.exists(video_path):
                logging.warning(f"Video file not found: {video_path}")
                unmatched_count += 1
                continue

            logging.info(f"Publishing preview for: {shot_name}")

            try:
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
                logging.error(f"Failed to publish preview for {shot_name}: {e}")
                failed_count += 1

        logging.info(f"Finished publishing previews.")
        logging.info(f"Matched: {len(mp4_files) - unmatched_count - failed_count}, "
                     f"Unmatched: {unmatched_count}, Failed: {failed_count}")

    def close(self):
        info = "Ingestion complete."
        if self.csv_path:
            info += f" Processed CSV file."
        if self.video_path:
            info += f" Processed video file."
        if self.kitsu_project:
            info += f" Pushed to Kitsu project '{self.kitsu_project}'."
        logging.info(info)


def main():
    parser = argparse.ArgumentParser(description='Kitsu Ingest Tool')

    parser.add_argument('--csv', help='Path to the breakdown CSV file')
    parser.add_argument('-v', '--video', help='Path to the breakdown video file')
    parser.add_argument('-p', '--push', metavar='PROJECT', help='Project name to push to Kitsu')
    parser.add_argument('--push_only', help='Push a folder (path) containing CSV and videos to Kitsu')
    parser.add_argument('--sequence', default='SQ01', help='Sequence name to assign to all shots')

    args = parser.parse_args()

    if args.push_only:
        if args.csv or args.video:
            parser.error("--push_only cannot be used with --csv or --video")
        if not args.push:
            parser.error("--push_only requires --push PROJECT argument")

    if args.video and not args.csv:
        parser.error("--video requires --csv to define shots and frame ranges")

    if not any([args.csv, args.video, args.push_only]):
        parser.error("You must provide at least one of --csv, --csv + --video, or --push_only + --push")

    ingest = KitsuIngest(
        csv_path=args.csv,
        video_path=args.video,
        project_name=args.push,
        sequence=args.sequence,
        push_only=args.push_only,
    )
    ingest.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"{e}")
        exit(1)
