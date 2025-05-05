import os
import gazu
import logging
import pandas as pd


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
    split_cols = df['SHOT'].str.split('_', expand=True)

    # Add temporary columns for sorting
    df['sort_prefix'] = split_cols[0]
    df['sort_number'] = pd.to_numeric(split_cols[1], errors='coerce')

    # Sort first by prefix alphabetically, then by number ascending
    df = df.sort_values(by=['sort_prefix', 'sort_number'], ascending=[True, True])

    # Drop the helper columns
    df = df.drop(columns=['sort_prefix', 'sort_number'])

    return df.copy()

# fetch the latest CSV file from a folder
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

        return csv_files[0]

    else:
        raise FileNotFoundError(f"[fetch_csv_from_folder] Path does not exist: {path}")


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


def build_data_dicts(shots_from_sequence, processed_csv_path):
    kitsu_data = {
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
    local_data = {
        row["Name"]: {
            "frame_in": int(row["Frame In"]),
            "frame_out": int(row["Frame Out"]),
            "nb_frames": int(row["Nb Frames"]),
            "fps": float(row["FPS"]),
            "description": row["Description"]
        }
        for _, row in df.iterrows()
    }

    return kitsu_data, local_data


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