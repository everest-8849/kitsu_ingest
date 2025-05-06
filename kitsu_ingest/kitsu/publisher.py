import os
import logging
import gazu
from .auth import kitsu_login
from ..utils.validation import safety_check_kitsu_vs_local_mp4, safety_check_matching_metadata, build_data_dicts, \
    fetch_shot_name_from_tasks

TASK_TYPE_NAME = "From EVEREST"
TASK_STATUS_NAME = "Done"


class KitsuPublisher:
    def __init__(self, project_name, sequence_name):
        self.project_name = project_name
        self.sequence_name = sequence_name
        self.project = None
        self.sequence = None
        self.kitsu_data = None
        self.local_data = None

    def connect(self):
        kitsu_login()
        try:
            self.project = gazu.project.get_project_by_name(self.project_name)
            self.sequence = gazu.shot.get_sequence_by_name(self.project, self.sequence_name)
            logging.info(f"Connected to project '{self.project_name}', sequence '{self.sequence_name}'")
            return True
        except gazu.exception.RouteNotFoundException as e:
            logging.warning(f"Project '{self.project_name}' or sequence not found: {e}")
            return False

    def import_shots_from_csv(self, csv_path):
        logging.info(f"Importing shots from CSV: {csv_path}")
        gazu.shot.import_shots_with_csv(self.project, csv_path)
        logging.info("Shot import complete")

    def publish_previews(self, output_dir):
        logging.info("Preparing to publish previews...")
        task_type = gazu.task.get_task_type_by_name(TASK_TYPE_NAME)
        task_status = gazu.task.get_task_status_by_name(TASK_STATUS_NAME)

        shots_from_sequence = gazu.shot.all_shots_for_sequence(self.sequence)
        all_tasks = gazu.task.all_tasks_for_project(self.project, task_type)
        shot_task_map = fetch_shot_name_from_tasks(all_tasks)

        mp4_files = [f for f in os.listdir(output_dir) if f.endswith(".mp4")]
        logging.info(f"Found {len(mp4_files)} MP4 files to publish")

        # Build data dictionaries for validation
        processed_csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
        if processed_csv_files:
            processed_csv_path = os.path.join(output_dir, processed_csv_files[0])
            self.kitsu_data, self.local_data = build_data_dicts(shots_from_sequence, processed_csv_path)

            # Perform safety checks
            safety_check_kitsu_vs_local_mp4(self.kitsu_data, mp4_files)
            safety_check_matching_metadata(self.kitsu_data, self.local_data)
        else:
            raise FileNotFoundError("No CSV file found for validation. Process aborted.")

        stats = {"matched": 0, "unmatched": 0, "failed": 0}

        for file_name in mp4_files:
            shot_name = os.path.splitext(file_name)[0]
            task = shot_task_map.get(shot_name)

            if not task:
                logging.warning(f"No matching Kitsu task found for shot: {shot_name}")
                stats["unmatched"] += 1
                continue

            video_path = os.path.join(output_dir, file_name)
            if not os.path.exists(video_path):
                logging.warning(f"Video file not found: {video_path}")
                stats["unmatched"] += 1
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
                stats["matched"] += 1
            except Exception as e:
                logging.error(f"Failed to publish preview for {shot_name}: {e}")
                stats["failed"] += 1

        return stats