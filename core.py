import argparse
import logging
from .processors.csv_processor import CsvProcessor
from .processors.video_processor import VideoProcessor
from .kitsu.publisher import KitsuPublisher
from .utils.validation import extract_shots, fetch_csv_from_folder

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

class Workflow:
    def __init__(self, args):
        self.args = args
        self.output_dir = None

    def run(self):
        if self.args.push_only:
            self.output_dir = self.args.push_only
            csv_path = fetch_csv_from_folder(self.args.push_only)

            publisher = KitsuPublisher(self.args.push, self.args.sequence)
            if publisher.connect():
                publisher.import_shots_from_csv(csv_path)
                stats = publisher.publish_previews(self.output_dir)
                logging.info(f"Finished publishing previews. "
                             f"Matched: {stats['matched']}, "
                             f"Unmatched: {stats['unmatched']}, "
                             f"Failed: {stats['failed']}")
        else:
            # Process CSV
            csv_processor = CsvProcessor(self.args.csv, self.args.sequence)
            processed_csv_path = csv_processor.process()
            self.output_dir = csv_processor.output_dir

            # Process video if provided
            if self.args.video:
                shots = extract_shots(csv_processor.df)
                video_processor = VideoProcessor(self.args.video, shots, self.output_dir)
                video_processor.process()

            # Push to Kitsu if requested
            if self.args.push:
                publisher = KitsuPublisher(self.args.push, self.args.sequence)
                if publisher.connect():
                    publisher.import_shots_from_csv(processed_csv_path)
                    stats = publisher.publish_previews(self.output_dir)
                    logging.info(f"Finished publishing previews. "
                                 f"Matched: {stats['matched']}, "
                                 f"Unmatched: {stats['unmatched']}, "
                                 f"Failed: {stats['failed']}")


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

    workflow = Workflow(args)
    workflow.run()