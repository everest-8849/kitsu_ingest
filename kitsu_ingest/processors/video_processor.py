import os
import logging
import ffmpeg


class VideoProcessor:
    def __init__(self, video_path, shots_data, output_dir):
        self.video_path = video_path
        self.shots_data = shots_data
        self.output_dir = output_dir
        self.processed_files = []

    def process(self):
        input_stream = ffmpeg.input(self.video_path)
        last_frame = 0

        logging.info(f"Processing video: {self.video_path}")
        logging.info(f"Found {len(self.shots_data)} shots to process")

        for idx, (shot_name, (length, fps)) in enumerate(self.shots_data.items(), 1):
            start_frame = last_frame
            end_frame = last_frame + length

            trimmed = (
                input_stream.video
                .trim(start_frame=start_frame, end_frame=end_frame)
                .setpts('PTS-STARTPTS')
            )

            output_path = os.path.join(self.output_dir, f"{shot_name}.mp4")

            try:
                logging.info(f"Processing shot {idx}/{len(self.shots_data)}: {shot_name} ({start_frame}→{end_frame})")

                (
                    ffmpeg
                    .output(
                        trimmed,
                        output_path,
                        vcodec='libx264',
                        pix_fmt='yuv420p',
                        crf=18
                    )
                    .overwrite_output()
                    .run(quiet=True)
                )
                logging.info(f"Exported: {output_path}")
                self.processed_files.append(output_path)
            except ffmpeg.Error as e:
                logging.warning(
                    f"Failed to export {shot_name}: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")

            last_frame = end_frame

        logging.info(f"Video processing complete. Exported {len(self.processed_files)} shots.")
        return self.processed_files