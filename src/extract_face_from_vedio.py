import cv2
import os
import time

from PIL import Image

from src.mtcnn_wrapper import FaceDetector
from src.async_processer import *

class ExtractFaceFromVideo:
    def __init__(self):
        pass

    @staticmethod
    def extract_face_from_video(video_path: str, output_path: str = None, passing: int = 10):

        async_processer = AsyncProcessor(max_workers=4)

        if not os.path.exists(video_path):
            print("Error: Video file not found.")
            return  # 추가

        if output_path is None:
            file_name = os.path.splitext(os.path.basename(video_path))[0]
            output_path = os.path.join(os.path.dirname(video_path),
                                       f"{file_name}_frames")

        os.makedirs(output_path, exist_ok=True)  # 추가

        detector = FaceDetector()

        frame_extractor = ExtractFaceFromVideo.extractor(video_path)

        if frame_extractor is None:
            print("Error: Could not extract frames.")
            return

        frame_count = 0

        try:  # 예외 처리 추가
            workstate: WorkState = None

            while True:
                frame = frame_extractor()

                if frame is None:
                    break

                if frame_count % passing != 0:
                    frame_count += 1
                    continue

                # BGR -> RGB 변환
                frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

                # NumPy 배열을 PIL 이미지로 변환
                pil_image = Image.fromarray(frame_rgb)

                # 파일명을 포함한 전체 경로 생성
                output_file_path = os.path.join(output_path, f"frame_{frame_count:05d}.jpg")

                faces = detector.process_image(pil_image)

                for face_idx, face_image in enumerate(faces):
                    def save_temp(face_image=face_image, face_idx=face_idx, frame_count=frame_count):
                        output_file_path = os.path.join(output_path, f"frame_{frame_count:05d}_face_{face_idx}.jpg")
                        face_image.save(output_file_path)
                        print(f"Saved frame {frame_count} face {face_idx} to {output_file_path}")

                    workstate = async_processer.submit(save_temp)
                    # face_image.save(os.path.join(output_path, f"frame_{frame_count:05d}_face_{face_idx}.jpg"))

                frame_count += 1
            print("추출 완료")

            while not workstate.state == ProcessState.DONE:
                time.sleep(0.1)
                print(f"Workstate: {workstate.state}")
            print("저장 완료")

        except Exception as e:
            print(f"Error processing video: {e}")
        finally:
            # 리소스 정리는 extractor 함수에서 처리해야 함
            pass

    @staticmethod
    def extractor(video_path: str):
        if not os.path.exists(video_path):
            print("Error: Video file not found.")
            return None

        # Load the video
        video = cv2.VideoCapture(video_path)

        if not video.isOpened():  # 추가
            print("Error: Could not open video file.")
            return None

        def get_frame():
            success, frame = video.read()
            if success:
                return frame
            else:
                video.release()  # 비디오 끝나면 리소스 해제
                return None

        return get_frame

    @staticmethod
    def video_to_frames(video_path: str, output_path: str = None, passing: int = 10, prefix='frame'):
        # Create output directory if it doesn't exist

        if output_path is None:
            file_name = os.path.splitext(os.path.basename(video_path))[0]
            output_path = os.path.join(os.path.dirname(video_path),
                                       f"{file_name}_frames")

        os.makedirs(output_path, exist_ok=True)

        # Initialize video capture
        video = cv2.VideoCapture(video_path)

        if not video.isOpened():
            print("Error: Could not open video file.")
            return

        fps = video.get(cv2.CAP_PROP_FPS)
        frame_count = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = frame_count / fps

        start_time = time.time()
        count = 0

        while True:
            success, frame = video.read()

            if not success:
                break

            if count % passing != 0:
                count += 1
                continue

            frame_output_path = os.path.join(output_path, f"{prefix}_{count:05d}.jpg")
            cv2.imwrite(frame_output_path, frame)
            count += 1

            if count % 100 == 0:
                print(f"Processed {count} frames...")


        video.release()

        end_time = time.time()

        print(f"Processed {count} frames in {end_time - start_time:.2f} seconds.")