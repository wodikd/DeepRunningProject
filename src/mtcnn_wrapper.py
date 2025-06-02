import os
import glob
from pathlib import Path
import math

import torch
import numpy as np
from PIL import Image
import cv2
from facenet_pytorch import MTCNN

class FaceDetector:
    """
    MTCNN을 사용한 얼굴 탐지 및 처리 클래스 (눈 정렬 기능 포함)
    """
    def __init__(self, image_size=224, margin=20, min_face_size=20, 
                 thresholds=[0.6, 0.7, 0.7], prob_threshold=0.9, 
                 align_faces=False, device=None):
        """
        FaceDetector 초기화
        
        Args:
            image_size (int): 출력 이미지 크기
            margin (int): 얼굴 주변 여백 (픽셀)
            min_face_size (int): 감지할 최소 얼굴 크기
            thresholds (list): MTCNN 단계별 임계값
            prob_threshold (float): 얼굴 감지 확률 임계값
            align_faces (bool): 눈 위치 기준 얼굴 정렬 여부
            device (str, optional): 연산 장치 ('cpu' 또는 'cuda'). None이면 자동 감지.
        """
        # 장치 설정
        if device is None:
            self.device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
        else:
            self.device = torch.device(device)
            
        print(f'Running on device: {self.device}')
        
        # 설정 저장
        self.image_size = image_size
        self.margin = margin
        self.prob_threshold = prob_threshold
        self.align_faces = align_faces
        
        # MTCNN 모델 초기화 (호환성 개선)
        self.mtcnn = MTCNN(
            image_size=image_size,
            margin=margin,
            min_face_size=min_face_size,
            thresholds=thresholds,
            factor=0.709,
            post_process=True,
            device=self.device,
            keep_all=True  # 모든 얼굴 반환
        )
    
    def detect_faces_with_landmarks(self, img):
        """
        이미지에서 얼굴과 랜드마크 감지
        
        Args:
            img (PIL.Image): 입력 이미지
            
        Returns:
            tuple: (boxes, probs, landmarks) - 경계 상자, 확률값, 랜드마크
        """
        # MTCNN detect 메서드 호출 (landmarks=True 옵션 사용)
        boxes, probs, landmarks = self.mtcnn.detect(img, landmarks=True)
        return boxes, probs, landmarks
    
    def align_face_by_eyes(self, img_array, landmarks):
        """
        눈 위치 기준 얼굴 정렬
        
        Args:
            img_array (numpy.ndarray): 이미지 배열 (H, W, 3)
            landmarks (numpy.ndarray): 얼굴 랜드마크 좌표 (5, 2)
            
        Returns:
            numpy.ndarray: 정렬된 이미지 배열
        """
        try:
            # MTCNN 랜드마크 순서: [left_eye, right_eye, nose, mouth_left, mouth_right]
            left_eye = landmarks[0]   # 왼쪽 눈
            right_eye = landmarks[1]  # 오른쪽 눈
            
            # 눈 사이 각도 계산
            dy = right_eye[1] - left_eye[1]
            dx = right_eye[0] - left_eye[0]
            angle = math.degrees(math.atan2(dy, dx))
            
            # 이미지 중심점
            center = (img_array.shape[1] // 2, img_array.shape[0] // 2)
            
            # 회전 변환 행렬
            M = cv2.getRotationMatrix2D(center, angle, 1.0)
            
            # 회전 적용
            aligned = cv2.warpAffine(img_array, M, (img_array.shape[1], img_array.shape[0]))
            
            return aligned
            
        except Exception as e:
            print(f"  Warning: Face alignment failed ({str(e)}), using original image")
            return img_array
    
    def process_image(self, img, save_path=None):
        """
        단일 이미지에서 얼굴 감지 및 처리 (정렬 포함)
        
        Args:
            img (PIL.Image 또는 str): 입력 이미지 또는 이미지 경로
            save_path (str, optional): 저장할 경로. None이면 저장하지 않음.
            
        Returns:
            list: 감지된 얼굴 이미지 리스트 (PIL.Image)
        """
        # 문자열이면 이미지 로드
        if isinstance(img, str):
            # RGB 모드로 이미지 로드 (알파 채널 제거)
            img = Image.open(img).convert('RGB')
            
        # 얼굴 및 랜드마크 감지
        boxes, probs, landmarks = self.detect_faces_with_landmarks(img)
        
        face_images = []
        
        if boxes is not None and len(boxes) > 0:
            # 각 얼굴 처리
            for i in range(len(boxes)):
                box = boxes[i]
                prob = probs[i] if probs is not None else 1.0
                landmark = landmarks[i] if landmarks is not None else None
                
                if prob > self.prob_threshold:
                    # print(f'  Face {i+1} detected with probability: {prob:.4f}')
                    
                    # 직접 크롭 및 리사이징
                    x1, y1, x2, y2 = [int(b) for b in box]
                    
                    # 마진 추가 (이미지 경계 확인)
                    x1 = max(0, x1 - self.margin)
                    y1 = max(0, y1 - self.margin)
                    x2 = min(img.width, x2 + self.margin)
                    y2 = min(img.height, y2 + self.margin)
                    
                    # 얼굴 영역 크롭
                    face_img = img.crop((x1, y1, x2, y2))
                    
                    # 크기 조정
                    face_img = face_img.resize((self.image_size, self.image_size), Image.BILINEAR)
                    
                    # 눈 정렬 수행 (옵션이 활성화된 경우)
                    if self.align_faces and landmark is not None:
                        # PIL Image를 numpy array로 변환
                        face_array = np.array(face_img)
                        
                        # 랜드마크 좌표를 크롭된 이미지 기준으로 조정
                        adjusted_landmarks = landmark.copy()
                        adjusted_landmarks[:, 0] -= x1  # x 좌표 조정
                        adjusted_landmarks[:, 1] -= y1  # y 좌표 조정
                        
                        # 크기 조정에 따른 랜드마크 스케일링
                        scale_x = self.image_size / (x2 - x1)
                        scale_y = self.image_size / (y2 - y1)
                        adjusted_landmarks[:, 0] *= scale_x
                        adjusted_landmarks[:, 1] *= scale_y
                        
                        # 눈 정렬 수행
                        aligned_face = self.align_face_by_eyes(face_array, adjusted_landmarks)
                        
                        # numpy array를 PIL Image로 변환
                        face_img = Image.fromarray(aligned_face)
                        
                        # print(f'    ✅ Face aligned using eye landmarks')
                    
                    # 리스트에 추가
                    face_images.append(face_img)
                    
                    # 저장이 필요하면
                    if save_path:
                        base_path, ext = os.path.splitext(save_path)
                        # 여러 얼굴이 있는 경우 번호 추가
                        if len(boxes) > 1:
                            output_path = f"{base_path}_face{i+1}{ext}"
                        else:
                            output_path = save_path
                            
                        # 디렉토리 확인
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        
                        # 이미지 저장
                        face_img.save(output_path)
                        # print(f'  Saved to {output_path}')
                else:
                    # print(f'  Face {i+1} detected but below threshold: {prob:.4f}')
                    pass
        else:
            # print(f'  No faces detected')
            pass
            
        return face_images
    
    def process_directory(self, input_dir, output_dir, recursive=False, 
                          extensions=["jpg", "jpeg", "png"]):
        """
        디렉토리 내 모든 이미지 처리
        
        Args:
            input_dir (str): 입력 이미지 디렉토리 경로
            output_dir (str): 출력 이미지 디렉토리 경로
            recursive (bool): 하위 디렉토리 처리 여부
            extensions (list): 처리할 이미지 확장자 목록
        
        Returns:
            int: 처리된 이미지 수
        """
        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)
        
        # 정렬 옵션 출력
        alignment_status = "✅ Enabled" if self.align_faces else "❌ Disabled"
        print(f'Face alignment: {alignment_status}')
        
        # 이미지 파일 목록 가져오기
        image_paths = []
        
        # 확장자 처리
        for ext in extensions:
            if not ext.startswith('.'):
                ext = f'.{ext}'
            
            # 대소문자 모두 처리
            pattern = f'**/*{ext}' if recursive else f'*{ext}'
            image_paths.extend(list(Path(input_dir).glob(pattern)))
            
            # 대문자 확장자도 검색
            pattern = f'**/*{ext.upper()}' if recursive else f'*{ext.upper()}'
            image_paths.extend(list(Path(input_dir).glob(pattern)))
        
        print(f'Found {len(image_paths)} images in {input_dir}')
        
        # 처리된 이미지 수
        processed_count = 0
        
        # 각 이미지 처리
        for idx, img_path in enumerate(image_paths):
            try:
                img_path_str = str(img_path)
                img_name = os.path.basename(img_path_str)
                img_name_base, img_ext = os.path.splitext(img_name)
                
                print(f'Processing image {idx+1}/{len(image_paths)}: {img_name}')
                
                # 출력 경로 생성
                output_path = os.path.join(output_dir, f"{img_name_base}{img_ext}")
                
                # 이미지 처리
                face_images = self.process_image(img_path_str, output_path)
                
                if face_images:
                    processed_count += 1
                    
            except Exception as e:
                print(f'Error processing {img_path}: {str(e)}')
        
        print(f'Processing complete! Processed {processed_count} images with faces.')
        return processed_count
    
    def process_image_to_class_dir(self, img_path, class_name, output_base_dir):
        """
        이미지를 처리하여 클래스별 디렉토리에 저장
        
        Args:
            img_path (str): 입력 이미지 경로
            class_name (str): 클래스 이름 (저장할 하위 디렉토리)
            output_base_dir (str): 출력 기본 디렉토리
        
        Returns:
            bool: 처리 성공 여부
        """
        # 출력 디렉토리 생성
        output_dir = os.path.join(output_base_dir, class_name)
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            img_name = os.path.basename(img_path)
            img_name_base, img_ext = os.path.splitext(img_name)
            
            # 출력 경로
            output_path = os.path.join(output_dir, f"{img_name_base}{img_ext}")
            
            # RGB 모드로 이미지 로드 (알파 채널 제거)
            img = Image.open(img_path).convert('RGB')
            
            # 이미지 처리
            face_images = self.process_image(img, output_path)
            
            return len(face_images) > 0
            
        except Exception as e:
            print(f'Error processing {img_path} to class {class_name}: {str(e)}')
            return False

    def process_image_to_class(self, frame, class_name, output_base_dir, frame_index):
        """
        프레임을 처리하여 클래스별 디렉토리에 저장

        Args:
            frame (numpy.ndarray): 이미지 프레임
            class_name (str): 클래스 이름 (저장할 하위 디렉토리)
            output_base_dir (str): 출력 기본 디렉토리
            frame_index (int): 현재 프레임 번호

        Returns:
            bool: 처리 성공 여부
        """
        try:
            # 출력 디렉토리 생성
            output_dir = os.path.join(output_base_dir, class_name)
            os.makedirs(output_dir, exist_ok=True)

            # 프레임을 PIL 이미지로 변환
            img = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))

            # 저장 경로
            output_path = os.path.join(output_dir, f"frame_{frame_index}.jpg")

            # 이미지 저장
            img.save(output_path)
            return True

        except Exception as e:
            print(f"Error processing frame {frame_index} to class {class_name}: {str(e)}")
            return False


# 사용 예시
if __name__ == "__main__":
    # 경로 설정 예시
    base_dir = Path(__file__).parent.parent.parent  # 프로젝트 루트 디렉토리
    input_dir = os.path.join(base_dir, 'data', 'raw')
    output_dir = os.path.join(base_dir, 'data', 'processed')
    
    # 기본 사용법 예시 (눈 정렬 활성화)
    detector = FaceDetector(image_size=224, align_faces=True)
    detector.process_directory(input_dir, output_dir)