from device import Device

import cv2
import mediapipe as mp
from time import time
import sys
import subprocess


def hand_detect(file, model):
    f = open("hand.jpg", "wb")
    f.write(file)
    f.close()
    with mp.solutions.hands.Hands(
            static_image_mode=True,
            max_num_hands=2,
            min_detection_confidence=0.5) as hands:
        image = cv2.flip(cv2.imread('hand.jpg'), 1)
        results = hands.process(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))

        return results


def video_detect(file, model):
    begin = time()
    f = open("video.mp4", "wb")
    f.write(file)
    f.close()
    print("Time for writing video file to disk:", time() - begin)
    process = subprocess.Popen(['python3', '/local/swjz/yolov5/detect.py', '--source', 'video.mp4', '--weights', '/local/swjz/yolov5/yolov5x6.pt'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    print(stderr)
    print("Total time:", time()-begin)
    return stdout


def image_detect(file, model):
    begin = time()
    f = open("image.jpg", "wb")
    f.write(file)
    f.close()
    print("Time for writing this image file to disk:", time() - begin)
    result = model("image.jpg")
    print("Per frame latency:", time()-begin)
    return result


def main(compressed=False):
    if not len(sys.argv) > 2:
        print('Usage example: device_driver.py dev0 video.mp4')
    # TODO: group_id should also be loaded from file
    # clarification on the usage of group_id?
    group_id = sys.argv[1]
    filename = sys.argv[2]
    print('Initialize device', group_id)
    if compressed:
        with open(filename, 'rb') as file:
            video = file.read()
        device = Device(group_id, {'data0': video})
        device.set_predict_func(video_detect)
    else:
        def imgs():
            video = cv2.VideoCapture(filename)
            while video.isOpened():
                success, frame = video.read()
                if not success:
                    break
                ret, buffer = cv2.imencode('.jpg', frame)
                yield buffer.tobytes()
        device = Device(group_id, {'data0': imgs()})
        device.set_predict_func(image_detect)

    # at this point the device should be spinning in a loop
    # publishing, subscribing, and reading
    # TODO: reading data blocks publishing?
    device.handle_messages()


if __name__ == '__main__':
    main()
