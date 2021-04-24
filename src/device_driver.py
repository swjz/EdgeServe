from device import Device

import cv2
import mediapipe as mp
import sys


def hand_detect(file):
    with mp.solutions.hands.Hands(
            static_image_mode=True,
            max_num_hands=2,
            min_detection_confidence=0.5) as hands:
        image = cv2.flip(cv2.imread(file), 1)
        results = hands.process(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))

        return results


def main():
    if not len(sys.argv) > 1:
        print('Usage example: device_driver.py dev0')
    # TODO: can pass an argument to a JSON file to read in device data
    # for now use kitten data
    with open('/home/azureuser/hand.jpg', 'rb') as file:
        file = file.read()
    # TODO: group_id should also be loaded from file
    # clarification on the usage of group_id?
    group_id = sys.argv[1]
    print('Initialize device', group_id)
    device = Device(group_id, {'data0': file, 'data1': file, 'data2': file})
    device.set_predict_func(hand_detect)

    # at this point the device should be spinning in a loop
    # publishing, subscribing, and reading
    # TODO: reading data blocks publishing?
    device.handle_messages()

if __name__ == '__main__':
    main()
