import sys

from device import Device

def main():
    if not len(sys.argv) > 1:
        print('Usage example: device_driver.py dev0')
    # TODO: can pass an argument to a JSON file to read in device data
    # for now use kitten data
    with open('/home/azureuser/kitten_small.jpg', 'rb') as file:
        kitten_file = file.read()
    # TODO: group_id should also be loaded from file
    # clarification on the usage of group_id?
    group_id = sys.arg
    device = Device(group_id, kitten_file)

    # at this point the device should be spinning in a loop
    # publishing, subscribing, and reading
    # TODO: reading data blocks publishing?
    device.handle_messages()

if __name__ == '__main__':
    main()
