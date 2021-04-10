import numpy as np
from collections import defaultdict

from optimizer import Optimizer
from device import Device

np.random.seed(0)

# a toy example as defined in the google doc
M = 2
N = 3
S = np.array(
    [[1,0,1],
     [0,1,0]
     ])
C = np.array([10,3,5])
V = np.array([20,30])
R = np.array([2,1,2])
colocation_list = [[0, 2]]

# a second toy example
M = 4
N = 7
S = np.array([[1, 1, 0, 0, 0, 0, 0],
     [0, 0, 1, 1, 0, 0, 0],
     [0, 0, 0, 0, 1, 1, 0],
     [0, 0, 0, 0, 0, 0, 1]])
C = np.array([100, 10, 100, 10, 100, 10, 15])
V = np.array([350, 350, 350, 15])
R = np.array([3, 3, 3, 3, 3, 3, 3])
colocation_list = [[0, 1], [2, 3], [4, 5]]

def generate_random_inputs(num_devices, num_data):
    S = np.random.randint(0, 2, (M, N))
    C = np.random.randint(10, 21, N)
    R = np.random.randint(5, 7, N)
    V = np.random.randint(100, 151, M)
    colocation_list = [
        list(range(0, num_data // 3, 2)),
        list(range(num_data // 3 * 2, num_data, 2))
        ]
    return (S, C, R, V, colocation_list)

def generate_random_data(source_matrix):
    """
    generate a data dictionary {dev0: {data0: }}
    """
    ret = defaultdict(dict)
    num_devices, num_data = source_matrix.shape
    for idx_device in range(num_devices):
        key_device = 'dev' + str(idx_device)
        for idx_data in range(num_data):
            if source_matrix[idx_device, idx_data] == 1:
                key_data = 'data' + str(idx_data)
                ret[key_device][key_data] = str(np.random.randint(1000))
    return ret

def main():
    optimizer = Optimizer()
    cost, T = optimizer.solve(S, C, R, V, colocation_list)

    num_devices = M
    num_data = N

    data_dict = generate_random_data(S)
    print('Data distribution from source matrix\n', data_dict)

    # initialize kafka services
    # a list of strings, has length num_data
    topics = ['data' + str(data_id) for data_id  in range(num_data)]
    group_ids = ['dev' + str(group_id) for group_id in range(num_devices)]

    # put data into devices
    devices = []
    for group_id in group_ids:
        device = Device(group_id, data_dict[group_id])
        devices.append(device)

    for topic in topics:
        print(topic, 'has', devices[0].consumer.partitions_for_topic(topic), 'partitions')

    routing_matrix = T - S
    subscriber_devices = set()
    # TODO: optimize the loop and reduce redundancy
    for idx_data in range(num_data):
        topic = topics[idx_data]
        for idx_device in range(num_devices):
            if routing_matrix[idx_device, idx_data] == 1: # need to subscribe
                devices[idx_device].subscribe(topic)
                subscriber_devices.add(idx_device)
        # locate one device that has the data at S
        for idx_device in range(num_devices):
            if S[idx_device, idx_data] == 1: # is publisher
                devices[idx_device].publish(topic)

    for idx_device in subscriber_devices:
        devices[idx_device].handle_messages()

if __name__ == '__main__':
    main()
