from optimizer import Optimizer

# toy example for 2 devices
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

def main():
    optimizer = Optimizer()
    # TODO: we assume here with the toy example that the optimizer
    # already knows the status of the devices, i.e., number of devices
    # out there, their storage etc.
    # in reality we might want some QUERY KEY in the STATUS channel
    # that's why optimizer is also a subscriber of the STATUS channel
    # and not just a publisher
    cost, T = optimizer.solve(S, C, R, V, colocation_list)
    print('The cost of the solution is', cost)
    source_matrix = S
    routing_matrix = T - S

    # initialize topics
    num_devices = M
    num_data = N
    topics = ['data' + str(data_id) for data_id  in range(num_data)]
    group_ids = ['dev' + str(group_id) for group_id in range(num_devices)]

    # at this point, optimizer knows from the routing matrix
    # which device should publish or subscribe to which topic
    # it can send the topic name as a keyed message through the STATUS
    # channel to the device
    subscriber_devices = set()
    # TODO: optimize the loop and reduce redundancy
    for idx_data in range(num_data):
        topic = topics[idx_data]
        for idx_device in range(num_devices):
            if routing_matrix[idx_device, idx_data] == 1:
                # send message thru the STATUS channel telling device to subscribe
                optimizer.notify_publish(group_ids[idx_device], topic)
                subscriber_devices.add(idx_device)
        # locate one device that has the data at S
        for idx_device in range(num_devices):
            if source_matrix[idx_device, idx_data] == 1:
                # send message thru the STATUS channel telling device to publish
                devices[idx_device].publish(topic)

    # send message thru the STATUS channel telling device to read
    for idx_device in subscriber_devices:
        devices[idx_device].handle_messages()

    # optimizer and spin, listen to device status report from STATUS
    # and dynamically re-solve the optimzation problem


if __name__ == '__main__':
    main()
