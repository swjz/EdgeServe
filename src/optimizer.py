import numpy as np
import cvxpy as cp

class Optimizer:
    # TODO: save state?
    def __init__(self):
        pass

    def solve(self, source_matrix, size_vec, replication_vec, storage_vec, colocation_list):
        """
        returns (cost, transmit_matrix)
        """
        transmit_matrix = cp.Variable(source_matrix.shape, boolean=True)
        objective = cp.Minimize(cp.sum((transmit_matrix - source_matrix) @ size_vec))
        constraints = [
            transmit_matrix >= source_matrix,
            cp.sum(transmit_matrix, axis=0) >= replication_vec,
            cp.matmul(transmit_matrix, size_vec) <= storage_vec
            ]
        for colocate in colocation_list:
            for i in range(len(colocate) - 1):
                col1 = colocate[i]
                col2 = colocate[i + 1]
                constraints.append(transmit_matrix[:, col1] == transmit_matrix[:, col2])
        prob = cp.Problem(objective, constraints)
        return prob.solve(), transmit_matrix.value
