import numpy as np

def calculate_rank(vector):
    a={}
    rank=len(vector)
    for num in sorted(vector):
        if num not in a:
            a[num]=rank
            rank=rank-1
    rank = [a[i] for i in vector]
    return rank

# Function: COPRAS (Complex Proportional Assessment)
def copras_method(dataset, weights, criterion_type):
    X   = np.copy(dataset)/1.0
    X   = X/np.sum(X, axis = 0)
    X[np.isnan(X)] = 0
    X   = X*weights
    s_p = np.zeros(X.shape[0])
    s_m = np.zeros(X.shape[0])
    s_d = np.zeros(X.shape[0])
    q_i = np.zeros(X.shape[0])
    u_i = np.zeros(X.shape[0])
    id1 = [i for i, j in enumerate(criterion_type) if j == 'max']
    id2 = [i for i, j in enumerate(criterion_type) if j == 'min']
    print(id2, id1)
    if (len(id1) > 0):
        s_p = np.sum(X[:,id1], axis = 1)
        q_i = s_p

    if (len(id2) > 0):
        s_m = np.sum(X[:,id2], axis = 1)
        s_d = np.min(s_m)/s_m
        q_i = s_p + (np.min(s_m)*np.sum(s_m))/(s_m*np.sum(s_d))

    u_i = q_i/np.max(q_i)
    flow = np.copy(u_i)
    flow = np.reshape(flow, (u_i.shape[0], 1))
    flow = np.insert(flow, 0, list(range(1, u_i.shape[0]+1)), axis = 1)
    vector = flow[:,1]
    rank = calculate_rank(vector)

    return rank
