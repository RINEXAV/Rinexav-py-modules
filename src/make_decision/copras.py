# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 10:05:23 2022

@author: filip
"""

# Required Libraries
import matplotlib.pyplot as plt
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


# COPRAS

# Weights
#weights = np.array([0.28, 0.14, 0.05, 0.24, 0.19, 0.05, 0.05])

# Load Criterion Type: 'max' or 'min'
#criterion_type = ['max', 'max', 'max', 'min', 'min', 'min', 'min']

# Dataset
#dataset = np.array([
                    # [75.5, 420,	 74.2,	2.8,	21.4,	0.37,  	0.16],   #a1
                    # [95,   91,	 70,	  2.68,	22.1,	0.33,	  0.16],   #a2
                    # [770,  1365, 189,	  7.9,	16.9,	0.04,	  0.08],   #a3
                    # [187,  1120, 210,	  7.9,	14.4,	0.03,	  0.08],   #a4
                    # [179,  875,	 112,	  4.43,	9.4,	0.016,	0.09],   #a5
                    # [239,  1190, 217,	  8.51,	11.5,	0.31,	  0.07],   #a6
                    # [273,  200,	 112,	  8.53,	19.9,	0.29,	  0.06]    #a7
                    # ])

# Call COPRAS Function
#rank = copras_method(dataset, weights, criterion_type)