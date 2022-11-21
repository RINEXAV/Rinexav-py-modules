import numpy as np

def floater(a):
    b = []
    for i in a:
        try:
            ix = []
            for j in i:
                ix.append(float(j))
        except:
            ix = float(i)
            pass
        b.append(ix)
    b = np.array(b)
    return b

def normalize(matrix):
    r = np.empty((matrix.shape[0], matrix.shape[1]), np.float64)
    for j in range(matrix.shape[1]):
        sq = np.sqrt(sum(matrix[:, j]**2))
        for i in range(matrix.shape[0]):
            if sq != 0:
                r[i, j] = matrix[i, j]/sq
            else:
                r[i, j] = 0
    return r


def weight_product(matrix, weight):
    r = matrix * weight
    return r


def calc_ideal_best_worst(sign, matrix, n, m):
    ideal_worst = []
    ideal_best = []
    for i in range(m):
        if sign[i] == 1:
            ideal_worst.append(min(matrix[:, i]))
            ideal_best.append(max(matrix[:, i]))
        else:
            ideal_worst.append(max(matrix[:, i]))
            ideal_best.append(min(matrix[:, i]))
    return (ideal_worst, ideal_best)


def euclidean_distance(matrix, ideal_worst, ideal_best, n, m):
    diw = (matrix - ideal_worst)**2
    dib = (matrix - ideal_best)**2
    dw = []
    db = []
    for i in range(n):
        dw.append(sum(diw[i, :])**0.5)
        db.append(sum(dib[i, :])**0.5)
    dw = np.array(dw)
    db = np.array(db)
    return (dw, db)


def performance_score(distance_best, distance_worst, n, m):
    score = []
    war = distance_best + distance_worst
    if war[0] == 0:
        score = [0]
    else:
        score = distance_worst/(distance_best + distance_worst)
    return score

def calculate_rank(vector):
    a={}
    rank=len(vector)
    for num in sorted(vector):
        if num not in a:
            a[num]=rank
            rank=rank-1
    rank = [a[i] for i in vector]
    return rank
 
        
def topsis(a, w, sign):
    a = floater(a)
    r = normalize(a)
    
    t = weight_product(r, w)
    (ideal_worst, ideal_best) = calc_ideal_best_worst(
        sign, t, a.shape[0], a.shape[1])
    
    (distance_worst, distance_best) = euclidean_distance(
        t, ideal_worst, ideal_best, a.shape[0], a.shape[1])
    score = performance_score(
        distance_best, distance_worst, a.shape[0], a.shape[1])
    rank = calculate_rank(score)
    return rank

