def solution(S, P, Q):
    result = []
    new_s = []
    for g in S:
        new_s.append(get_factor(g))
    for a, b in zip(P, Q):
        result.append(get_smallest(new_s, a, b))
    return result


def get_factor(x):
    return {'A':1, 'C':2, 'G':3, 'T':4}[x]


def get_smallest(S, m, n):
    if m == n:
        return S[m]
    else:
        smallest = min(S[m:n+1])
    return smallest


if __name__ == '__main__':
    S = 'CAGCCTA'
    P = [2, 5, 0]
    Q = [4, 5, 6]
    print(solution(S, P, Q))