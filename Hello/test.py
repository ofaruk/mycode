# you can write to stdout for debugging purposes, e.g.
# print("this is a debug message")

from collections import Counter


# def solution(S):
#     # write your code in Python 3.6
#
#     str_dict = Counter(S)
#     occ_list = []
#
#     for key, value in str_dict.items():
#         occ_list.append(value)
#
#     return b(occ_list)


to_del = 0


def b(lst):
    st = []
    global to_del
    for i, item in enumerate(lst):
        if i == 0:
            st.append(item)
        else:
            if item in st:
                st.append(item - 1)
                to_del += 1
            else:
                st.append(item)
    return st


x = b([1, 2, 3, 4, 1, 1, 1, 4, 2])

print(x)
