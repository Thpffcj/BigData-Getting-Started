# _*_ coding: utf-8 _*_
__author__ = 'Thpffcj'

import random
import time

infos = [
    "116.397026,39.918058",
    "116.410886,39.881949",
    "116.272876,39.99243",
    "116.544079,40.417555",
    "116.225404,40.258186",
    "116.38631,39.937209",
    "116.399466,39.989743"
]

phones = [
    "13888888888", "13877777777", "13866666666",
    "13988888888", "13977777777", "13966666666",
    "13788888888", "13777777777", "13766666666",
    "13688888888", "13677777777", "13666666666",
]

def sample_phone():
    return random.sample(phones, 1)[0]


def sample_info():
    return random.sample(infos, 1)[0]


def generate_log(count = 3):
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    f = open("/home/thpffcj/data/logs/access.log", "a+")
    while count >= 1:
        query_log = "{phone}\t{info}\t[{local_time}]".format(phone = sample_phone(),
                                                             info = sample_info(), local_time = time_str)
        # print(query_log)
        f.write(query_log + "\n")
        count = count - 1


if __name__ == '__main__':
    generate_log(10)