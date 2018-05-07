# _*_ coding: utf-8 _*_
__author__ = 'Thpffcj'

import random
import time

url_path = [
    "http://www.imooc.com/video/8701",
    "http://www.imooc.com/video/8702",
    "http://www.imooc.com/video/8703",
    "http://www.imooc.com/article/8701",
    "http://www.imooc.com/article/8704",
    "http://www.imooc.com/article/8705",
    "http://www.imooc.com/video/8709"
]

ip_slices = [132, 156, 124, 10, 29, 143, 187, 30, 46, 55, 63, 72, 98, 168]


def sample_traffic():
    return random.randint(0, 100)


def sample_url():
    return random.sample(url_path, 1)[0]


def sample_ip():
    slice = random.sample(ip_slices, 4)
    return ".".join([str(item) for item in slice])


def generate_log(count=1000):
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    while count >= 1:
        query_log = "{local_time}\t{url}\t{traffic}\t{ip}".format(local_time=time_str, url=sample_url(), traffic=sample_traffic(), ip=sample_ip())
        print(query_log)
        count = count - 1


if __name__ == '__main__':
    generate_log()