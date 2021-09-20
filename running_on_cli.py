from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from subprocess import Popen, PIPE
import re
import asyncio
from time import perf_counter
from multiprocessing import Pool


async def get_blkid():
    with Popen("/usr/sbin/blkid", stdout=PIPE, stderr=PIPE, shell=True) as blkid:
        with Popen("grep dev", stdin=blkid.stdout, stdout=PIPE, shell=True) as grep_dev:
            return {
                drive: dict(
                    map(
                        lambda x: (x.split("=")[0], x.split("=")[1].strip('"')),
                        attributes.split(" "),
                    )
                )
                for drive, attributes in map(
                    lambda line: line.decode("utf-8").strip().split(": "),
                    grep_dev.stdout.readlines(),
                )
            }


async def get_df():
    with Popen("/usr/bin/df", stdout=PIPE, stderr=PIPE, shell=True) as df:
        with Popen("grep dev", stdin=df.stdout, stdout=PIPE, shell=True) as grep_dev:
            formatted_output = map(
                lambda x: re.sub(r" +", " ", x.decode("utf-8").strip()),
                grep_dev.stdout.readlines(),
            )
    filtered_dev = filter(
        lambda x: "dev" in x.split(" ")[0], formatted_output
    )  # dev should be dev/s
    return {
        each_drive.split(" ")[0]: each_drive.split(" ")[1]
        for each_drive in filtered_dev
    }


def get_df2():
    with Popen("/usr/bin/df", stdout=PIPE, stderr=PIPE, shell=True) as df:
        with Popen("grep dev", stdin=df.stdout, stdout=PIPE, shell=True) as grep_dev:
            formatted_output = map(
                lambda x: re.sub(r" +", " ", x.decode("utf-8").strip()),
                grep_dev.stdout.readlines(),
            )
    filtered_dev = filter(
        lambda x: "dev" in x.split(" ")[0], formatted_output
    )  # dev should be dev/s
    return {
        each_drive.split(" ")[0]: each_drive.split(" ")[1]
        for each_drive in filtered_dev
    }


def get_blkid2():
    with Popen("/usr/sbin/blkid", stdout=PIPE, stderr=PIPE, shell=True) as blkid:
        with Popen("grep dev", stdin=blkid.stdout, stdout=PIPE, shell=True) as grep_dev:
            return {
                drive: dict(
                    map(
                        lambda x: (x.split("=")[0], x.split("=")[1].strip('"')),
                        attributes.split(" "),
                    )
                )
                for drive, attributes in map(
                    lambda line: line.decode("utf-8").strip().split(": "),
                    grep_dev.stdout.readlines(),
                )
            }


async def main():
    a = await get_blkid()
    b = await get_df()
    return a, b


async def main2():
    task1 = asyncio.create_task(get_blkid())
    task2 = asyncio.create_task(get_df())
    value1 = await task1
    value2 = await task2
    return value1, value2


def start_asyncio(rounding):
    start0 = perf_counter()
    output = asyncio.run(main2())
    print(output)
    print(round(perf_counter() - start0, rounding))


def start_threadpool(rounding):
    start1 = perf_counter()
    tasks = [get_df2, get_blkid2]
    with ThreadPoolExecutor() as executor:
        fs = map(lambda x: executor.submit(x), tasks)
        output = list(map(lambda x: x.result(), fs))
    # print(output)
    print(round(perf_counter() - start1, rounding))


def start_processpool(rounding):
    start2 = perf_counter()
    tasks = [get_df2, get_blkid2]
    with ProcessPoolExecutor() as executor:
        fs = map(lambda x: executor.submit(x), tasks)
        output = list(map(lambda x: x.result(), fs))
    # print(output)
    print(round(perf_counter() - start2, rounding))
    
def __fstab_details(each_line):
    text_tab_split = each_line.split("\t")
    e = text_tab_split[0].split("=")
    c = {e[0]:e[1]}
    d = {"mount_location": text_tab_split[1]}
    f = {"format": text_tab_split[2]}
    g = text_tab_split[3:]
    return c, d, f, g

def get_fstab():
    with Popen("cat /etc/fstab", stdout=PIPE, stderr=PIPE, shell=True) as fstab:
        z = fstab.stdout.readlines()
    y = map(lambda x: __fstab_details(x.decode("utf-8").strip()), z)
    return list(y)

get_fstab()
