import multiprocessing as mp
import threading
from typing import List, Union


def run_multi_processes(
    processes_num: int,
    target_function,
    index_sep_char: str = "_",
    daemon: bool = True,
) -> List[mp.Process]:
    processes = []
    for i in range(1, processes_num + 1):
        crawler_process = mp.Process(target=target_function, args=(f"sc-crawler{index_sep_char}{i}",))
        crawler_process.daemon = daemon
        processes.append(crawler_process)

    for process in processes:
        process.start()

    if daemon is True:
        for process in processes:
            process.join()

    return processes


def run_2_diff_workers(
    func1_ps: tuple, func2_ps: tuple, worker: str = "thread"
) -> List[Union[threading.Thread, mp.Process]]:
    func1, func1_args, func1_daemon = func1_ps
    func2, func2_args, func2_daemon = func2_ps

    workers = []
    if worker == "thread":
        func1_worker = threading.Thread(target=func1, args=(func1_args or ()))
        func2_worker = threading.Thread(target=func2, args=(func2_args or ()))
    elif worker == "process":
        func1_worker = mp.Process(target=func1, args=(func1_args or ()))
        func2_worker = mp.Process(target=func2, args=(func2_args or ()))
    else:
        raise ValueError(f"Doesn't support worker type {worker}.")
    func1_worker.daemon = func1_daemon
    func2_worker.daemon = func2_daemon
    workers.append(func1_worker)
    workers.append(func2_worker)

    func1_worker.start()
    func2_worker.start()

    if func1_daemon is False:
        func1_worker.join()
    if func2_daemon is False:
        func2_worker.join()

    return workers


def run_multi_diff_workers(funcs: List[tuple], worker: str = "thread") -> List[Union[threading.Thread, mp.Process]]:
    workers = []
    for func, func_args, func_daemon in funcs:
        if worker == "thread":
            func_worker = threading.Thread(target=func, args=(func_args or ()))
        elif worker == "process":
            func_worker = mp.Process(target=func, args=(func_args or ()))
        else:
            raise ValueError(f"Doesn't support worker type {worker}.")
        func_worker.daemon = func_daemon
        workers.append(func_worker)

    for one_worker in workers:
        one_worker.start()

    for index, one_func in enumerate(funcs):
        if one_func[2] is False:
            workers[index].join()

    return workers
