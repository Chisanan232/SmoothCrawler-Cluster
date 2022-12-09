from typing import List
import multiprocessing as mp
import threading


def run_multi_processes(processes_num: int, target_function, index_sep_char: str = "_", daemon: bool = True) -> List[mp.Process]:
    processes = []
    for i in range(1, processes_num + 1):
        _crawler_process = mp.Process(target=target_function, args=(f"sc-crawler{index_sep_char}{i}",))
        _crawler_process.daemon = daemon
        processes.append(_crawler_process)

    for _process in processes:
        _process.start()

    if daemon is True:
        for _process in processes:
            _process.join()

    return processes


def run_2_diff_workers(func1_ps: tuple, func2_ps: tuple, worker: str = "thread") -> List[mp.Process]:
    func1, func1_args, func1_daemon = func1_ps
    func2, func2_args, func2_daemon = func2_ps

    workers = []
    if worker == "thread":
        _func1_worker = threading.Thread(target=func1, args=(func1_args or ()))
        _func2_worker = threading.Thread(target=func2, args=(func2_args or ()))
    elif worker == "process":
        _func1_worker = mp.Process(target=func1, args=(func1_args or ()))
        _func2_worker = mp.Process(target=func2, args=(func2_args or ()))
    else:
        raise ValueError(f"Doesn't support worker type {worker}.")
    _func1_worker.daemon = func1_daemon
    _func2_worker.daemon = func2_daemon
    workers.append(_func1_worker)
    workers.append(_func2_worker)

    _func1_worker.start()
    _func2_worker.start()

    if func1_daemon is False:
        _func1_worker.join()
    if func2_daemon is False:
        _func2_worker.join()

    return workers
