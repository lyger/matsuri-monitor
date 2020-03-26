import multiprocessing as mp

manager = mp.Manager()

archives = manager.list()

lives = manager.dict()

api_key = mp.Array('u', 40)
