import datetime


def runtime(some_function):
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now()
        result = some_function(*args, **kwargs)
        print(f"Runtime is: {datetime.datetime.now() - start_time} seconds")
        return result

    return wrapper
