from threading import Thread


class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None, args=None, kwargs=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None
        self._target = target
        self._args = args
        self._kwargs = kwargs
    ex = None

    def run(self):
        if self._target is not None:
            try:
                self._return = self._target(self._args, self._kwargs)
            except BaseException as e:
                self.ex = e

    def waiting(self, *args):
        Thread.join(self, *args)
        if self.ex:
            raise self.ex
        return self._return
