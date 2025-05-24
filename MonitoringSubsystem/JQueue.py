from multiprocessing.queues import Queue as BaseQueue
from multiprocessing import Value, get_context
# import ctypes
# from uuid import uuid4


class SharedCounter:
    def __init__(self, n=0):
        self.count = Value('i', n)

    def increment(self, n=1):
        with self.count.get_lock():
            self.count.value += n

    @property
    def value(self):
        return self.count.value

#
# class SharedName:
#     def __init__(self, name=None):
#         _name = name if name else f'{uuid4()}'
#         self.name = Value(ctypes.c_char_p, bytes(str(_name), 'utf-8'))
#
#     def set_name(self, name):
#         with self.name.get_lock():
#             self.name = bytes(str(name), 'utf-8')
#
#     @property
#     def value(self):
#         return self.name.value.decode('utf-8')


class JQueue(BaseQueue):
    def __init__(self):
        super().__init__(ctx=get_context())
        self.size = SharedCounter(0)

    def __getstate__(self):
        return {
            'parent_state': super().__getstate__(),
            'size': self.size,
        }

    def __setstate__(self, state):
        super().__setstate__(state['parent_state'])
        self.size = state['size']

    def put(self, *args, **kwargs):
        super().put(*args, **kwargs)
        self.size.increment(1)

    def get(self, *args, **kwargs):
        item = super().get(*args, **kwargs)
        self.size.increment(-1)
        return item

    def qsize(self):
        return self.size.value

    def is_empty(self):
        return not self.qsize()

    def clear(self):
        while not self.is_empty():
            self.get()

    # def qname(self):
    #     return self.name.value


# if __name__ == "__main__":
#     tq = JQueue()
#     print(tq.qname())
