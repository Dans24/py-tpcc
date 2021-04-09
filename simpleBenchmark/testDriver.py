import random
import time
from abstractDriver import AbstractDriver

class testDriver(AbstractDriver):

    def connect(self):
        self.dict = {}

    def load(self):
        for i in range(0, 10):
            self.dict[i] = 0

    def update(self):
        time.sleep(random.random())
    
    def query(self):
        time.sleep(random.random())

    def clear(self):
        self.dict = None