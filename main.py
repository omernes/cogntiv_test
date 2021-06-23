import argparse
import asyncio
import math
import pickle
import random
import sys
import time
from multiprocessing import Process

from numpy.random import default_rng
import numpy as np


class DataGenerator:
    def __init__(self, vector_length: int = 50, noisy_mode: bool = False):
        self.noisy_mode = noisy_mode
        self.vector_length = vector_length

    async def run(self, socket_host, socket_port):
        server = await asyncio.start_server(self._generate_data, socket_host, socket_port)
        async with server:
            await server.serve_forever()

    async def _generate_data(self, reader, writer):
        # for i in range(10):
        timestamp_ms = (int(round(time.time() * 1000)) // 1000) * 1000
        while True:
            # for i in range(5000):
            if self.noisy_mode and random.randrange(0, 2500) == 1234:
                await asyncio.sleep(0.001)
                timestamp_ms += 1
                continue

            vector = self._generate_random_vector(length=self.vector_length)
            timestamp_ms += 1
            data = pickle.dumps((timestamp_ms, vector))
            writer.write(data)

            await asyncio.sleep(0.001)

    def _generate_random_vector(self, length: int = 50):
        vector = default_rng().standard_normal(length)
        return vector


class DataAggregator:
    def __init__(self, vector_length: int = 50):
        self.vector_length = vector_length
        self.num_vectors_received = 0
        self.start_window_timestamp = None
        self.rates = np.array([])
        self.matrix = None

    async def run(self, socket_host, socket_port):
        reader, writer = await asyncio.open_connection(socket_host, socket_port)
        # for i in range(20):
        while True:
            data = await reader.readexactly(self.vector_length * 8 + 160)
            timestamp, vector = pickle.loads(data)
            # vector = np.frombuffer(data, count=self.vector_length, dtype="int32")
            self._handle_vector(vector, timestamp)

    def _handle_vector(self, vector, timestamp_ms):
        if not self.start_window_timestamp or self.num_vectors_received % 1000 == 0:
            self.start_window_timestamp = timestamp_ms

        self.num_vectors_received += 1

        if timestamp_ms % 1000 == 0:
            rate = self.num_vectors_received
            self.rates = np.append(self.rates[-100:], [rate])
            rate_mean = self.rates.mean()
            rate_std = self.rates.std()
            print(f"Rate: {rate} Hz | Mean: {rate_mean} | STD: {rate_std}")
            if rate < 1000:
                print("WARNING! Rate fell below expected frequency")
            self._append_line(f"Rate: {rate} | Mean: {rate_mean} | STD: {rate_std}")
            self.num_vectors_received = 0

        if self.matrix is None:
            self.matrix = np.array([vector])
        else:
            self.matrix = np.append(self.matrix, [vector], axis=0)

        if self.num_vectors_received % 100 == 0:
            mat_mean = self.matrix.mean(axis=0)
            mat_std = self.matrix.std(axis=0)

            # print(f"Matrix Mean: {mat_mean} | Matrix SD: {mat_std}")
            self._append_line(f"Matrix Mean: {mat_mean} | Matrix SD: {mat_std}")

            self.matrix = None

    def _append_line(self, line):
        with open("results.txt", "a") as f:
            f.write(line + "\n")


SOCKET_HOST = '127.0.0.1'
SOCKER_PORT = 8888


def run_aggregator(socket_host, socket_port):
    asyncio.run(DataAggregator().run(socket_host, socket_port))


def run_generator(socket_host, socket_port, noisy_mode):
    asyncio.run(DataGenerator(noisy_mode=noisy_mode).run(socket_host, socket_port))


if __name__ == "__main__":
    # noisy_mode = bool(sys.argv[1]) if len(sys.argv) > 1 else False
    parser = argparse.ArgumentParser()
    parser.add_argument('--noisy', help='Run in Noisy Mode', default=False, action="store_true")
    args = parser.parse_args()

    p_gen = Process(target=run_generator, args=(SOCKET_HOST, SOCKER_PORT, args.noisy))
    p_gen.start()

    p_agg = Process(target=run_aggregator, args=(SOCKET_HOST, SOCKER_PORT))
    p_agg.start()

    p_gen.join()
    p_agg.join()
