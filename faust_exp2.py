#!/usr/bin/env python
import faust
import numpy as np
from collections import deque

app = faust.App(
    'numbers',
    broker='kafka://localhost:9092',
)

numbers_topic = app.topic('numbers', value_type=str)


@app.agent(numbers_topic)
async def rolling_average(numbers):
    que = deque([0 for _  in range(5)])
    async for number in numbers:
        print(f'message arrived {number}')
        que.popleft()
        que.append(float(number))
        rolling_average = np.mean(que)
        print(f'the new rolling average {rolling_average}')


if __name__ == '__main__':
    app.main()