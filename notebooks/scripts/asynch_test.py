import asyncio
import random

# ANSI colors
import time

c = (
    "\033[0m",  # End of color
    "\033[36m",  # Cyan
    "\033[91m",  # Red
    "\033[35m",  # Magenta
)


async def makerandom():
    a = random.random()
    await asyncio.sleep(1)
    return a


async def main():
    res = await asyncio.gather(*(makerandom() for i in range(3)))
    return res


if __name__ == "__main__":
    startTime = time.time()
    r1, r2, r3 = asyncio.run(main())

    print(f"r1: {r1}, r2: {r2}, r3: {r3}")
    print('The script took {0} second !'.format(time.time() - startTime))
