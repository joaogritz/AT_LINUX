import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


def tarefa():
    time.sleep(1)
    return 1


def tarefa_cpu():
    total = 0

    for i in range(5_000_000):
        total += i

    return total


async def tarefa_async():
    await asyncio.sleep(1)


async def executar_async():
    await asyncio.gather(
        tarefa_async(),
        tarefa_async(),
        tarefa_async(),
        tarefa_async()
    )


def main():
    # 1. Versão original
    inicio = time.time()

    for _ in range(4):
        tarefa()

    tempo_original = time.time() - inicio

    # 2. Asyncio
    inicio = time.time()

    asyncio.run(executar_async())

    tempo_async = time.time() - inicio

    # 3. Multithreading
    inicio = time.time()

    with ThreadPoolExecutor() as executor:
        tarefas = [executor.submit(tarefa) for _ in range(4)]

        for resultado in tarefas:
            resultado.result()

    tempo_threads = time.time() - inicio

    # 4. Processamento paralelo
    inicio = time.time()

    with ProcessPoolExecutor() as executor:
        tarefas = [executor.submit(tarefa_cpu) for _ in range(4)]

        for resultado in tarefas:
            resultado.result()

    tempo_processos = time.time() - inicio

    print("Original:", tempo_original)
    print("Asyncio:", tempo_async)
    print("Multithreading:", tempo_threads)
    print("Processamento paralelo:", tempo_processos)


if __name__ == "__main__":
    main()