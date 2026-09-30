import argparse
import asyncio
import multiprocessing as mp
import os
import random
import threading
import time
from datetime import datetime


# ------------------------------------------------------------
# Monte Carlo
# ------------------------------------------------------------

def monte_carlo(samples):
    hits = 0

    for _ in range(samples):
        x = random.uniform(-1, 1)
        y = random.uniform(-1, 1)

        if x * x + y * y <= 1:
            hits += 1

    return hits


# ------------------------------------------------------------
# Versão serial
# ------------------------------------------------------------

def executar_serial(total_samples):
    inicio = time.perf_counter()

    hits = monte_carlo(total_samples)

    pi = 4 * hits / total_samples

    fim = time.perf_counter()

    return pi, fim - inicio


# ------------------------------------------------------------
# Asyncio - tarefas de monitoramento
# ------------------------------------------------------------

async def tarefa_monitoramento(numero):
    await asyncio.sleep(0.5)


async def executar_monitoramento():
    tarefas = []

    for i in range(20):
        tarefa = asyncio.create_task(tarefa_monitoramento(i + 1))
        tarefas.append(tarefa)

    await asyncio.gather(*tarefas)


def iniciar_asyncio():
    asyncio.run(executar_monitoramento())


# ------------------------------------------------------------
# Log
# ------------------------------------------------------------

def escrever_log(stop_event, progresso, total_tasks):
    while not stop_event.is_set():

        concluidas = progresso.value
        porcentagem = (concluidas / total_tasks) * 100

        horario = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pid = os.getpid()

        mensagem = (
            f"{horario} | "
            f"Progresso: {concluidas}/{total_tasks} "
            f"({porcentagem:.2f}%) | "
            f"PID escritor: {pid}\n"
        )

        with open("execucao.log", "a", encoding="utf-8") as arquivo:
            arquivo.write(mensagem)

        time.sleep(0.5)


# ------------------------------------------------------------
# Versão paralela
# ------------------------------------------------------------

def executar_paralelo(n_tasks, chunk_size):
    inicio = time.perf_counter()

    workers = mp.cpu_count()

    progresso = mp.Value("i", 0)
    stop_event = threading.Event()

    thread_log = threading.Thread(
        target=escrever_log,
        args=(stop_event, progresso, n_tasks)
    )

    thread_log.start()

    thread_asyncio = threading.Thread(
        target=iniciar_asyncio
    )

    thread_asyncio.start()

    resultados = []

    tarefas = [chunk_size] * n_tasks

    with mp.Pool(processes=workers) as pool:

        for resultado in pool.imap_unordered(monte_carlo, tarefas):
            resultados.append(resultado)

            with progresso.get_lock():
                progresso.value += 1

    stop_event.set()

    thread_log.join()
    thread_asyncio.join()

    total_hits = sum(resultados)
    total_samples = n_tasks * chunk_size

    pi = 4 * total_hits / total_samples

    fim = time.perf_counter()

    return pi, fim - inicio


# ------------------------------------------------------------
# Programa principal
# ------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Monte Carlo serial e paralelo"
    )

    parser.add_argument(
        "--n_tasks",
        type=int,
        default=8,
        help="Número de blocos/tarefas"
    )

    parser.add_argument(
        "--chunk_size",
        type=int,
        default=1_000_000,
        help="Quantidade de amostras por bloco"
    )

    args = parser.parse_args()

    total_samples = args.n_tasks * args.chunk_size
    workers = mp.cpu_count()

    print("=" * 60)
    print("MONTE CARLO - APROXIMAÇÃO DE PI")
    print("=" * 60)

    print(f"Total de amostras: {total_samples:,}")
    print(f"Número de tarefas: {args.n_tasks}")
    print(f"Chunk size: {args.chunk_size:,}")
    print(f"CPUs disponíveis: {workers}")

    # --------------------------------------------------------
    # Serial
    # --------------------------------------------------------

    print("\nExecutando versão serial...")

    pi_serial, tempo_serial = executar_serial(total_samples)

    # --------------------------------------------------------
    # Paralelo
    # --------------------------------------------------------

    print("\nExecutando versão paralela...")

    pi_paralelo, tempo_paralelo = executar_paralelo(
        args.n_tasks,
        args.chunk_size
    )

    # --------------------------------------------------------
    # Resultados
    # --------------------------------------------------------

    print("\n" + "=" * 60)
    print("RESULTADOS")
    print("=" * 60)

    print("\nSerial:")
    print(f"Pi estimado: {pi_serial}")
    print(f"Tempo: {tempo_serial:.4f} segundos")

    print("\nParalelo:")
    print(f"Pi estimado: {pi_paralelo}")
    print(f"Tempo: {tempo_paralelo:.4f} segundos")

    speedup = tempo_serial / tempo_paralelo

    print(f"\nSpeedup: {speedup:.2f}x")


if __name__ == "__main__":
    main()