import time

def calculate_runtime(start_time):
    end_time = time.time()
    runtime = end_time - start_time
    print(f"Total Run Time: {runtime:.2f} seconds")