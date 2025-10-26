import numpy as np
import time

# Create a large array of 10 million elements
data = np.arange(10_000_000)

start_time = time.time()

# GOOD: Use NumPy vectorized operation
squared = data ** 2

end_time = time.time()
print(f"Vectorized version took {end_time - start_time:.4f} seconds")
