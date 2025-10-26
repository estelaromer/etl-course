import numpy as np
import time

# Create a large array of 10 million elements
data = np.arange(10_000_000)

start_time = time.time()

# BAD: Using a Python loop to square each element
squared = []
for value in data:
    squared.append(value ** 2)
squared = np.array(squared)

end_time = time.time()
print(f"Loop version took {end_time - start_time:.4f} seconds")
