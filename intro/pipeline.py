import sys
import pandas as pd

# print system arguments
print(sys.argv)

# argument 1 is the name os the file
# argument 2 contains the actual first argument we care about
day = sys.argv[1]

# Cool stuff using pandas

# print a sentence with the argument
print(f'job finished successfully for day = {day}')