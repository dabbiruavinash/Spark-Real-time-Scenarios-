# In a given list of integers, returns the top 3 most frequent numbers along with their counts

input => [1,2,2,3,3,3,4,4,4,4,5,5,6]
output => [(4,4), (3,3), (2,2)]

def top_3_frequent(nums):
       freq = Counter(nums)
       return freq.most_common(3)

result = top_3_frequent(input_list)
print(result)

# print prime numbers between 20 and 50

def is_prime(n):
    if n < 2:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

primes = [num for num in range(20, 51) if is_prime(num)]
print("Prime numbers between 20 and 50:", primes)

# Have a string a a a a b b b c need to find maximum iterating character

from collections import Counter

def max_iterating_char(s):
    s = s.replace(' ', '')
    freq = Counter(s)
    max_char = max(freq, key=freq.get)
    return max_char, freq[max_char]

test_string = "a a a a b b b c"
result_char, result_count = max_iterating_char(test_string)
print(f"Maximum iterating character: '{result_char}' with count {result_count}")