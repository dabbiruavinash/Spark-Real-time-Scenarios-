1. "the sky is blue"   >> "blue is sky the"

s = "the key is blue"
new_s = s.split(" ")
print(new_s)

ss = [ ]

for w in new_s:
      if w! = " ":
         ss.append(w)

print(" ".join(ss[::-1]))

2. 2[a][b] = aabdbdbd

s = "2[a]3[bd]"

res = " "
for i in s:
      if i.isdigit():
         res = res + i
         temp_i = i
      else:
         res = res + int(temp_i) * i
print(res)

new_res = res.replace('[', ' ')
final_res = new_res.replace(']', ' ')
print(final_res) #### aabbbddd

ss = s.replace('[', ',')
print(ss.replace(']', ','))
s = ss.replace(']', ',')
res = ' '

for i in s:
      if i.isdigit():
         res = res + i
         temp_i = i
      else:
         res = res + int(temp_i) * i
print('new---', res)

def decode(s : str) -> str:
       stack = [ ]
       num = 0
       curr_str = " "

       for ch in s:
             if ch.isdigit():
                num = num * 10 + int(cb) # handles multi-digit numbers
             elif ch == "["
                stack.append((curr_str, num))
                curr_str, num = " ", 0
             elif ch == "]":
                 prev_str , repeat = stack.pop()
                 curr_str = prev_str + repeat + curr_str
             else:
                 curr_str += ch
        return curr_str

print(decode("2[a]3[bd]")) 