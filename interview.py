"""#Input: 'abc'
 
#Expected Output: ['a', 'b', 'c', 'ab', 'bc', 'ac', 'abc']
str='abc'

l=[]
for i in  range(len(str)):
    l.append(str[i])
    for  j in range(i+1,len(str)):
        str1=str[i]+str[j]
        l.append(str1)
        #print(str1)
l.append(str)
print(l)

"""

#Input: ['flower', 'flow', 'flight']
 
#Expected Output: 'fl'

l=['flower', 'flow', 'flight']
str=l[0]
output=''
for i in str:
    c=0
    for string in l:
        if i in string:
            c=c+1
            if c==len(l):
                output=output+i
print(output)