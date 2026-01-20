"""
class ActionToTest():
    def __init__(self):
        pass
    def sum(x,y):
        return x+y
    def sub(x,y):
        return x-y
cl=ActionToTest()
print(cl.sum(5,7))
print(cl.sub(5,2))
"""

#[1, 2, 1, 1, 3, 4, 3, 1, 1, 2, 1, 1, 2]
rep=1
count=0
l=[1, 2, 1, 1, 3, 4, 3, 1, 1, 2, 1, 1, 2]
for i in range(0,len(l)):
    #rint(l[i],rep)
    if l[i]==rep:
        count=count+1
        if count==5:
            print(i)
            break

    
