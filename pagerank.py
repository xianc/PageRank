import sqlite3
import sys
from operator import itemgetter

conn = sqlite3.connect('link.db')
c = conn.cursor() 
d = conn.cursor()
e = conn.cursor()

#c.execute("create table pages as select distinct source from (select source from links order by source) where source in (select destination from (select destination from links order by destination)) order by source")
c.execute("create table pages as select distinct source from links union select distinct destination from links")
c.execute("create table out as select distinct destination from links where destination in (select * from pages) order by destination")
c.execute("create table filtered as select source, destination from links where source in (select * from pages) and destination in (select * from pages)")

P=[]
N=0
c.execute("select * from pages")
for r in c:
	P.append(r[0])
	N=N+1

#print P , N

I = [1.0/N]*N
#print I;

R = []
done =0

while done==0:
	R = [.15/N]*N

	pindex=0;
	d.execute("select * from pages")
	for pagesp in d:
		Q=[]
		lenq=0;
		c.execute("select distinct destination from filtered where source== ? and destination IN(select * from pages) order by destination", pagesp)
		for row in c:
			#if s==pagesp:
			#if d in P:
			Q.append(row[0])
			lenq+=1
		if lenq>0:
			#print Q
			for pagesq in Q:
				indx=P.index(pagesq);
				R[indx] = R[indx]+(1-.15)*I[pindex]/lenq
		else:
			e.execute("select * from out")
			for pagesq in e:
				indx=P.index(pagesq[0]);
				R[indx] = R[indx]+(1-.15)*I[pindex]/N

		if ((sum(R)-sum(I))/N)<=.001:
			#print (sum(R)-sum(I))/N
			done=1
		I = R[:]

		pindex+=1

c.execute("drop table out")
c.execute("drop table pages")
c.execute("drop table filtered")
conn.close()


print R

I = zip(P,R)
write = open('topInlinks2.txt', 'w')
br = 0
for item1, item2 in sorted(I, key=itemgetter(1), reverse=True):
	br = br + 1
	print >> write, item1, "\t" , item2
	if br >= N:
		break
write.close
