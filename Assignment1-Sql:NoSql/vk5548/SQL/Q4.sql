
Select m.id as m1, null as m2, null as p
from
movie as m where m.id in
(Select m.id
from movie as m join actor as a join person as p
on p.id = a.pid and m.id = a.mid
where p.name = "Kate Winslet") 
and m.id in
(Select m.id
from movie as m join actor as a join person as p
on p.id = a.pid and m.id = a.mid
where p.name = "Leonardo Dicaprio") 

union

Select  KW.m1 as m1, LDC.m1 as m2, LDC.p as p
from (Select a1.pid as p, m1.id as m1 
from actor as a1 join movie as m1
on a1.mid = m1.id
where m1.id in(Select m.id from actor as a join person as p join movie as m on p.id = a.pid and m.id = a.mid 
where p.name = "Kate Winslet") ) as KW
join
(Select a1.pid as p, m1.id as m1 
from actor as a1 join movie as m1
on a1.mid = m1.id
where m1.id in(Select m.id from actor as a join person as p join movie as m on p.id = a.pid and m.id = a.mid 
where p.name = "Leonardo DiCaprio") ) as LDC
on LDC.p = KW.p where KW.m1 not in
 (Select m.id as m1
from
movie as m where m.id in
(Select m.id
from movie as m join actor as a join person as p
on p.id = a.pid and m.id = a.mid
where p.name = "Kate Winslet") 
and m.id in
(Select m.id
from movie as m join actor as a join person as p
on p.id = a.pid and m.id = a.mid
where p.name = "Leonardo Dicaprio") )
and LDc.m1 not in
 (Select m.id as m1
from
movie as m where m.id in
(Select m.id
from movie as m join actor as a join person as p
on p.id = a.pid and m.id = a.mid
where p.name = "Kate Winslet") 
and m.id in
(Select m.id
from movie as m join actor as a join person as p
on p.id = a.pid and m.id = a.mid
where p.name = "Leonardo Dicaprio") )