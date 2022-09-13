Select m.otitle as title
from movie as m join director as d join person as p
on p.id = d.pid and d.mid = m.id
where p.name = 'Steven Spielberg' and year <=

(Select min(m1.year)
from movie as m1 join actor as a join person as p1
on p1.id = a.pid and a.mid = m1.id
where p1.name = 'Mahershala Ali')

and year <=
(Select min(m2.year)
from movie as m2 join writer as w join person as p2
on p2.id = w.pid and w.mid = m2.id
where p2.name = 'Fran Walsh')

