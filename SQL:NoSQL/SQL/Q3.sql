
Select distinct(p1.name) from
actor as a join movie as m1 join person as p1 join knownfor as k
 on m1.id = a.mid and p1.id = a.pid and k.pid = p1.id and k.pid = a.pid and k.mid = m1.id
where m1.id in
(Select m.id
from movie as m join director as d join person  as p join genre as g join moviegenre as mg
on p.id = d.pid and d.mid = m.id and mg.mid = m.id and g.id = mg.gid
where p.name = 'Woody Allen' and g.name = 'Comedy')
and p1.name <> 'Woody Allen'
group by p1.name
having count(m1.id)> 1