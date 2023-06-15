Select distinct(name)
from actor join person on person.id = actor.pid where mid in (Select m.id
from person as p join genre as g join moviegenre as mg join movie as m join director as d 
on mg.mid = m.id and mg.gid = g.id and d.pid = p.id and d.mid = m.id
where d.pid in 
(Select d1.pid from director as d1 join movie as m1 on d1.mid = m1.id where m1.otitle = "Big" and m1.year = 1988)
and g.name = 'Comedy' and m.year between 1985 and 2005
)
