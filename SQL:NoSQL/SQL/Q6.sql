Select  tableKB.movieKb as m1, tableJC.movieJC as m2, tableKB.actorKB as p
from
(Select p2.id as actorJC, m2.id as movieJC
from movie as m2 join actor as a2 join person as p2
on p2.id = a2.pid and a2.mid = m2.id
where m2.id in
(Select m1.id
from director as d1 join person as p1 join movie as m1
on m1.id = d1.mid and d1.pid = p1.id
where name = 'James Cameron')) as tableJC
join
(Select p1.id as actorKB , m1.id as movieKb
from movie as m1 join actor as a2 join person as p1
on p1.id = a2.pid and a2.mid = m1.id
where m1.id in
(Select m1.id
from director as d1 join person as p1 join movie as m1
on m1.id = d1.mid and d1.pid = p1.id
where name = 'Kathryn Bigelow')) as tableKB
on tableKB.actorKB = tableJC.actorJC