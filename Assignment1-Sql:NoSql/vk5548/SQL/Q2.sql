Select movie.id
from movie join producer join person
on movie.id = producer.mid and person.id = producer.pid where person.name = 'Kathleen Kennedy' and movie.id in 
(Select m.id
from movie as m join producer as p on p.mid = m.id
group by m.id
having count(p.pid) = 1)