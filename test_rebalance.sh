#!/bin/bash
# Test script for internal node rebalancing

./tinydb << 'EOF'
insert 1 a a@a.com
insert 2 b b@b.com
insert 3 c c@c.com
insert 4 d d@d.com
insert 5 e e@e.com
insert 6 f f@f.com
insert 7 g g@g.com
insert 8 h h@h.com
insert 9 i i@i.com
insert 10 j j@j.com
insert 11 k k@k.com
insert 12 l l@l.com
insert 13 m m@m.com
insert 14 n n@n.com
insert 15 o o@o.com
insert 16 p p@p.com
insert 17 q q@q.com
insert 18 r r@r.com
insert 19 s s@s.com
insert 20 t t@t.com
.btree
delete 1
delete 2
delete 3
delete 4
delete 5
.btree
select
.exit
EOF
