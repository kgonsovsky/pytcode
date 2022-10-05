
 insert into vost_pol (﻿id_uch)
 select distinct * from (
 select DISTINCT ﻿id_uch_vost_pol as ﻿id_uch
from source
UNION ALL
select ﻿id_uch_vost_pol as ﻿id_uch
 from source) a

CREATE TABLE article2 (
    article_id bigserial primary key,
    article_desc text NOT NULL
);

insert into article2 (article_desc)
values ('aa')