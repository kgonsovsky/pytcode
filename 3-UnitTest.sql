/*
 Шаг 3. Заполним данным в таблицу result и создадим resultView
*/

truncate result;
insert into result (vost_uch_begin_slave_id,vost_uch_end_slave_id,
                    melk_set_begin_slave_id, melk_set_end_slave_id,
                    okato_begin_vost_uch_slave_id, okato_end_vost_uch_slave_id,
                    okato_begin_melk_set_slave_id, okato_end_melk_set_slave_id,
                     vost_pol_slave_id
                  )
select
      vost_uch_begin.relation_id as vost_uch_begin_slave_id, vost_uch_end.relation_id as vost_uch_end_slave_id
      ,melk_set_begin.relation_id as melk_set_begin_slave_id, melk_set_end.relation_id as melk_set_end_slave_id
      ,okato_vost_uch_begin.relation_id as okato_vost_uch_slave_id, okato_vost_uch_end.relation_id as okato_vost_uch_end_slave_id
      ,okato_melk_set_begin.relation_id as okato_melk_set_slave_id, okato_melk_set_end.relation_id as okato_melk_set_end_slave_id
      ,vost_pol.relation_id as vost_pol_slave_id
from source
left outer join vost_uch vost_uch_begin on vost_uch_begin.name = source.name_begin_vost_uch
left outer join vost_uch vost_uch_end on vost_uch_end.name = source.name_end_vost_uch
left join melk_set melk_set_begin on melk_set_begin.num = source.num_cnsi_melk_set and melk_set_begin.esr  = source.esr_begin_melk_set  and melk_set_begin.dor  = source.dor_begin_melk_set
left join melk_set melk_set_end on melk_set_end.num = source.num_cnsi_melk_set and melk_set_end.esr  = source.esr_end_melk_set  and melk_set_end.dor  = source.dor_end_melk_set
left outer join okato okato_vost_uch_begin on okato_vost_uch_begin.name = source.okato_begin_vost_uch_name
left outer join okato okato_vost_uch_end on okato_vost_uch_end.name = source.okato_end_vost_uch_name
left outer join okato okato_melk_set_begin on okato_melk_set_begin.name = source.okato_begin_melk_set_name
left outer join okato okato_melk_set_end on okato_melk_set_end.name = source.okato_end_melk_set_name
left outer join vost_pol on vost_pol.﻿id = source.﻿id_uch_vost_pol
;
drop view if exists resultView;
create view resultView as
    select vost_uch_begin.name as name_begin_vost_uch, vost_uch_end.name as name_end_vost_uch,
              vost_uch_begin.esr as esr_begin_vost_uch,       vost_uch_begin.dor as dor_begin_vost_uch,     vost_uch_begin.x as x_beg_vost_uch,    vost_uch_begin.y as y_beg_vost_uch
           ,vost_uch_end.esr as esr_end_vost_uch,       vost_uch_end.dor as dor_end_vost_uch,     vost_uch_end.x as x_end_vost_uch,    vost_uch_end.y as y_end_vost_uch

            ,melk_set_begin.name as name_begin_melk_set, melk_set_end.name as name_end_melk_set
            ,melk_set_begin.esr as esr_begin_melk_set,       melk_set_end.esr as esr_end_melk_set
            ,melk_set_begin.dor as dor_begin_melk_set,       melk_set_end.dor as dor_end_melk_set

            ,okato_vost_uch_begin.name as okato_begin_vost_uch_name, okato_vost_uch_end.name as okato_end_vost_uch_name
            ,okato_melk_set_begin.name as okato_begin_melk_set_name, okato_melk_set_end.name as okato_end_melk_set_name,
           vost_pol.﻿id as ﻿id_uch_vost_pol
    from result
left outer join vost_uch vost_uch_begin on vost_uch_begin.relation_id = result.vost_uch_begin_slave_id
left outer join vost_uch vost_uch_end on vost_uch_end.relation_id = result.vost_uch_end_slave_id
left outer join melk_set melk_set_begin on melk_set_begin.relation_id = result.melk_set_begin_slave_id
left outer join melk_set melk_set_end on melk_set_end.relation_id = result.melk_set_end_slave_id
left outer join okato okato_vost_uch_begin on okato_vost_uch_begin.relation_id = result.okato_begin_vost_uch_slave_id
left outer join okato okato_vost_uch_end on okato_vost_uch_end.relation_id = result.okato_end_vost_uch_slave_id
left outer join okato okato_melk_set_begin on okato_melk_set_begin.relation_id = result.okato_begin_melk_set_slave_id
left outer join okato okato_melk_set_end on okato_melk_set_end.relation_id = result.okato_end_melk_set_slave_id
left outer join vost_pol on vost_pol.relation_id = result.vost_pol_slave_id
;
select * from resultView