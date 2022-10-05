/*
 Шаг 1. Иимпорт RAZB_UCH.CSV в таблицу "SOURCE"
*/
CREATE OR REPLACE FUNCTION load_csv_file(target_table text,csv_path text,col_count integer)
  RETURNS void AS
$BODY$
declare
iter integer;
col text;
col_first text;
begin
    set schema 'public';
    create table temp_table ();
    for iter in 1..col_count
    loop
        execute format('alter table temp_table add column col_%s text;', iter);
    end loop;
    execute format('copy temp_table from %L with delimiter '','' quote ''"'' csv ', csv_path);
    iter := 1;
    col_first := (select col_1 from temp_table limit 1);
    for col in execute format('select unnest(string_to_array(trim(temp_table::text, ''()''), '','')) from temp_table where col_1 = %L', col_first)
    loop
        execute format('alter table temp_table rename column col_%s to %s', iter, col);
        iter := iter + 1;
    end loop;
    execute format('delete from temp_table where %s = %L', col_first, col_first);
    if length(target_table) > 0 then
        execute format('alter table temp_table rename to %I', target_table);
    end if;
end;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION load_csv_file(text, text, integer)
  OWNER TO postgres;

drop table if exists source cascade ;
SELECT load_csv_file(csv_path => 'C:/Users/Administrator/source/repos/PytCode/razb_uch.csv', target_table => 'source', col_count => 22);