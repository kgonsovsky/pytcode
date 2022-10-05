/*
 Шаг 2. Скрипт проанализирует колонки и данные, создаст реалиционную модель данных, заполнит таблицы и создаст связи
*/

/* из имени стобца исходной таблицы возвращает имя реалиционной сущности */
CREATE OR REPLACE FUNCTION entityName(columnName text)
  RETURNS text AS $$
DECLARE
    a text[];
    b text;
    n int;
BEGIN
    a := (string_to_array(columnName,'_'));
    n := array_length(a,1);
    if entityLevel(columnName)=2 then
        return a[1];
    end if;
    RETURN concat(a[n-1],'_',a[n]);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entityLevel(columnName text)
 RETURNS int AS $$
DECLARE
    a text[];
    n int;
BEGIN
    a := (string_to_array(columnName,'_'));
    n := array_length(a,1);
    return 7-n;
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entityParent(columnName text)
 RETURNS text AS $$
DECLARE
    a text[];
    n int;
BEGIN
   if entityLevel(columnName)<>2 then
        return '';
    end if;
    a := (string_to_array(columnName,'_'));
    n := array_length(a,1)-1;
    RETURN concat(a[n-1],'_',a[n]);
end;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entitySuffixes(entityName text)
 RETURNS TABLE (suffix text)
 LANGUAGE plpgsql AS
$func$
begin
    RETURN QUERY
    with cte as (
    Select distinct fieldSuffix(column_name, entityName) as suffix FROM information_schema.columns
    where table_name = 'source' and entityName(column_name) = entityName)
    select distinct cte.suffix from cte;
end;
$func$;

/* из имени стобца исходной таблицы и указанной сущности возвращает имя поля реалиционно сущности */
CREATE OR REPLACE FUNCTION fieldName(columnName text, enityName text)
  RETURNS text AS $$
DECLARE
    a text[];
    b text;
    n int;
BEGIN
    if (entityName(columnName) <> enityName) then
        return null;
    end if;
    a := (string_to_array(columnName,'_'));
    n := array_length(a,1);
    if entityLevel(columnName)=2 then
        return a[n];
    end if;
    return a[1];
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fieldSuffixIs(columnName text, enityName text, suffix text)
  RETURNS int AS $$
DECLARE
    a text;
BEGIN
    a := fieldSuffix(columnName, enityName);
    if (a = '') then
        return 1;
    end if;
    if (a <> suffix) then
        return 0;
    end if;
    return 1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fieldSuffix(columnName text, enityName text)
  RETURNS text AS $$
DECLARE
    a text;
BEGIN
    if (entityName(columnName) <> enityName) then
        return '';
    end if;
    a := split_part(columnName,'_',2);
    if (a='beg') then
        a='begin';
    end if;
    if (a='begin' or a = 'end') then
        return a;
    end if;
    return '';
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fieldId(entity text,  suffix text, src text)
  RETURNS text AS $$
DECLARE
    a text;
BEGIN
   if (src='' and suffix='') then
        return concat(entity,'_','id');
    end if;
    if (suffix='') then
        return concat(entity,'_',src,'_','id');
    end if;
    if (src='') then
        return concat(entity,'_',suffix,'_','id');
    end if;
   return concat(entity,'_',suffix,'_',src,'_','id');
END;
$$ LANGUAGE plpgsql;



/* соберем список полей назначения в реалиционной таблице */
CREATE OR REPLACE FUNCTION fieldsDest(entityName text,more text)
RETURNS text
 LANGUAGE plpgsql AS
$func$
begin
    return (select STRING_AGG(concat(column_name,more),',')
        from (Select distinct fieldName(column_name, entityName) as column_name   FROM information_schema.columns
    where table_name = 'source' and entityName(column_name) = entityName)
        a);
end;
$func$;

CREATE OR REPLACE FUNCTION fieldSources(entityName text)
 RETURNS TABLE (src text)
  LANGUAGE plpgsql AS
$func$
begin
    RETURN QUERY
    with cte as (
    Select distinct entityParent(column_name) as src FROM information_schema.columns
    where table_name = 'source' and entityName(column_name) = entityName)
    select distinct cte.src from cte;
end;
$func$;

/* соберем список полей исходных полей и соответсвующих полей назначения в реалицинных таблицах */
CREATE OR REPLACE FUNCTION fieldsSource(entityName text, src text, suffix text)
RETURNS text
 LANGUAGE plpgsql AS
$func$
begin
    return (select STRING_AGG(concat(column_name,' as ', fieldName(column_name, entityName)),',')
    FROM information_schema.columns
    where table_name = 'source' and (src='' or entityParent(column_name) = src) and entityName(column_name) = entityName and fieldSuffixIs(column_name,entityName,suffix)=1);
end;
$func$;

/* соберем список сущностей */
drop table if exists entities;
with cte as (
    SELECT entityName(column_name) AS entity
    FROM information_schema.columns
    where table_name = 'source'
)
select distinct entity  into temp  entities from cte  where entity is not null

/* создаддим и заполним таблицы */
DO $$
DECLARE
entityName varchar;
source varchar;
sql text;
BEGIN
FOR entityName IN SELECT entity FROM entities
	LOOP
        raise notice '%', entityName;
        sql := format('
            drop table if exists %s; CREATE TABLE %s (RELATION_ID bigserial primary key,%s);'
            ,entityName,entityName,fieldsDest(entityName,'  text'));
        raise notice '%', sql;
        raise notice '';
        EXECUTE sql;
        sql := 'WITH cte as (';

        sql = concat(sql,
            (Select STRING_AGG(
                format('select distinct %s from source union all select distinct %s from source ',
            fieldsSource(entityName,src,'begin'), fieldsSource(entityName,src,'end'))
                        ,' union all ') FROM fieldSources(entityName)));
        sql = concat(sql,format(') insert into %s (%s) select distinct %s from cte ',
               entityName, fieldsDest(entityName,''), fieldsDest(entityName,'')));
        raise notice '%', sql;
        raise notice '';
        EXECUTE sql;
	END LOOP;
END$$;

/* создадим резуьлтирующую таблицу */
drop table if exists result;

--
-- DO $$
-- DECLARE
-- entityName varchar;
-- suffixName varchar;
-- sql text;
-- BEGIN
--     sql := 'CREATE TABLE RESULT (MASTER_ID bigserial primary key';
--     FOR entityName IN SELECT entity FROM entities
-- 	LOOP
--         FOR suffixName IN SELECT suffix FROM entitySuffixes(entityName)
-- 	    LOOP
--             raise notice '%, %', entityName, suffixName;
--             raise notice '';
--             sql = concat(sql,',',
--                     (select STRING_AGG(concat(fieldId(entityName,suffixName, src), ' bigint '),', ') FROM fieldSources(entityName))
--                 );
--         END LOOP;
-- 	END LOOP;
--     sql=concat(sql,');');
--     raise notice '%', sql;
--     raise notice '';
--     EXECUTE sql;
-- END$$;