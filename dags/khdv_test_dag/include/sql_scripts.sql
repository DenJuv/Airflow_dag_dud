create schema if not exists ckkd;
create table if not exists ckkd.khdv_npd_table_row_count (schemaname text, table_name text, row_count bigint, date_load timestamp default current_date);
do $$
declare
	schemaname_t text;
    table_name_t text;
    row_count bigint;
begin
create temp table if not exists temp_khdv_table_row_count (schemaname text, table_name text, row_count bigint);
    for schemaname_t, table_name_t in
        select schemaname, tablename
        from pg_tables
        where schemaname in ('public', 'pg_catalog','information_schema')
    loop
        execute 'select count(*) from ' || schemaname_t || '.' || table_name_t into row_count;
        insert into temp_khdv_table_row_count (schemaname, table_name, row_count) values (table_name_t, table_name_t, row_count);
    end loop;
end $$;
insert into ckkd.khdv_npd_table_row_count select * from temp_khdv_table_row_count;