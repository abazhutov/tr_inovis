create or replace function get_count_products()
returns integer
language plpgsql
as $$
declare
    rec record;
    cur cursor for
        select id, name, groupname, updated_at
        from dwh_dim_products;

    v_count integer := 0;
begin
    open cur;

    loop
        fetch cur into rec;
        exit when not found;

        v_count := v_count + 1;

    end loop;

    close cur;

    return v_count;
end;
$$;