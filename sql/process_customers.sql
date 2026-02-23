create or replace procedure process_customers()
language plpgsql
as $$
declare
    rec record;
    cur cursor for
        select id, name, country, updated_at
        from dwh_dim_customers;

    v_start_time timestamp;
    v_end_time timestamp;
    v_duration numeric(12,3);
begin
    open cur;

    loop
        fetch cur into rec;
        exit when not found;

        v_start_time := clock_timestamp();

        begin
            -- например сountry is null это ошибка
            if rec.country is null then
                raise exception 'Country is null';
            end if;

            --например для ошибки uinique_violation нужен инсёрт

        exception
            when others then
                v_end_time := clock_timestamp();
                v_duration := extract(epoch from (v_end_time - v_start_time));

                insert into action_log(
                    object_name,
                    id,
                    start_time,
                    end_time,
                    duration_seconds,
                    error_message
                )
                values (
                    'dwh_dim_customers',
                    rec.id,
                    v_start_time,
                    v_end_time,
                    v_duration,
                    sqlerrm
                );
        end;
    end loop;

    close cur;
end;
$$;
