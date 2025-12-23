{% macro load_data_from_s3(stage_name, target_table, source_file) %}

    {% set truncate_sql %}
        TRUNCATE TABLE WALMART_DB.RAW.{{ target_table }};
    {% endset %}

    {% set copy_sql %}
        COPY INTO WALMART_DB.RAW.{{ target_table }}
        FROM @WALMART_DB.RAW.{{ stage_name }}/{{ source_file }}
        FILE_FORMAT = (FORMAT_NAME = 'WALMART_DB.RAW.CSV_FORMAT')
        FORCE = TRUE
        ON_ERROR = 'CONTINUE';
    {% endset %}

    {{ log("Truncating " ~ target_table, info=True) }}
    {% do run_query(truncate_sql) %}

    {{ log("Copying " ~ source_file ~ " into " ~ target_table, info=True) }}
    {% do run_query(copy_sql) %}

{% endmacro %}