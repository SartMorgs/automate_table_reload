select
*
from database_name_athena.table_name
where date_field = date_add('DAY', {{ days_gone }}, current_date)