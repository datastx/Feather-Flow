select
    activity_id
    , user_id
    , action
    , ts
from raw_activity
{% if is_exists() %} where ts > (select max(ts) from fct_activity) {% endif %}
