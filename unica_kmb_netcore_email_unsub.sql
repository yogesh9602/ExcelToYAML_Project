select
crn_prospect
crn,
CASE 
        WHEN user_is_member_of("current_user"():: name, 'pii_users_group':: name) THEN cast(email as VARCHAR) ELSE sha2(cast(email as VARCHAR), 256) 
    END as email,
mobile,
name,
unsubscribe_link_url,
unica_load_date,
etl_created_by,
etl_created_time,
etl_last_updated_by,
etl_last_updated_time,
etl_change_flag,
wf_audit_id
from srcl.unica_kmb_netcore_email_unsub