select B.mac,
       A.full_account_number,
       A.lvl1,
       A.lvl2,
       A.lvl3,
       A.lvl4,
       A.lvl5,
       A.creation_time
from
(SELECT T2.S_FA_ID AS full_account_number,
    T1.id_number, T1.creation_time as creation_time, T1.case_type_lvl1 as lvl1, T1.case_type_lvl2 as lvl2, T1.case_type_lvl3 as lvl3, T1.x_case_type_lvl4 as lvl4, T1.x_case_type_lvl5 as lvl5
FROM ela_crm.table_case T1
LEFT JOIN ela_crm.table_fin_accnt T2
ON T1.CASE2FIN_ACCNT = T2.OBJID) AS A
JOIN
(select mac, full_account_number, MODEM_MANUFACTURER, event_date from hem.modem_accessibility_daily where MODEM_MANUFACTURER="HITRON" and event_date='2021-07-16') AS B
ON A.full_account_number=B.full_account_number