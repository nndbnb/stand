-- Одна бумага: прошлое и текущее состояние по интервалу [valid_from,valid_to).
-- Параметры: secid:String, past/today: DateTime64 с явным UTC.
SELECT SECID,attribute_kind,attribute,valid_from,valid_to,'past' AS point
FROM mart.dim_security WHERE SECID={secid:String} AND valid_from<={past:DateTime64(3, 'UTC')} AND {past:DateTime64(3, 'UTC')}<valid_to
UNION ALL
SELECT SECID,attribute_kind,attribute,valid_from,valid_to,'today' AS point
FROM mart.dim_security WHERE SECID={secid:String} AND valid_from<={today:DateTime64(3, 'UTC')} AND {today:DateTime64(3, 'UTC')}<valid_to;
