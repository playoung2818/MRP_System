WITH params AS (
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago')::date AS as_of_date
), demands AS (
    SELECT btrim("QB Num") AS qb_num, upper(btrim("Item")) AS item,
           min("Name") AS customer, sum("Qty(-)")::double precision AS qty
    FROM public.wo_structured
    WHERE nullif(btrim("QB Num"), '') IS NOT NULL
      AND nullif(btrim("Item"), '') IS NOT NULL AND "Qty(-)" > 0
    GROUP BY 1, 2
), ledger AS (
    SELECT upper(btrim("Item")) AS item, "Date"::date AS event_date,
           "Kind" AS kind, btrim("QB Num") AS qb_num,
           coalesce("Delta", 0)::double precision AS delta,
           coalesce("Opening", 0)::double precision AS opening,
           row_number() OVER (ORDER BY ctid) AS event_id,
           CASE "Kind" WHEN 'OPEN' THEN 0 WHEN 'IN' THEN 1 WHEN 'ADJ' THEN 2 ELSE 3 END AS priority
    FROM public.ledger_analytics
), stock AS (
    SELECT item, max(opening) AS opening FROM ledger GROUP BY item
), adjusted AS (
    SELECT d.qb_num, d.item, l.event_date, l.priority, l.event_id, l.delta, s.opening
    FROM demands d JOIN ledger l USING (item) JOIN stock s USING (item)
    WHERE NOT (coalesce(l.qb_num, '') = d.qb_num AND l.kind = 'OUT')
      AND l.event_date IS NOT NULL
      AND NOT (l.kind = 'IN' AND l.event_date >= DATE '2099-12-31')
    UNION ALL
    -- Include today even if no ledger movement is scheduled today.
    SELECT d.qb_num, d.item, p.as_of_date, 9, 0::bigint, 0::double precision, s.opening
    FROM demands d JOIN stock s USING (item) CROSS JOIN params p
), balances AS (
    SELECT *, opening + sum(delta) OVER (
        PARTITION BY qb_num, item ORDER BY event_date, priority, event_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS projected
    FROM adjusted
), future AS (
    SELECT *, min(projected) OVER (
        PARTITION BY qb_num, item ORDER BY event_date, priority, event_id
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS future_min,
    row_number() OVER (
        PARTITION BY qb_num, item, event_date ORDER BY priority DESC, event_id DESC
    ) AS last_in_day
    FROM balances
), item_readiness AS (
    SELECT d.qb_num, d.item, d.customer, d.qty,
           min(f.event_date) FILTER (WHERE f.future_min >= d.qty) AS ready_date
    FROM demands d CROSS JOIN params p
    LEFT JOIN future f ON f.qb_num = d.qb_num AND f.item = d.item
        AND f.last_in_day = 1 AND f.event_date >= p.as_of_date
        AND f.event_date < DATE '2099-12-31'
    GROUP BY d.qb_num, d.item, d.customer, d.qty
), summary AS (
    SELECT qb_num AS "QB Num", min(customer) AS customer, count(*)::integer AS item_count,
           CASE WHEN bool_and(ready_date IS NOT NULL) THEN max(ready_date) END AS earliest_material_ready_date,
           count(*) FILTER (WHERE ready_date IS NULL)::integer AS blocking_item_count,
           coalesce(string_agg(item, ', ' ORDER BY item) FILTER (WHERE ready_date IS NULL), '') AS blocking_items,
           jsonb_agg(jsonb_build_object('item', item, 'required_qty', qty,
               'earliest_ready_date', ready_date) ORDER BY item) AS component_details
    FROM item_readiness GROUP BY qb_num
)
SELECT s.*, (s.earliest_material_ready_date IS NOT NULL) AS can_assign,
       coalesce(s.earliest_material_ready_date <= p.as_of_date, false) AS ready_today,
       p.as_of_date, CURRENT_TIMESTAMP AS calculated_at
FROM summary s CROSS JOIN params p
