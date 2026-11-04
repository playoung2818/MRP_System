"""Read the same latest saved material blockers displayed by Planning."""
from sqlalchemy import bindparam, text


def with_planning_blockers(engine, rows):
    """Copy display rows; never cache review data alongside the ledger."""
    numbers = sorted({str(row.get('QB Num') or '').strip() for row in rows
                      if str(row.get('Kind') or '').strip().upper() != 'IN'} - {''})
    reviews = {}
    if numbers:
        query = text('''SELECT DISTINCT ON (qb_num) qb_num, blockers
            FROM public.mrp_review_history WHERE qb_num IN :numbers
            ORDER BY qb_num, assessed_at DESC, id DESC''').bindparams(bindparam('numbers', expanding=True))
        with engine.connect() as conn:
            reviews = {row['qb_num']: row['blockers'] for row in
                       conn.execute(query, {'numbers': numbers}).mappings()}
    result = []
    for row in rows:
        record = dict(row)
        number = str(row.get('QB Num') or '').strip()
        if 'Waiting_Item' in record:
            if str(row.get('Kind') or '').strip().upper() == 'IN':
                record['Waiting_Item'] = ''
            elif not number:
                record['Waiting_Item'] = '{}'
            elif number not in reviews:
                record['Waiting_Item'] = 'Not reviewed'
            else:
                items = sorted({str(blocker['item']).strip() for blocker in reviews[number] or []
                                if blocker.get('item')})
                record['Waiting_Item'] = '{' + ', '.join(items) + '}'
        result.append(record)
    return result
