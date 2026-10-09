"""Train the item-order fallback from WO Details without modifying the database."""
import argparse
import json

from sqlalchemy import text

from mrp_system.normalize.mrp_normalize import refresh_part_number_aliases
from mrp_system.runtime.db_config import get_engine
from mrp_system.transform.item_order import (
    ItemOrderModel, MODEL_PATH, alias_fingerprint, evaluate, training_sequences,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(MODEL_PATH))
    parser.add_argument("--min-support", type=int, default=3, help="Minimum independent SO votes per item pair")
    parser.add_argument("--min-agreement", type=float, default=0.8)
    args = parser.parse_args()
    engine = get_engine()
    try:
        refresh_part_number_aliases(engine)
        with engine.connect() as conn:
            records = conn.execution_options(yield_per=1000).execute(text('''
                SELECT sales_order, items FROM public."WO Details"
                WHERE items IS NOT NULL
                ORDER BY "Generated Date" DESC NULLS LAST, pushed_at DESC NULLS LAST, id DESC
            ''')).mappings()
            sequences = training_sequences(records)
        if not sequences:
            raise ValueError("No usable WO sequences found; existing model was not replaced.")
        model = ItemOrderModel.train(sequences, min_support=args.min_support, min_agreement=args.min_agreement)
        model.metadata["alias_fingerprint"] = alias_fingerprint()
        model.metadata["validation"] = evaluate(sequences, min_support=args.min_support, min_agreement=args.min_agreement)
        model.save(args.output)
        print(json.dumps(model.metadata, indent=2))
        print("Saved model:", args.output)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
