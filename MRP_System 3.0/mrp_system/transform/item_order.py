"""Learn SO line ordering from ordered WO Details item arrays, not quantities.

Exact WO sequences take precedence over PDF references. For orders with no
reference, sufficiently consistent pairwise precedence votes provide a fallback.
"""
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile

import pandas as pd

from mrp_system.normalize.mrp_normalize import normalize_item
from .sales_order import normalize_wo_number


MODEL_PATH = Path(os.getenv("WO_ITEM_ORDER_MODEL_PATH") or (
    Path(__file__).resolve().parents[2] / "data" / "wo_item_order.json"
))
MODEL_VERSION = 1


def item_key(value):
    if value is None or pd.isna(value):
        return ""
    name = str(normalize_item(value)).strip()
    if name.casefold() in {"", "nan", "none", "<na>", "null"}:
        return ""
    return name.casefold()


def so_key(value):
    if value is None or pd.isna(value):
        return ""
    value = str(value).strip()
    if value.casefold() in {"", "nan", "none", "<na>", "null"}:
        return ""
    return normalize_wo_number(value).upper()


def alias_fingerprint():
    from mrp_system.normalize.part_aliases import part_alias_fingerprint
    return part_alias_fingerprint()


def training_sequences(records):
    """Records must be newest first. Deduplicate releases per SO and sequence."""
    result, seen = [], set()
    for record in records:
        order = so_key(record.get("sales_order"))
        items = record.get("items")
        if isinstance(items, str):
            try:
                items = json.loads(items)
            except (ValueError, TypeError):
                continue
        if not order or not isinstance(items, list):
            continue
        sequence = [item_key(line.get("item")) for line in items if isinstance(line, dict)]
        sequence = [key for key in sequence if key]
        identity = (order, tuple(sequence))
        if sequence and identity not in seen:
            seen.add(identity)
            result.append((order, sequence))
    return result


@dataclass
class ItemOrderModel:
    votes: dict = field(default_factory=dict)
    so_sequences: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    min_support: int = 3
    min_agreement: float = 0.8

    @classmethod
    def train(cls, sequences, *, min_support=3, min_agreement=0.8):
        if min_support < 1 or not 0.5 < min_agreement <= 1:
            raise ValueError("Use support >= 1 and agreement > 0.5 and <= 1.")
        references = defaultdict(list)
        per_so_votes = defaultdict(set)
        for order, sequence in sequences:
            if sequence not in references[order]:
                references[order].append(list(sequence))
            unique = list(dict.fromkeys(sequence))
            for i, before in enumerate(unique):
                for after in unique[i + 1:]:
                    per_so_votes[order].add((before, after))
        votes = defaultdict(Counter)
        for pairs in per_so_votes.values():
            for before, after in pairs:
                # Conflicting releases within one SO are not independent evidence.
                if (after, before) not in pairs:
                    votes[before][after] += 1
        return cls(
            votes={key: dict(value) for key, value in votes.items()},
            so_sequences=dict(references),
            metadata={"sales_orders": len(references),
                      "unique_sequences": sum(map(len, references.values())),
                      "trained_at": datetime.now(timezone.utc).isoformat()},
            min_support=min_support, min_agreement=min_agreement,
        )

    def reference(self, order, keys):
        """Choose the WO release covering most current lines; newest wins ties."""
        sequences = self.so_sequences.get(order, [])
        if not sequences:
            return []
        target = Counter(keys)
        best = max(sequences, key=lambda seq: sum((target & Counter(seq)).values()))
        return best if any(key in target for key in best) else []

    def predict(self, keys):
        """Stable topological order; ignore weak/tied evidence and handle cycles."""
        unique = list(dict.fromkeys(key for key in keys if key))
        edges = {key: {} for key in unique}
        for i, left in enumerate(unique):
            for right in unique[i + 1:]:
                forward = self.votes.get(left, {}).get(right, 0)
                reverse = self.votes.get(right, {}).get(left, 0)
                total = forward + reverse
                if total < self.min_support:
                    continue
                if forward / total >= self.min_agreement:
                    edges[left][right] = forward - reverse
                elif reverse / total >= self.min_agreement:
                    edges[right][left] = reverse - forward
        involved = {key for key, following in edges.items() if following}
        involved.update(after for following in edges.values() for after in following)
        remaining = [key for key in unique if key in involved]
        ordered = []
        while remaining:
            ready = [key for key in remaining if not any(key in edges[other] for other in remaining)]
            if ready:
                chosen = ready[0]
            else:
                # Contradictory evidence: choose strongest net precedence, stable on ties.
                chosen = max(remaining, key=lambda key: sum(edges[key].get(other, 0) - edges[other].get(key, 0) for other in remaining))
            ordered.append(chosen)
            remaining.remove(chosen)
        # Unknown/unconstrained items retain their original relative order.
        return ordered + [key for key in unique if key not in involved]

    def save(self, path=None):
        target = Path(path) if path is not None else MODEL_PATH
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": MODEL_VERSION, "votes": self.votes,
                   "so_sequences": self.so_sequences, "metadata": self.metadata,
                   "min_support": self.min_support, "min_agreement": self.min_agreement}
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=target.parent,
                    prefix=f".{target.name}.", suffix=".tmp", delete=False) as handle:
                temporary = Path(handle.name)
                json.dump(payload, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    @classmethod
    def load(cls, path=None):
        target = Path(path) if path is not None else MODEL_PATH
        with target.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict) or payload.get("version") != MODEL_VERSION:
            raise ValueError("Unsupported WO item-order model version; retrain it.")
        model = cls(**{key: payload[key] for key in ("votes", "so_sequences", "metadata", "min_support", "min_agreement")})
        if not isinstance(model.votes, dict) or not isinstance(model.so_sequences, dict) or not isinstance(model.metadata, dict):
            raise ValueError("Invalid WO item-order snapshot; retrain it.")
        if model.min_support < 1 or not 0.5 < model.min_agreement <= 1:
            raise ValueError("Invalid WO item-order thresholds; retrain it.")
        for key, following in model.votes.items():
            if not isinstance(key, str) or not isinstance(following, dict) or any(
                not isinstance(after, str) or not isinstance(count, int) or count < 1
                for after, count in following.items()
            ):
                raise ValueError("Invalid item-precedence votes; retrain the model.")
        for order, sequences in model.so_sequences.items():
            if not isinstance(order, str) or not isinstance(sequences, list) or any(
                not isinstance(seq, list) or any(not isinstance(key, str) for key in seq)
                for seq in sequences
            ):
                raise ValueError("Invalid WO reference sequences; retrain the model.")
        if model.metadata.get("alias_fingerprint") != alias_fingerprint():
            raise ValueError("Part-number aliases changed since training; retrain the WO item-order model.")
        return model


def evaluate(sequences, *, min_support=3, min_agreement=0.8):
    """Hold out entire SOs, including all releases, to prevent train/test leakage."""
    train, test = [], []
    for order, sequence in sequences:
        held_out = int(hashlib.sha256(order.encode()).hexdigest()[:8], 16) % 5 == 0
        (test if held_out else train).append((order, sequence))
    model = ItemOrderModel.train(train, min_support=min_support, min_agreement=min_agreement)
    exact, correct, pairs, supported, eligible = 0, 0, 0, 0, 0
    for _, sequence in test:
        expected = list(dict.fromkeys(sequence))
        if len(expected) < 2:
            continue
        eligible += 1
        # Alphabetical input removes the desired sequence from the prediction input.
        predicted = model.predict(sorted(expected))
        rank = {key: i for i, key in enumerate(predicted)}
        exact += predicted == expected
        for i, before in enumerate(expected):
            for after in expected[i + 1:]:
                pairs += 1
                correct += rank[before] < rank[after]
                forward = model.votes.get(before, {}).get(after, 0)
                reverse = model.votes.get(after, {}).get(before, 0)
                total = forward + reverse
                if total >= min_support and max(forward, reverse) / total >= min_agreement:
                    supported += 1
    return {"held_out_sales_orders": len({order for order, _ in test}),
            "evaluated_sequences": eligible,
            "exact_sequence_accuracy": exact / eligible if eligible else None,
            "pairwise_accuracy": correct / pairs if pairs else None,
            "supported_pair_fraction": supported / pairs if pairs else None}


def reorder_so_items(df, reference=None, model=None):
    """Change row order only. Reference matching uses canonical names and occurrences."""
    if df.empty:
        return df.copy().reset_index(drop=True)
    refs = defaultdict(list)
    if reference is not None and not reference.empty:
        for order, item in reference[["QB Num", "Item"]].itertuples(index=False, name=None):
            key = item_key(item)
            if key:
                refs[so_key(order)].append(key)
    groups = defaultdict(list)
    for position, order in enumerate(df["QB Num"]):
        groups[so_key(order)].append(position)
    result = []
    for order in sorted(groups):
        positions = groups[order]
        keys = [item_key(df.iloc[pos]["Item"]) for pos in positions]
        sequence = model.reference(order, keys) if model is not None else []
        if not sequence:
            sequence = refs.get(order, [])
        queues = defaultdict(deque)
        for pos, key in zip(positions, keys):
            queues[key].append(pos)
        matched = []
        for key in sequence:
            if queues[key]:
                matched.append(queues[key].popleft())
        used = set(matched)
        remaining = [pos for pos in positions if pos not in used]
        # A trusted reference is authoritative; unmatched rows stay in source order.
        if matched:
            result.extend(matched + remaining)
        elif model is not None:
            for key in model.predict(keys):
                result.extend(queues.pop(key, []))
            result.extend(pos for pos in remaining if not item_key(df.iloc[pos]["Item"]))
        else:
            result.extend(positions)
    return df.iloc[result].copy().reset_index(drop=True)
