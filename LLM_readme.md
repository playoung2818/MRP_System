# LLM Integration Plan (Local)

Last updated: 2026-02-03

## Purpose
Add a local LLM assistant to the ERP system that can answer inventory/ATP questions, show a computation trace, and learn from user corrections (teacher mode).

## Scope (Phase 1)
- Chat UI embedded in existing web app
- Local LLM inference (no external API calls)
- Deterministic numeric answers (must be derived from ERP data)
- Computation trace shown for each numeric answer
- Teacher mode to capture corrections and preferred reasoning

## Non-Goals (Phase 1)
- Fully autonomous decision-making
- Freeform speculation without data
- Automatic model fine-tuning on every correction

## User Experience
### Chat
- Ask: "How many i9-14900 right now, and how many i9-14900 ATP?"
- Response includes:
  - Direct answer
  - Computation trace (inputs, formulas, intermediate totals)
  - Timestamp of data snapshot

### Teacher Mode
- User can submit:
  - Corrected answer
  - Corrected reasoning/steps
  - Tag(s) for error type (data source, formula, item mapping)
- System stores feedback and uses it in future answers

## Architecture Overview
1) UI Layer
- Chat panel + results panel
- Toggle for computation trace
- Teacher mode panel for corrections

2) Orchestration Layer
- Intent detection (inventory vs ATP vs drilldown)
- Tool selection
- Response composer

3) Data/Tools Layer
- Inventory snapshot tool
- ATP calculation tool
- Item normalization / alias resolver
- Audit log of tool calls + inputs

4) LLM Layer
- Local model (e.g., Llama/Mistral family)
- System prompt with domain rules
- Retrieval context (item aliases, definitions, recent corrections)

5) Learning Loop
- Feedback store
- Retrieval of similar corrections
- Optional scheduled fine-tuning (later phase)

## Data Contracts (Conceptual)
- inventory_snapshot(item) -> on_hand, on_hand_wip, on_po, on_so, timestamp
- atp(item, as_of_date, horizon) -> available_qty, first_shortage_date, timestamp
- item_lookup(query) -> canonical_item, aliases

## Reasoning / Trace Policy
- Show computation trace, not full chain-of-thought
- Trace includes:
  - Data sources used
  - Query filters
  - Arithmetic steps
  - Final computed result

## Guardrails
- Numeric answers must be backed by tool output
- If data is missing, respond with "cannot determine" + missing fields
- Always include data snapshot time
- Avoid hidden assumptions; list any defaults used

## Feedback Schema
- user_question
- model_answer
- teacher_answer
- reasoning_corrections
- tags (item, module, error_type)
- timestamp
- model_version + prompt_version

## Open Decisions
- Final definition of ATP (ledger vs custom rule)
- Authoritative data source for inventory
- Which tables are safe for cached reads
- Feedback weighting and retrieval policy

## Next Steps
- Agree on ATP definition
- Choose local model + hardware target
- Define tool interfaces + data sources
- Implement UI wireframe
- Add feedback storage and retrieval

## CLI (Backend) Usage
- Run: `python "ERP_System 2.0/llm_cli.py"`
- Optional local LLM: set `LLM_PROVIDER=ollama` and `OLLAMA_MODEL=llama3.1`
- Commands: `help`, `reload`, `exit`
