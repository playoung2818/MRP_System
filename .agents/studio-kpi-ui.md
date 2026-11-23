# Studio pilot: SO Planning KPI

## Decision and scope

- User selected Hallmark's Studio theme and approved the SO Planning KPI page as the first pilot.
- Apply Studio as an operational app restyle, not as a portfolio/marketing layout.
- Markdown is project memory; no Notion connection.
- Changed only `Webpage/templates/planning_kpi.html` and `Webpage/static/planning-kpi.css` for application behavior/presentation. Shared `lt-planning.css` is untouched.
- No backend, KPI calculation, database/schema, or workflow changes.

## Visual system

- Studio source: `Nutlope/hallmark`, `skills/hallmark/references/themes/studio.md` and its reference image, reviewed at implementation time.
- Cool proof paper (OKLCH hue 215), blue-black ink, restrained cadmium-orange Refresh action and active-nav marker.
- Hedvig Letters Serif headings/brand, Geist body/data; Georgia/system-ui fallbacks. Google Fonts adds an external font request; no application data is included in its URL. If unavailable, fallbacks preserve functionality.
- Hairline-divided KPI strip instead of separate blue-topped cards; serif detail heading; clean table surface.
- App mode deliberately omits crop marks, grain, decorative guides, hero layout and portfolio imagery.
- Sentence-case sidebar labels; removed redundant `PLANNING PERFORMANCE` eyebrow. Report wording, all six columns, real figures, links and conditional states retained.
- Keyboard-focusable, labelled table scroll region. Sticky table header; SO column pinned on mobile. Two-by-two metrics on smaller widths.
- Existing full, empty and error states remain. No artificial loading UI added to the server-rendered route.
- Page-only cache version: `planning-kpi.css?v=3`.

## Validation

- `PYTHONPATH='MRP_System 3.0' python -m pytest 'MRP_System 3.0/tests/test_planning_kpi.py' -q`: **17 passed**.
- Combined KPI/UI-assets run: **27 passed, 2 failed**. Remaining failures are `test_home_preserves_white_sidebar_snoopy_search_and_live_alerts` and `test_production_planning_removed`, in areas not modified by this pilot. Do not change these tests merely to make a UI restyle pass.
- `git diff --check`: passed.
- Hallmark script fetched to OS temp, not installed globally or added as a project dependency. `puppeteer-core` installed only in that temp tool directory; browser checks used installed Microsoft Edge.
- Live read-only GET: `http://192.168.60.215:5002/planning/kpi`.
- Hallmark rendered 1280x800, 1440x900, 1920x1080, plus full desktop and 375px mobile screenshots. Desktop and mobile screenshots inspected directly; screenshots contain operational data and remain outside Git in OS temp.
- Final rendered CSS/app sweep: **0 Floor FAIL, 1 Reflex, 2 warnings**. Not a claim of a completely clean full-repository lint.
  - Reflex 71: metric labels caption the preceding figures; generic heading-spacing heuristic treats them as headings for subsequent content. Retained deliberately.
  - F25: off-screen table columns on mobile; horizontal scroll with pinned SO identity is deliberate. No columns hidden.
  - Gate 3 warning: forced-colors active-navigation border is an accessibility fallback, not a decorative card stripe.
- No independent subagent review was available; manual screenshot review performed instead.

## Next

- User review of the Studio pilot before extending styles to other modules.
- Keep future rollouts scoped; do not recolor shared styles without inspecting all consuming screens.
