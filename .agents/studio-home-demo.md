# Studio homepage demo

## Live homepage background follow-up

User explicitly switched to modifying the current homepage and requested background dots only. Added subtle 24px radial dot grid to `.home-page .shell` in `Webpage/static/home.css`, with `--home-dot`. Existing sidebar/cards keep their solid backgrounds; layouts, greeting, search, alerts and workflows unchanged. Bumped homepage stylesheet to `home.css?v=11` in `Webpage/ui.py`. Live `/` GET confirmed new stylesheet reference; diff check passed. Demo has not replaced the production homepage.

- User then requested replacing dots with the zyuan.org notebook background (source inspected at `playoung2818/my_blog/app/globals.css`). Live homepage now uses off-white `#fbfbf9`, 1px horizontal rules every 27px, and a vertical margin 72px into the main area. Margin offsets account for 232px/212px desktop sidebar and mobile single-column layout. Sidebar and cards remain opaque; fonts and behavior unchanged. `home.css?v=12`, live GET reference and diff check verified. No remote CSS dependency added.

- Follow-up: merged notebook vertical margin into the actual sidebar divider instead of drawing an extra line inside content. Removed vertical background gradient/offsets; sidebar border uses notebook margin color. Horizontal rules remain; mobile uses its existing horizontal sidebar separator. Homepage CSS version 13.

- Follow-up: sidebar now shares notebook paper color and identical 27px horizontal ruling. Both shell and sidebar use viewport-anchored (`background-attachment: fixed`) gradients so lines align across the divider while scrolling and in mobile stacked layout. Cards remain solid. Homepage CSS version 14.

- Follow-up: added contrast by tinting sidebar paper light grey-green (`--home-sidebar-paper: #edf0ed`), retaining main off-white `#fbfbf9`, matching ruled lines and existing divider. Homepage CSS version 15; no layout/behavior changes.

- User prefers white rather than grey-green sidebar: `--home-sidebar-paper` is now `#ffffff`, contrasting with main off-white paper. Aligned horizontal lines retained. Homepage CSS version 16 supersedes version 15 tint.

- Removed the live homepage sidebar note at user's request: “Review sales orders and materials, assign production dates, then send completed plans to MES.” Removed its container from `INDEX_TPL`; sidebar navigation unchanged.

- Latest live-homepage preference: revert notebook paper to subtle 24px dot grid on light `#f5f7fa` canvas, keep sidebar plain white and standard divider. Removed notebook lines from both shell/sidebar. Removed sidebar note stays removed; all functionality unchanged. Homepage CSS version 17.

- White-sidebar correction: use `background: var(--home-sidebar-paper)` rather than `background-color` so legacy inline `.sidebar` gradient is reset, not left painted above white. Homepage CSS version 18.

- Removed live homepage heading/description “Keep production moving / Review materials, priorities, and upcoming work.” Kept the Open planning button, now right-aligned. Homepage CSS version 19.
- Live-server caveat: Flask debug reloader can miss edits saved while a restart is still importing/loading data (baseline mtimes are captured after startup). Symptom: served HTML references the previous CSS version. Touching `Webpage/ui.py` triggered reload; live GET then confirmed heading absent, v19 present, Open planning retained.

## Current revision: minimal operational homepage

User rejected the portfolio-like demo as messy and requested only three workspaces, Open Planning, and Needs Attention. The original design below is historical, not the current appearance.

- Same preview URL; live `/` remains unchanged.
- Follow-up: user requested more visual character, starting with background dots. Added a subtle 24px dot grid to the page canvas only; header, workspace links and alert surfaces remain solid for readability. CSS cache version is now 3.
- Removed oversized serif wordmark, all marketing descriptions, proof grid, crop marks, search form, module descriptions and footer. Compact Geist typography, pale Studio paper, simple rules and one orange Open Planning action.
- Follow-up: copied local-time Good morning/afternoon/evening greeting and existing Snoopy image into a compact top row. Snoopy opens a search bubble submitting GET `q` to `/global_search`; reuses `home-search.js` (Escape/outside-click/focus handling). No autocomplete added. CSS version 4, demo JS version 2. Four demo tests pass; JS syntax and diff checks pass.
- User selected top-bar navigation (option 3): Quotation Lookup, Item Information, Purchase Orders now sit beside the NTA MRP brand. Removed the standalone Workspaces heading/boxed row. Mobile navigation wraps into a second header row. Greeting is the page h1; Snoopy and dashboard remain in the main area. User subsequently chose compact spacing (option 1): desktop top margin 24px instead of 48px, greeting gap 12px instead of 28px, Snoopy 52×64px, dashboard header gap 10px, alert row padding 12px vertically, mobile page margin 16px. Layout and content unchanged. CSS version 6. Four tests pass, including top-bar destination assertions; diff check passes.
- User clarified the problem was unused space below the dashboard, not internal gaps. Revision 7 makes the main workspace fill available viewport height and lets Needs Attention/alert rows share the remaining height. Restored Snoopy to 72×88 and greeting gap to 20px. Content can grow/scroll naturally on small screens. Desktop live screenshot inspected and 4 tests pass. This revision replaces the compact-height approach rather than adding dummy dashboard content.
- Added `Webpage/static/studio-home-demo.js`: read-only same-origin GET of the existing homepage, extracts actual Needs Attention labels and links. No copied business formulas, fabricated figures, or new backend route. Text is rebuilt with `textContent`; destinations restricted to same-origin HTTP(S). This DOM dependency is acceptable for the preview; production integration should render the existing `alerts` directly.
- Loading, failure, no-script and existing empty-dashboard messages handled. Does not periodically refresh; reload preview for updated alerts.
- Three revised demo tests pass; JavaScript syntax check passed. Live desktop screenshot confirmed actual alert rows loaded and inspected for restrained layout.

- Revision 8: user rejected stretching and requested original format with larger fonts. Restored natural-height dashboard/rows, 48px desktop top margin, 28px greeting gap, 18px row padding and previous Snoopy size. Increased dashboard text 14→18px, section headings 14→20px, greeting 26→32px, desktop navigation/planning action to 16px. Top navigation wraps below 960px for larger labels. CSS version 8; 4 demo tests and diff check pass. This supersedes revision 7's viewport-filling layout.

- Revision 9: user requested bigger Snoopy. Increased image from 72×88 to 108×132px (50% larger), matching HTML dimensions; CSS cache version 9. Other layout settings unchanged.

- Revision 10: restored existing `/static/robot.svg` beside NTA MRP in the top-bar home link, at 35×35px with decorative alt text and labelled home destination. CSS cache version 10.

## Original scope (superseded visual design)

User requested a homepage replacement **demo**, specifically inspired by the opening screen at `https://www.usehallmark.com/themes/studio/`. Built a standalone preview; did not replace `/`, alter `INDEX_TPL`, add backend routes, or change operational data.

- Preview: `http://192.168.60.215:5002/static/studio-home-demo.html`
- Files: `Webpage/static/studio-home-demo.html`, `Webpage/static/studio-home-demo.css`.
- Reviewed the live reference HTML, Studio specification and reference image. Retains its opening composition: oversized serif wordmark, dot-grid proof paper, crop corners, orange construction border, split introductory text, orange primary action.
- Work-specific content and existing routes replace the reference's agency content. Search submits `q` via GET to `/global_search`; module links open existing application pages.
- No homepage KPI link. No fabricated alert counts, operational metrics, or testimonials. Explicit preview disclosure and return-to-current-homepage links.
- Uses Google Fonts (Hedvig Letters Serif and Geist), with Georgia/system fallbacks. No font metric values or dimensional readouts invented.
- User must review before integrating into the live homepage. Live alerts, autocomplete, Snoopy and current order-detail/customer-result rendering have not been ported; those belong to a subsequent integration, not a static demo.

## Validation

- Live preview GET returned HTTP 200.
- Hallmark render sweep via temporary tooling and installed Edge: screenshots at 1280x800, 1440x900, 1920x1080, full desktop and 375px mobile. Desktop and mobile inspected directly.
- Sweep in app scope: 0 FAIL, 1 warning (44ch one-sentence module description, retained as caption). This is not a claim of a full marketing-page audit or independent subagent review.
- `python -m pytest 'MRP_System 3.0/tests/test_studio_home_demo.py' -q`: 3 passed. Tests cover actual destinations, no homepage KPI link, real search form wiring, preview disclosure and basic accessibility CSS.
- `git diff --check` passed. Screenshots remain outside Git in OS temp.
- Existing unrelated modification in `MRP_System 3.0/mrp_system/runtime/paths.py` was left untouched.
