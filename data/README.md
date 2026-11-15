# data/

- Data is **downloaded manually in the browser** from its canonical source (Harvard Dataverse requires a guestbook form with an email), then verified and processed by script (see `docs/data-sources.md`). It is **never committed**: everything in this folder except this README and the manifest files is git-ignored.
- Download **data files only**. Never run code from a dataset repository.
- The manifest lives at `data/manifest.json` (one entry per downloaded file: source, download date, SHA-256 hash, license). `manifest-template.md` shows the fields.
- Planned layout: `data/raw/` (downloaded files as-is), `data/processed/` (subset, topology, derived KPIs, injected faults, ground-truth log).
