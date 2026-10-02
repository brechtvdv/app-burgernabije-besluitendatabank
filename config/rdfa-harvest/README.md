Harvests a single web page: stores its RDFa triples, starts a PDF-to-enriched pipeline job for it, and once the job succeeds, sets the decision's content to the articles the pipeline found. Documents linked to a decision with `dct:isPartOf` (e.g. an annex) each become a new decision in the same session (see [Part decisions](#part-decisions)).

# REST API

## POST /rdfa-harvest?url={url}

The URL can also be passed as JSON body, `{ "url": "https://..." }`, with `Content-Type: application/vnd.api+json`; a plain `application/json` body isn't parsed.

1. Fetches the URL and parses its RDFa triples.
2. Removes what an earlier enrichment set on the page's decisions (see [Enriching the decision](#enriching-the-decision)): `eli:description`, `besluit:motivering`, `prov:value` and the articles it made, and for [part decisions](#part-decisions) also `eli:title`. Then inserts the triples in `TARGET_GRAPH` (default `http://mu.semte.ch/graphs/harvester-2`). Blank nodes are skolemized.
3. Derives `ext:governingBodyAbstract` for the sessions in `TARGET_GRAPH` that miss it, like the besluiten consumer does. mu-search reaches the location and governing body of sessions and agenda items through this link.
4. Finds the municipality of the page: the organization its sessions' governing body governs (`besluit:isGehoudenDoor` → `ext:governingBodyAbstract` → `besluit:bestuurt`, an `org:Organization`).
5. Sets `ext:owningBody` to that municipality on the harvested `besluit:Besluit` resources, in `TARGET_GRAPH`, like app-decide's municipality-linker. The decisions the job creates are linked by the municipality-linker from the job's municipality container.
6. Sets the URL as `prov:wasDerivedFrom` on the harvested `besluit:Agendapunt` and `besluit:Besluit` resources, in `TARGET_GRAPH`, like the besluiten consumer. The frontend lists these as the agenda item's sources ("Bronnen").
7. Removes what the pipeline made of the documents the page references (e.g. its PDFs, including the subjects of `dct:isPartOf`) before, so the pdf-scraper sees them as new and pdf-to-eli and the following steps process them completely again. For each manifestation exemplified by such a document, this removes:
   - the manifestation, its expressions (one per decision when split) and their translations, and their works (unless a work is shared with another manifestation)
   - annotations on those expressions, with their specific resources, selectors and statements, and embedding vectors
   - triples pointing at the removed resources, such as task results or validation results

   The deletes go through `JOB_SPARQL_ENDPOINT`, so app-decide's delta-notifier and search see them.
8. Creates a `pdf-to-enriched` harvesting job with a scheduled `singleton-job` task, like a "Harvest PDFs from Website URL" job created in the pipeline dashboard, but without a target shape (`ext:shapeForTargets`) for the URL. pdf-to-eli adds a shape for the expressions it makes, and the annotation-job-splitter reads only one of a job's shapes. When it picks the URL's, the translating task gets the URL instead of the expressions, and translating, segmenting and entity extracting have nothing to process. The task's input containers hold:
   - a harvesting collection with the URL as remote data object
   - the municipality as `task:hasResource`, which the municipality-linker uses to set the owning body of the resulting decisions. This container is left out when no municipality is found.
9. Queues the job. Every `JOB_POLL_INTERVAL` the queue checks the job's status. Once the job succeeds, it enriches the page's decision and makes the part decisions: see [Enriching the decision](#enriching-the-decision) and [Part decisions](#part-decisions). A canceled job is dropped. A failed job stays queued, since it can still succeed when a task is retried; like a job still running, it is dropped after `JOB_TIMEOUT`. The queue lives in memory, so a restart empties it; `POST /rdfa-harvest/enrich` picks a job up again.

### Response

#### 201 Created

```json
{
  "url": "https://example.org/besluiten",
  "graph": "http://mu.semte.ch/graphs/harvester-2",
  "triples": 42,
  "governingBodyLinks": 1,
  "municipalities": ["http://data.lblod.info/id/bestuurseenheden/..."],
  "owningBodyLinks": 1,
  "sourceLinks": 2,
  "partDecisions": ["http://data.lblod.info/id/besluiten/..."],
  "removedResources": 8,
  "job": "http://redpencil.data.gift/id/annotation-job/...",
  "task": "http://redpencil.data.gift/id/task/...",
  "inputContainer": "http://redpencil.data.gift/id/dataContainers/...",
  "municipalityContainer": "http://redpencil.data.gift/id/dataContainers/..."
}
```

#### 400 Bad Request

No valid http(s) URL given.

#### 502 Bad Gateway

The URL could not be fetched or parsed. No triples are inserted and no job is created.

## POST /rdfa-harvest/enrich?url={url}

Starts checking the latest `pdf-to-enriched` job for the URL again (found through the URL of its remote data object, or of its target shape for dashboard jobs), e.g. after a restart. The URL can also be passed as JSON body. The page is fetched again for its decisions and documents, and its agenda items and decisions get the URL as source (step 6), so pages harvested before that step existed get it too. Nothing else from the page is inserted.

- `200 OK`: the job succeeded already, and the decisions are enriched right away. Returns `{ url, job, status, enriched, expressions, articles, parts }`, or `skipped` with the reason when the page's decision wasn't enriched. `parts` has a result per [part decision](#part-decisions): `{ decision, document, parents, session, agendaItem, title, descriptions, motivations, articles }`, or `skipped` with the reason.
- `202 Accepted`: the job is still running, or failed and may still be retried, and is queued. Returns `{ url, job, status, queued: true }`.
- `404 Not Found`: no job targets the URL.
- `409 Conflict`: the job was canceled.
- `400` and `502`: as above.

### Enriching the decision

The pipeline's expressions are the ones embodied by the manifestations of the page's documents (not their translations), leaving out the documents of [part decisions](#part-decisions). Their annotation statements, ordered by where each starts in the text (`oa:hasTarget/oa:hasSelector/oa:start`) and joined with blank lines, replace properties of the harvested `besluit:Besluit` in `TARGET_GRAPH`:
- `ext:decision` → `eli:description`, after the decision's own description on the page and a warning that AI found it (`AI_WARNING`). The page's description is taken from the page every time, so enriching again doesn't repeat the AI part.
- `ext:motivation` → `besluit:motivering`
- `ext:article` → `prov:value`, and `besluit:Artikel` resources (below)

The pipeline can annotate the same text twice, e.g. an article and a copy of it that is cut off sooner; a statement whose text range (`oa:start`–`oa:end`) lies within another one's of the same expression is left out. A property without annotations keeps the value it had. The expressions can't be matched to decisions one by one, so a page with more than one decision is skipped, and so is a job that yields none of these annotations.

The frontend shows `prov:value` as the decision's content. Each article also becomes a `besluit:Artikel` in `TARGET_GRAPH`, linked from the decision with `eli:has_part`, with:
- `eli:number`: its position, zero-padded to the same width (`01` … `12`) because the frontend sorts articles by number as a string
- `prov:value`: its text
- `eli:language`: the expression's language
- `prov:wasDerivedFrom`: the annotation's statement
- `dct:creator`: this service

Enriching again replaces the articles this service made before; articles from the page itself are kept.

### Part decisions

A document on the page that is `dct:isPartOf` a decision, e.g. an annex linked with `<a rev="dct:isPartOf" href="….pdf">`:

```
<https://lblod.zottegem.be/.../GetPublication/?filename=Retributiereglement_..._71444.pdf> dct:isPartOf <https://lblod.zottegem.be/LBLODWeb/id/besluiten/758ae...-71444> .
```

becomes a decision of its own, described by that document instead of by the page. Only the object decides; the subject's type isn't checked. The document is left out of the page's own decision's enrichment, but its earlier processing is removed like that of the page's other documents (step 7). Without that, the pdf-scraper skips it because a manifestation of it exists already.

Once the job succeeds, for each such document that yields annotations, in `TARGET_GRAPH`:
- a new decision `http://data.lblod.info/id/besluiten/{id}` (`besluit:Besluit`) is `dct:isPartOf` the decisions the document is part of (its parents), with `ext:owningBody` set to the municipality
- a new agenda item (`besluit:Agendapunt`, `besluit:geplandOpenbaar true`) is linked from the session with `besluit:behandelt`. The session is the one treating a parent on the page (`besluit:behandelt` → agenda item ← `dct:subject` handling → `prov:generated` parent), or else the page's only session. Without a session, the document is skipped.
- a new handling (`besluit:BehandelingVanAgendapunt`, `besluit:openbaar true`) has the agenda item as `dct:subject` and generated (`prov:generated`) the new decision
- the agenda item and the decision get the page URL and the document as `prov:wasDerivedFrom`, and this service as `dct:creator`
- the first `eli:title` annotation of the document's expressions becomes the agenda item's `dct:title` and the decision's `eli:title`. Without one, the document's file name is used (the `filename` parameter or the last path segment, without `.pdf`).
- the decision is enriched with the document's `ext:decision`, `ext:motivation` and `ext:article` annotations like the page's decision

The decision, agenda item and handling get URIs and `mu:uuid`s derived from the document's URL, so harvesting or enriching again reuses them instead of making new ones.

## POST /rdfa-harvest/governing-body-abstract

Only runs step 3, e.g. for sessions harvested before this step existed. Returns `{ "graph": "...", "governingBodyLinks": 1 }`.

# Configuration

- `TARGET_GRAPH`: graph for the RDFa triples (default `http://mu.semte.ch/graphs/harvester-2`)
- `JOB_GRAPH`: graph for the job, task and containers (default `http://mu.semte.ch/graphs/harvesting`)
- `JOB_SPARQL_ENDPOINT`: endpoint for writing the job, task and containers (default `MU_SPARQL_ENDPOINT`). Point it at the database whose delta-notifier drives the pipeline, e.g. another app's database. The RDFa triples always go through `MU_SPARQL_ENDPOINT`.
- `DIRECT_DATABASE_ENDPOINT`: the triplestore itself (default `http://triplestore:8890/sparql`). When removing earlier processing, literals longer than `LONG_LITERAL_LENGTH` (default `1000`), such as an expression's full text, are deleted here. sparql-parser can't delete them: it checks deletes by sending the triples back to Virtuoso in a `VALUES` block, which Virtuoso refuses for long values (SR478). These deletes produce no deltas.
- `GOVERNING_BODY_LOOKUP_GRAPHS`: comma-separated graphs, next to `TARGET_GRAPH`, to classify governing bodies in (default `http://mu.semte.ch/graphs/mandaten,http://mu.semte.ch/graphs/organisations`)
- `BATCH_SIZE`: triples per insert query (default `100`)
- `AI_WARNING`: text between a decision's own description and the one found by AI (default `Let op: onderstaande tekst werd automatisch met AI uit het besluit gehaald en kan fouten bevatten.`)
- `JOB_POLL_INTERVAL`: milliseconds between checks of the queued jobs (default `30000`)
- `JOB_TIMEOUT`: milliseconds after which a queued job that hasn't finished is dropped (default 6 hours)
