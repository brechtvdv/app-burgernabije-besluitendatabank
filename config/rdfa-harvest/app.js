import { createHash } from 'crypto';
import { app, uuid, errorHandler, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime } from "mu";
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { RdfaParser } from 'rdfa-streaming-parser';

const TARGET_GRAPH = process.env.TARGET_GRAPH || 'http://mu.semte.ch/graphs/harvester-2';
const JOB_GRAPH = process.env.JOB_GRAPH || 'http://mu.semte.ch/graphs/harvesting';
// Jobs are written through the database whose delta-notifier drives the pipeline (falls back
// to MU_SPARQL_ENDPOINT when unset)
const JOB_CONNECTION = { sparqlEndpoint: process.env.JOB_SPARQL_ENDPOINT };
// Virtuoso itself, for deleting literals too long for sparql-parser: it checks deletes by sending
// the triples back in a VALUES block, which Virtuoso refuses for long values (SR478)
const DIRECT_CONNECTION = { sparqlEndpoint: process.env.DIRECT_DATABASE_ENDPOINT || 'http://triplestore:8890/sparql' };
const LONG_LITERAL_LENGTH = parseInt(process.env.LONG_LITERAL_LENGTH || '1000');
const GOVERNING_BODY_LOOKUP_GRAPHS = (
  process.env.GOVERNING_BODY_LOOKUP_GRAPHS ||
  'http://mu.semte.ch/graphs/mandaten,http://mu.semte.ch/graphs/organisations'
).split(',');
// Resources of these types are described by the organisation data already; a page only
// references them, so their own triples on the page are not stored
const SKIPPED_SUBJECT_TYPES = (
  process.env.SKIPPED_SUBJECT_TYPES || 'http://data.vlaanderen.be/ns/besluit#Bestuursorgaan'
).split(',');
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '100');
// How often the queue checks whether its jobs are finished, and how long it keeps waiting for one
const JOB_POLL_INTERVAL = parseInt(process.env.JOB_POLL_INTERVAL || '30000');
const JOB_TIMEOUT = parseInt(process.env.JOB_TIMEOUT || String(6 * 60 * 60 * 1000));
const CREATOR = 'http://lblod.data.gift/services/rdfa-harvest-service';
// Put between a decision's own description and the description the pipeline's AI found
const AI_WARNING = process.env.AI_WARNING ||
  'Let op: onderstaande tekst werd automatisch met AI uit het besluit gehaald en kan fouten bevatten.';

const JOB_OPERATION = 'http://lblod.data.gift/id/jobs/concept/JobOperation/harvesting/pdf-to-enriched';
const TASK_OPERATION = 'http://lblod.data.gift/id/jobs/concept/TaskOperation/singleton-job';
const STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
const STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
const STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';
const STATUS_CANCELED = 'http://redpencil.data.gift/id/concept/JobStatus/canceled';
const REQUEST_HEADER_HTML = 'http://data.lblod.info/request-headers/accept/text/html';

const XSD_STRING = 'http://www.w3.org/2001/XMLSchema#string';
const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
const IS_GEHOUDEN_DOOR = 'http://data.vlaanderen.be/ns/besluit#isGehoudenDoor';
const BESLUIT = 'http://data.vlaanderen.be/ns/besluit#Besluit';
const AGENDAPUNT = 'http://data.vlaanderen.be/ns/besluit#Agendapunt';
// Resources the frontend lists sources for, with prov:wasDerivedFrom
const SOURCED_TYPES = [AGENDAPUNT, BESLUIT];
const OWNING_BODY = 'http://mu.semte.ch/vocabularies/ext/owningBody';
const PROV_VALUE = 'http://www.w3.org/ns/prov#value';
const ELI_DESCRIPTION = 'http://data.europa.eu/eli/ontology#description';
const MOTIVERING = 'http://data.vlaanderen.be/ns/besluit#motivering';
const EXT_DECISION = 'http://mu.semte.ch/vocabularies/ext/decision';
const EXT_MOTIVATION = 'http://mu.semte.ch/vocabularies/ext/motivation';
const EXT_ARTICLE = 'http://mu.semte.ch/vocabularies/ext/article';
const ELI_TITLE = 'http://data.europa.eu/eli/ontology#title';
const DCT_TITLE = 'http://purl.org/dc/terms/title';
const DCT_IS_PART_OF = 'http://purl.org/dc/terms/isPartOf';
const DCT_SUBJECT = 'http://purl.org/dc/terms/subject';
const PROV_GENERATED = 'http://www.w3.org/ns/prov#generated';
const BEHANDELT = 'http://data.vlaanderen.be/ns/besluit#behandelt';
const ENRICHED_PROPERTIES = [ELI_DESCRIPTION, MOTIVERING, PROV_VALUE];
// A part decision's title comes from the pipeline as well
const PART_ENRICHED_PROPERTIES = [...ENRICHED_PROPERTIES, ELI_TITLE];
const PREFIXES = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX hrvst: <http://lblod.data.gift/vocabularies/harvesting/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX rpioHttp: <http://redpencil.data.gift/vocabularies/http/>
  PREFIX sh: <http://www.w3.org/ns/shacl#>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
`;

function isValidUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

async function fetchRdfaQuads(url) {
  const response = await fetch(url, { headers: { Accept: 'text/html,application/xhtml+xml' } });
  if (!response.ok) {
    throw new Error(`Fetching ${url} failed with status ${response.status}`);
  }
  const html = await response.text();
  const contentType = (response.headers.get('content-type') || 'text/html').split(';')[0].trim();

  return new Promise((resolve, reject) => {
    const quads = [];
    const parser = new RdfaParser({ baseIRI: response.url || url, contentType });
    parser
      .on('data', (quad) => quads.push(quad))
      .on('error', reject)
      .on('end', () => resolve(quads));
    parser.write(html);
    parser.end();
  });
}

// Blank nodes are skolemized so they stay consistent across insert batches
function termToSparql(term, blankNodes) {
  if (term.termType === 'NamedNode') {
    return sparqlEscapeUri(term.value);
  }
  if (term.termType === 'BlankNode') {
    if (!blankNodes.has(term.value)) {
      blankNodes.set(term.value, `http://data.lblod.info/.well-known/genid/${uuid()}`);
    }
    return sparqlEscapeUri(blankNodes.get(term.value));
  }
  const literal = sparqlEscapeString(term.value);
  if (term.language) return `${literal}@${term.language}`;
  if (term.datatype && term.datatype.value !== XSD_STRING) {
    return `${literal}^^${sparqlEscapeUri(term.datatype.value)}`;
  }
  return literal;
}

function withoutSkippedSubjects(quads) {
  const skipped = new Set(
    quads
      .filter((q) => q.predicate.value === RDF_TYPE && SKIPPED_SUBJECT_TYPES.includes(q.object.value))
      .map((q) => q.subject.value)
  );
  return quads.filter((q) => !skipped.has(q.subject.value));
}

async function insertQuads(quads) {
  const blankNodes = new Map();
  const triples = quads.map((q) =>
    `${termToSparql(q.subject, blankNodes)} ${termToSparql(q.predicate, blankNodes)} ${termToSparql(q.object, blankNodes)} .`
  );
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    await update(`
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
          ${triples.slice(i, i + BATCH_SIZE).join('\n')}
        }
      }
    `);
  }
}

// Derives ext:governingBodyAbstract for sessions in the target graph that miss it, like the
// besluiten consumer does: mu-search reaches location and governing body through this link.
// The abstract body is the one the session's body is a time specialisation of, or the body
// itself when it is abstract already.
async function deriveGoverningBodyAbstract() {
  const lookupGraphs = [TARGET_GRAPH, ...GOVERNING_BODY_LOOKUP_GRAPHS].map(sparqlEscapeUri).join(' ');
  const result = await query(`
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT DISTINCT ?subject ?abstract WHERE {
      GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} { ?subject besluit:isGehoudenDoor ?body . }
      VALUES ?lookupGraph { ${lookupGraphs} }
      {
        GRAPH ?lookupGraph { ?body mandaat:isTijdspecialisatieVan ?abstract . }
      }
      UNION
      {
        GRAPH ?lookupGraph { ?body a besluit:Bestuursorgaan . }
        FILTER NOT EXISTS { GRAPH ?specialisationGraph { ?body mandaat:isTijdspecialisatieVan ?otherAbstract . } }
        FILTER NOT EXISTS { GRAPH ?bindingGraph { ?body mandaat:bindingStart ?bindingStart . } }
        BIND(?body AS ?abstract)
      }
      FILTER NOT EXISTS {
        GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} { ?subject ext:governingBodyAbstract ?abstract . }
      }
    }
  `);
  const triples = result.results.bindings.map((b) =>
    `${sparqlEscapeUri(b.subject.value)} <http://mu.semte.ch/vocabularies/ext/governingBodyAbstract> ${sparqlEscapeUri(b.abstract.value)} .`
  );
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    await update(`
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
          ${triples.slice(i, i + BATCH_SIZE).join('\n')}
        }
      }
    `);
  }
  return triples.length;
}

// The municipalities governed by the bodies holding the given sessions. Relies on
// ext:governingBodyAbstract being derived already.
async function findMunicipalities(sessions) {
  if (!sessions.length) return [];
  const result = await query(`
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX org: <http://www.w3.org/ns/org#>
    SELECT DISTINCT ?municipality WHERE {
      VALUES ?session { ${sessions.map(sparqlEscapeUri).join(' ')} }
      GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} { ?session ext:governingBodyAbstract ?body . }
      GRAPH ?bodyGraph { ?body besluit:bestuurt ?municipality . }
      GRAPH ?organizationGraph { ?municipality a org:Organization . }
    }
  `);
  return result.results.bindings.map((b) => b.municipality.value);
}

// The documents (e.g. PDFs) the page references, including those of part decisions: these are the
// subject of dct:isPartOf (e.g. <a rev="dct:isPartOf" href="….pdf">), not an object
function referencedDocuments(quads) {
  return [...new Set([
    ...quads.filter((q) => q.object.termType === 'NamedNode').map((q) => q.object.value),
    ...partDecisions(quads).map((p) => p.document),
  ])];
}

async function selectValues(queryString, variable) {
  const result = await query(queryString);
  return [...new Set(result.results.bindings.map((b) => b[variable].value))];
}

// Removes what the pipeline made of the given documents before, so the pdf-scraper sees them as
// new and pdf-to-eli and the following steps process them completely again: the manifestations
// exemplified by a document, their expressions (including translations) and works, and what
// annotates those expressions (annotations, specific resources with their selectors, statements,
// embedding vectors). Triples pointing at removed resources, like task results or validation
// results, are removed as well. Deletes go through the job endpoint, so app-decide's
// delta-notifier (and search) see them.
async function removePreviousProcessing(documents) {
  if (!documents.length) return 0;
  const values = (resources) => resources.map(sparqlEscapeUri).join(' ');
  const prefixes = `
    PREFIX eli: <http://data.europa.eu/eli/ontology#>
    PREFIX gold: <http://purl.org/linguistics/gold/>
    PREFIX oa: <http://www.w3.org/ns/oa#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  `;

  const manifestations = await selectValues(`${prefixes}
    SELECT DISTINCT ?manifestation WHERE {
      VALUES ?document { ${values(documents)} }
      GRAPH ?g { ?manifestation eli:is_exemplified_by ?document . }
    }
  `, 'manifestation');
  if (!manifestations.length) return 0;

  // Works shared with an expression of another manifestation are kept. Filtered here: Virtuoso
  // fails on that filter in SPARQL ("SQ200: Stack Overflow in cost model").
  const candidateWorks = await selectValues(`${prefixes}
    SELECT DISTINCT ?work WHERE {
      VALUES ?manifestation { ${values(manifestations)} }
      ?expression eli:is_embodied_by ?manifestation ; eli:realizes ?work .
    }
  `, 'work');
  const shared = candidateWorks.length ? (await query(`${prefixes}
    SELECT DISTINCT ?work ?manifestation WHERE {
      VALUES ?work { ${values(candidateWorks)} }
      ?work eli:is_realized_by ?expression .
      ?expression eli:is_embodied_by ?manifestation .
    }
  `)).results.bindings.filter((b) => !manifestations.includes(b.manifestation.value)).map((b) => b.work.value) : [];
  const works = candidateWorks.filter((w) => !shared.includes(w));

  // The embodied expressions (one per decision when split) and their translations
  const expressions = await selectValues(`${prefixes}
    SELECT DISTINCT ?expression WHERE {
      VALUES ?manifestation { ${values(manifestations)} }
      ?original eli:is_embodied_by ?manifestation .
      ?original gold:translation? ?expression .
    }
  `, 'expression');

  const dependents = expressions.length ? await selectValues(`${prefixes}
    SELECT DISTINCT ?resource WHERE {
      VALUES ?expression { ${values(expressions)} }
      { ?resource oa:hasTarget ?expression . }
      UNION { ?resource oa:hasSource ?expression . }
      UNION { ?target oa:hasSource ?expression . ?target oa:hasSelector ?resource . }
      UNION { ?target oa:hasSource ?expression . ?resource oa:hasTarget ?target . }
      UNION { ?resource rdf:subject ?expression . }
      UNION { ?statement rdf:subject ?expression . ?resource oa:hasBody ?statement . }
      UNION { ?expression ext:embeddingVector ?resource . }
    }
  `, 'resource') : [];

  const resources = [...new Set([...manifestations, ...works, ...expressions, ...dependents])];
  for (let i = 0; i < resources.length; i += BATCH_SIZE) {
    const batch = values(resources.slice(i, i + BATCH_SIZE));
    // Long texts (e.g. an expression's content) go directly; the rest still yields deltas
    await update(`
      DELETE { GRAPH ?g { ?resource ?p ?o . } }
      WHERE {
        VALUES ?resource { ${batch} }
        GRAPH ?g { ?resource ?p ?o . }
        FILTER(isLiteral(?o) && STRLEN(STR(?o)) > ${LONG_LITERAL_LENGTH})
      }
    `, {}, DIRECT_CONNECTION);
    await update(`
      DELETE { GRAPH ?g { ?resource ?p ?o . } }
      WHERE { VALUES ?resource { ${batch} } GRAPH ?g { ?resource ?p ?o . } }
    `, {}, JOB_CONNECTION);
    await update(`
      DELETE { GRAPH ?g { ?s ?p ?resource . } }
      WHERE { VALUES ?resource { ${batch} } GRAPH ?g { ?s ?p ?resource . } }
    `, {}, JOB_CONNECTION);
  }
  return resources.length;
}

// Sets ext:owningBody to the municipalities on the harvested besluit:Besluit resources, like
// app-decide's municipality-linker does for the decisions a job produces
async function linkOwningBody(quads, municipalities) {
  if (!municipalities.length) return 0;
  const owningBodies = municipalities.map(sparqlEscapeUri).join(', ');

  const decisions = harvestedDecisions(quads);
  if (decisions.length) {
    await update(`
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
          ${decisions.map((d) => `${sparqlEscapeUri(d)} <${OWNING_BODY}> ${owningBodies} .`).join('\n')}
        }
      }
    `);
  }
  return decisions.length;
}

// Sets the page URL as prov:wasDerivedFrom on the harvested agenda items and decisions, which the
// frontend lists as their sources, like the besluiten consumer does
async function linkSources(quads, url) {
  const resources = [...new Set(
    quads
      .filter((q) => q.predicate.value === RDF_TYPE && SOURCED_TYPES.includes(q.object.value) && q.subject.termType === 'NamedNode')
      .map((q) => q.subject.value)
  )];
  if (resources.length) {
    await update(`
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
          ${resources.map((r) => `${sparqlEscapeUri(r)} <http://www.w3.org/ns/prov#wasDerivedFrom> ${sparqlEscapeString(url)} .`).join('\n')}
        }
      }
    `);
  }
  return resources.length;
}

// Mirrors the "Harvest PDFs from Website URL" job created by the pipeline dashboard. The
// municipalities go in a second input container of the first task, where app-decide's
// municipality-linker picks them up to set ext:owningBody on the resulting decisions.
// Unlike the dashboard, the job gets no target shape for the URL: pdf-to-eli adds a shape for
// the expressions it makes, and the annotation-job-splitter only reads one of the job's shapes.
// When it reads the URL's, translating gets the URL instead of the expressions, and translating,
// segmenting and entity extracting find nothing to process.
async function createPdfToEnrichedJob(url, municipalities) {
  const now = sparqlEscapeDateTime(new Date());
  const ids = {
    job: uuid(), remoteDataObject: uuid(),
    collection: uuid(), container: uuid(), municipalityContainer: uuid(), task: uuid(),
  };
  const job = `http://redpencil.data.gift/id/annotation-job/${ids.job}`;
  const remoteDataObject = `http://data.lblod.info/id/remote-data-objects/${ids.remoteDataObject}`;
  const collection = `http://data.lblod.info/id/harvesting-collection/${ids.collection}`;
  const container = `http://redpencil.data.gift/id/dataContainers/${ids.container}`;
  const municipalityContainer = municipalities.length
    ? `http://redpencil.data.gift/id/dataContainers/${ids.municipalityContainer}`
    : null;
  const task = `http://redpencil.data.gift/id/task/${ids.task}`;
  const inputContainers = [container, municipalityContainer].filter(Boolean);

  await update(`
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(JOB_GRAPH)} {
        ${sparqlEscapeUri(job)} a cogs:Job, ext:AnnotationJob ;
          mu:uuid ${sparqlEscapeString(ids.job)} ;
          adms:status ${sparqlEscapeUri(STATUS_BUSY)} ;
          dct:created ${now} ;
          dct:modified ${now} ;
          dct:creator ${sparqlEscapeUri(CREATOR)} ;
          task:operation ${sparqlEscapeUri(JOB_OPERATION)} ;
          ext:splitDecisions true ;
          ext:confidenceThreshold 0 .

        ${sparqlEscapeUri(remoteDataObject)} a nfo:RemoteDataObject ;
          mu:uuid ${sparqlEscapeString(ids.remoteDataObject)} ;
          nie:url ${sparqlEscapeUri(url)} ;
          rpioHttp:requestHeader ${sparqlEscapeUri(REQUEST_HEADER_HTML)} ;
          dct:created ${now} ;
          dct:modified ${now} ;
          dct:creator ${sparqlEscapeUri(CREATOR)} .

        ${sparqlEscapeUri(collection)} a hrvst:HarvestingCollection ;
          mu:uuid ${sparqlEscapeString(ids.collection)} ;
          dct:creator ${sparqlEscapeUri(CREATOR)} ;
          dct:hasPart ${sparqlEscapeUri(remoteDataObject)} .

        ${sparqlEscapeUri(container)} a nfo:DataContainer ;
          mu:uuid ${sparqlEscapeString(ids.container)} ;
          task:hasHarvestingCollection ${sparqlEscapeUri(collection)} .

        ${municipalityContainer ? `${sparqlEscapeUri(municipalityContainer)} a nfo:DataContainer ;
          mu:uuid ${sparqlEscapeString(ids.municipalityContainer)} ;
          task:hasResource ${municipalities.map(sparqlEscapeUri).join(', ')} .` : ''}
      }
    }
  `, {}, JOB_CONNECTION);

  // Inserted last: the scheduled status triggers the pipeline through the delta-notifier
  await update(`
    ${PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(JOB_GRAPH)} {
        ${sparqlEscapeUri(task)} a task:Task ;
          mu:uuid ${sparqlEscapeString(ids.task)} ;
          adms:status ${sparqlEscapeUri(STATUS_SCHEDULED)} ;
          dct:created ${now} ;
          dct:modified ${now} ;
          task:operation ${sparqlEscapeUri(TASK_OPERATION)} ;
          task:index "0" ;
          task:inputContainer ${inputContainers.map(sparqlEscapeUri).join(', ')} ;
          dct:isPartOf ${sparqlEscapeUri(job)} .
      }
    }
  `, {}, JOB_CONNECTION);

  return { job, task, inputContainer: container, municipalityContainer };
}

// The harvested besluit:Besluit resources of the page
function harvestedDecisions(quads) {
  return [...new Set(
    quads
      .filter((q) => q.predicate.value === RDF_TYPE && q.object.value === BESLUIT && q.subject.termType === 'NamedNode')
      .map((q) => q.subject.value)
  )];
}

// The documents on the page that are dct:isPartOf a decision, e.g. an annex linked with
// <a rev="dct:isPartOf" href="….pdf">. Each such document is a decision of its own, described by
// the document instead of by the page, and part of the decisions it is linked to (its parents).
// Its URI is derived from the document, so harvesting again reuses it.
function partDecisions(quads) {
  const parts = new Map();
  for (const q of quads) {
    if (q.predicate.value !== DCT_IS_PART_OF || q.object.termType !== 'NamedNode' || q.subject.termType !== 'NamedNode') continue;
    const document = q.subject.value;
    if (!parts.has(document)) {
      parts.set(document, {
        document, decision: `http://data.lblod.info/id/besluiten/${stableId('decision', document)}`, parents: [],
      });
    }
    const { parents } = parts.get(document);
    if (!parents.includes(q.object.value)) parents.push(q.object.value);
  }
  return [...parts.values()];
}

// The session treating the decisions on the page (the session besluit:behandelt the agenda item
// whose handling prov:generated a decision), or else the page's only session
function sessionOf(quads, decisions) {
  const objectsOf = (subjects, predicate) => quads
    .filter((q) => q.predicate.value === predicate && subjects.includes(q.subject.value))
    .map((q) => q.object.value);
  const subjectsOf = (predicate, objects) => quads
    .filter((q) => q.predicate.value === predicate && objects.includes(q.object.value))
    .map((q) => q.subject.value);
  const sessions = [...new Set(subjectsOf(BEHANDELT, objectsOf(subjectsOf(PROV_GENERATED, decisions), DCT_SUBJECT)))];
  if (sessions.length === 1) return sessions[0];
  const page = pageSessions(quads);
  return page.length === 1 ? page[0] : undefined;
}

// The page's sessions: the subjects of besluit:isGehoudenDoor
function pageSessions(quads) {
  return [...new Set(
    quads
      .filter((q) => q.predicate.value === IS_GEHOUDEN_DOOR && q.subject.termType === 'NamedNode')
      .map((q) => q.subject.value)
  )];
}

// What the queue needs to enrich the page's decision and make its part decisions once the job
// succeeds. The part decisions' documents are left out of the page's own decision's documents.
function enrichmentItem(url, quads, municipalities) {
  const parts = partDecisions(quads).map((part) => ({ ...part, session: sessionOf(quads, part.parents) }));
  const partDocuments = new Set(parts.map((p) => p.document));
  return {
    url,
    decisions: harvestedDecisions(quads),
    descriptions: pageDescriptions(quads),
    documents: referencedDocuments(quads).filter((d) => !partDocuments.has(d)),
    parts,
    municipalities,
  };
}

// The page's own eli:description of each harvested decision, as one text per decision. The same
// description often occurs more than once, differing only in whitespace.
function pageDescriptions(quads) {
  const descriptions = {};
  for (const q of quads) {
    if (q.predicate.value !== ELI_DESCRIPTION || q.subject.termType !== 'NamedNode') continue;
    const text = q.object.value.trim();
    const known = (descriptions[q.subject.value] ||= []);
    if (text && !known.some((d) => d.replace(/\s+/g, ' ') === text.replace(/\s+/g, ' '))) known.push(text);
  }
  return Object.fromEntries(Object.entries(descriptions).map(([decision, texts]) => [decision, texts.join('\n\n')]));
}

// The latest pdf-to-enriched job for the URL, whether this service or the dashboard created it.
// The URL is the remote data object in its first task's input, or, for dashboard jobs and older
// jobs of this service, the target node of its shape.
async function findLatestJob(url) {
  const result = await query(`
    ${PREFIXES}
    SELECT ?job ?status WHERE {
      GRAPH ${sparqlEscapeUri(JOB_GRAPH)} {
        ?job a cogs:Job ;
          task:operation ${sparqlEscapeUri(JOB_OPERATION)} ;
          dct:created ?created ;
          adms:status ?status .
        {
          ?task dct:isPartOf ?job ;
            task:inputContainer/task:hasHarvestingCollection/dct:hasPart/nie:url ${sparqlEscapeUri(url)} .
        } UNION {
          ?job ext:shapeForTargets/sh:targetNode ${sparqlEscapeUri(url)} .
        }
      }
    }
    ORDER BY DESC(?created)
    LIMIT 1
  `, {}, JOB_CONNECTION);
  const binding = result.results.bindings[0];
  return binding ? { job: binding.job.value, status: binding.status.value } : null;
}

async function jobStatus(job) {
  const result = await query(`
    ${PREFIXES}
    SELECT ?status WHERE {
      GRAPH ${sparqlEscapeUri(JOB_GRAPH)} { ${sparqlEscapeUri(job)} adms:status ?status . }
    }
  `, {}, JOB_CONNECTION);
  return result.results.bindings[0]?.status.value;
}

// The expressions the pipeline made of the documents (one per decision when split), oldest first.
// Translations are left out: they are not embodied by a manifestation.
async function expressionsOfDocuments(documents) {
  if (!documents.length) return [];
  const result = await query(`
    PREFIX eli: <http://data.europa.eu/eli/ontology#>
    PREFIX dct: <http://purl.org/dc/terms/>
    SELECT DISTINCT ?expression ?created WHERE {
      VALUES ?document { ${documents.map(sparqlEscapeUri).join(' ')} }
      ?manifestation eli:is_exemplified_by ?document .
      ?expression eli:is_embodied_by ?manifestation .
      OPTIONAL { ?expression dct:created ?created . }
    }
  `, {}, JOB_CONNECTION);
  return result.results.bindings
    .sort((a, b) => (a.created?.value || '').localeCompare(b.created?.value || '') || a.expression.value.localeCompare(b.expression.value))
    .map((b) => b.expression.value);
}

// The pipeline can annotate the same text twice, e.g. an article and a copy of it that is cut off
// sooner. A statement whose text lies within another statement's text of the same expression is
// left out; of statements with the same text, one is kept.
function withoutNestedStatements(statements) {
  return statements.filter((s) => !statements.some((other) =>
    other !== s && other.order === s.order && other.start <= s.start && s.end <= other.end &&
    (other.start < s.start || s.end < other.end || other.statement < s.statement)
  ));
}

// The statements with the predicate (e.g. ext:article) annotating the expressions, in the order
// of the expressions and of where each statement starts in its expression's text
async function statementsOf(expressions, predicate) {
  if (!expressions.length) return [];
  const result = await query(`
    PREFIX eli: <http://data.europa.eu/eli/ontology#>
    PREFIX oa: <http://www.w3.org/ns/oa#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?expression ?language ?statement ?start ?end ?object WHERE {
      VALUES ?expression { ${expressions.map(sparqlEscapeUri).join(' ')} }
      ?annotation oa:hasBody ?statement ;
        oa:hasTarget/oa:hasSelector ?selector .
      ?selector oa:start ?start .
      OPTIONAL { ?selector oa:end ?end . }
      ?statement rdf:subject ?expression ;
        rdf:predicate ${sparqlEscapeUri(predicate)} ;
        rdf:object ?object .
      OPTIONAL { ?expression eli:language ?language . }
    }
  `, {}, JOB_CONNECTION);
  // A statement with several selectors counts once, at its first start. Without an end, the
  // statement's text is taken to run from the start.
  const statements = new Map();
  for (const b of result.results.bindings) {
    const start = Number(b.start.value);
    const known = statements.get(b.statement.value);
    if (!known || start < known.start) {
      statements.set(b.statement.value, {
        order: expressions.indexOf(b.expression.value), start,
        end: b.end ? Number(b.end.value) : start + b.object.value.length,
        statement: b.statement.value, value: b.object.value.trim(), language: b.language?.value,
      });
    }
  }
  return withoutNestedStatements([...statements.values()]).sort((a, b) => a.order - b.order || a.start - b.start);
}

// Replaces the subject's values for the predicate, e.g. a decision's prov:value, which the
// frontend shows as the decision's content
async function setProperty(subject, predicate, value) {
  await update(`
    DELETE { GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} { ${sparqlEscapeUri(subject)} ${sparqlEscapeUri(predicate)} ?value . } }
    WHERE { GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} { ${sparqlEscapeUri(subject)} ${sparqlEscapeUri(predicate)} ?value . } }
  `);
  await update(`
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
        ${sparqlEscapeUri(subject)} ${sparqlEscapeUri(predicate)} ${sparqlEscapeString(value)} .
      }
    }
  `);
}

// Removes the besluit:Artikel resources this service made for the decision
async function removeDecisionArticles(decision) {
  await update(`
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX eli: <http://data.europa.eu/eli/ontology#>
    DELETE {
      GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
        ${sparqlEscapeUri(decision)} eli:has_part ?article .
        ?article ?p ?o .
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
        ${sparqlEscapeUri(decision)} eli:has_part ?article .
        ?article a besluit:Artikel ;
          dct:creator ${sparqlEscapeUri(CREATOR)} ;
          ?p ?o .
      }
    }
  `);
}

// Removes what an earlier enrichment set on the decisions: the properties it replaces and the
// articles it made. Harvesting the page again inserts the page's own values of those properties,
// which would otherwise stand next to the enriched ones until the new job enriches them again.
async function resetEnrichment(decisions, properties = ENRICHED_PROPERTIES) {
  for (const decision of decisions) {
    await update(`
      DELETE { GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} { ${sparqlEscapeUri(decision)} ?p ?value . } }
      WHERE {
        VALUES ?p { ${properties.map(sparqlEscapeUri).join(' ')} }
        GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} { ${sparqlEscapeUri(decision)} ?p ?value . }
      }
    `);
    await removeDecisionArticles(decision);
  }
}

// Replaces the besluit:Artikel resources this service made for the decision earlier by one per
// article, linked with eli:has_part. Articles from the page itself are kept. eli:number is the
// article's position, zero-padded because the frontend sorts articles by number as a string.
async function setDecisionArticles(decision, articles) {
  await removeDecisionArticles(decision);

  const width = String(articles.length).length;
  const triples = articles.map((article, index) => {
    const id = uuid();
    const uri = sparqlEscapeUri(`http://data.lblod.info/id/articles/${id}`);
    return `
      ${sparqlEscapeUri(decision)} eli:has_part ${uri} .
      ${uri} a besluit:Artikel ;
        mu:uuid ${sparqlEscapeString(id)} ;
        eli:number ${sparqlEscapeString(String(index + 1).padStart(width, '0'))} ;
        prov:value ${sparqlEscapeString(article.value)} ;
        ${article.language ? `eli:language ${sparqlEscapeUri(article.language)} ;` : ''}
        prov:wasDerivedFrom ${sparqlEscapeUri(article.statement)} ;
        dct:creator ${sparqlEscapeUri(CREATOR)} .`;
  });
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    await update(`
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX eli: <http://data.europa.eu/eli/ontology#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
          ${triples.slice(i, i + BATCH_SIZE).join('\n')}
        }
      }
    `);
  }
}

const joined = (statements) => statements.map((s) => s.value).join('\n\n');

// Enriches a decision with what the pipeline found in the expressions: the ext:decision
// annotations after its own description on the page (behind a warning that AI found them) as its
// eli:description, the ext:motivation annotations as its besluit:motivering, and the ext:article
// annotations joined as its prov:value and as besluit:Artikel resources. A property without
// annotations keeps its value.
async function enrichDecision(decision, expressions, ownDescription) {
  const descriptions = await statementsOf(expressions, EXT_DECISION);
  const motivations = await statementsOf(expressions, EXT_MOTIVATION);
  const articles = await statementsOf(expressions, EXT_ARTICLE);
  if (!descriptions.length && !motivations.length && !articles.length) {
    return { expressions, skipped: 'No decision, motivation or article annotations found' };
  }
  if (descriptions.length) {
    // Built from the page's description every time, so enriching again doesn't repeat the AI part
    const description = [ownDescription, AI_WARNING, joined(descriptions)].filter(Boolean).join('\n\n');
    await setProperty(decision, ELI_DESCRIPTION, description);
  }
  if (motivations.length) await setProperty(decision, MOTIVERING, joined(motivations));
  if (articles.length) {
    await setProperty(decision, PROV_VALUE, joined(articles));
    await setDecisionArticles(decision, articles);
  }
  return {
    expressions,
    descriptions: descriptions.length, motivations: motivations.length, articles: articles.length,
  };
}

// Enriches the page's decision with what the pipeline found in the page's documents. Expressions
// can't be told apart per decision, so a page with several decisions is left alone.
async function enrichDecisions({ decisions, documents, descriptions = {} }) {
  if (decisions.length !== 1) {
    return { enriched: [], skipped: `The page has ${decisions.length} decisions, expected 1` };
  }
  const [decision] = decisions;
  const result = await enrichDecision(decision, await expressionsOfDocuments(documents), descriptions[decision]);
  return { enriched: result.skipped ? [] : decisions, ...result };
}

// A mu:uuid that stays the same for the resource of this kind made for the URI, so making it again
// reuses the resource
function stableId(kind, uri) {
  return createHash('sha256').update(`${kind}:${uri}`).digest('hex').slice(0, 32);
}

// The document's file name, e.g. from GetPublication/?filename=Retributiereglement_..._71444.pdf
function documentName(document) {
  try {
    const url = new URL(document);
    const name = url.searchParams.get('filename') || decodeURIComponent(url.pathname.split('/').filter(Boolean).pop() || '');
    return name.replace(/\.pdf$/i, '').replace(/_/g, ' ').trim() || undefined;
  } catch {
    return undefined;
  }
}

// Makes the part decision an agenda item of the session: the session treats a new agenda item
// whose handling generated the decision, which is dct:isPartOf its parents. The agenda item and
// handling get URIs derived from the document, so making them again doesn't duplicate them.
async function linkPartDecision({ document, decision, parents, session, url, municipalities }) {
  const ids = {
    decision: stableId('decision', document),
    agendaItem: stableId('agenda-item', document),
    handling: stableId('handling', document),
  };
  const agendaItem = `http://data.lblod.info/id/agendapunten/${ids.agendaItem}`;
  const handling = `http://data.lblod.info/id/behandelingen-van-agendapunt/${ids.handling}`;
  const sources = [url, document].map(sparqlEscapeString).join(', ');
  await update(`
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
        ${sparqlEscapeUri(session)} besluit:behandelt ${sparqlEscapeUri(agendaItem)} .

        ${sparqlEscapeUri(agendaItem)} a besluit:Agendapunt ;
          mu:uuid ${sparqlEscapeString(ids.agendaItem)} ;
          besluit:geplandOpenbaar true ;
          prov:wasDerivedFrom ${sources} ;
          dct:creator ${sparqlEscapeUri(CREATOR)} .

        ${sparqlEscapeUri(handling)} a besluit:BehandelingVanAgendapunt ;
          mu:uuid ${sparqlEscapeString(ids.handling)} ;
          besluit:openbaar true ;
          dct:subject ${sparqlEscapeUri(agendaItem)} ;
          prov:generated ${sparqlEscapeUri(decision)} ;
          dct:creator ${sparqlEscapeUri(CREATOR)} .

        ${sparqlEscapeUri(decision)} a besluit:Besluit ;
          mu:uuid ${sparqlEscapeString(ids.decision)} ;
          dct:isPartOf ${parents.map(sparqlEscapeUri).join(', ')} ;
          ${municipalities.length ? `ext:owningBody ${municipalities.map(sparqlEscapeUri).join(', ')} ;` : ''}
          prov:wasDerivedFrom ${sources} ;
          dct:creator ${sparqlEscapeUri(CREATOR)} .
      }
    }
  `);
  return agendaItem;
}

// Makes a new decision of each part decision's document, with what the pipeline found in that
// document only: linked to the session through a new agenda item and handling, titled after the
// eli:title annotations (or the document's file name), and enriched like the page's decision. A
// document without annotations is left alone.
async function enrichPartDecisions({ url, parts = [], municipalities = [] }) {
  const results = [];
  for (const part of parts) {
    const { document, decision, parents, session } = part;
    if (!session) {
      results.push({ decision, document, parents, skipped: 'No session found: none treats its parents, and the page has no single session' });
      continue;
    }
    const expressions = await expressionsOfDocuments([document]);
    const titles = await statementsOf(expressions, ELI_TITLE);
    const { skipped, ...enrichment } = await enrichDecision(decision, expressions);
    if (skipped && !titles.length) {
      results.push({ decision, document, parents, expressions, skipped: 'No title, decision, motivation or article annotations found' });
      continue;
    }
    const agendaItem = await linkPartDecision({ ...part, url, municipalities });
    const title = titles[0]?.value || documentName(document);
    if (title) {
      await setProperty(agendaItem, DCT_TITLE, title);
      await setProperty(decision, ELI_TITLE, title);
    }
    results.push({ decision, document, parents, session, agendaItem, title, ...enrichment });
  }
  return results;
}

// Jobs waiting to finish, by job URI. Kept in memory: after a restart, POST /enrich picks a job
// up again.
const jobQueue = new Map();
let checkingQueue = false;

function enqueueJob(item) {
  jobQueue.set(item.job, { ...item, queuedAt: Date.now() });
}

async function checkQueue() {
  if (checkingQueue) return;
  checkingQueue = true;
  try {
    for (const item of [...jobQueue.values()]) {
      try {
        const status = await jobStatus(item.job);
        if (status === STATUS_SUCCESS) {
          jobQueue.delete(item.job);
          const result = await enrichDecisions(item);
          const parts = await enrichPartDecisions(item);
          console.log(`Job ${item.job} for ${item.url} succeeded: ${JSON.stringify({ ...result, parts })}`);
        } else if (status === STATUS_CANCELED || !status) {
          // A failed job isn't dropped here: it can still succeed when a task is retried
          jobQueue.delete(item.job);
          console.warn(`Job ${item.job} for ${item.url} ended with status ${status}, not enriching`);
        } else if (Date.now() - item.queuedAt > JOB_TIMEOUT) {
          jobQueue.delete(item.job);
          console.warn(`Job ${item.job} for ${item.url} still ${status} after ${JOB_TIMEOUT}ms, no longer waiting`);
        }
      } catch (e) {
        // Stays queued, to be retried on the next check
        console.error(`Checking job ${item.job} for ${item.url} failed`, e);
      }
    }
  } finally {
    checkingQueue = false;
  }
}

setInterval(checkQueue, JOB_POLL_INTERVAL);

app.post("/", async function (req, res) {
  const url = (req.query.url || req.body?.url || '').trim();
  if (!isValidUrl(url)) {
    return res.status(400).send({ error: 'A valid http(s) "url" parameter is required' });
  }

  let quads;
  try {
    quads = withoutSkippedSubjects(await fetchRdfaQuads(url));
  } catch (e) {
    console.error(e);
    return res.status(502).send({ error: `Could not fetch or parse ${url}: ${e.message}` });
  }

  try {
    const parts = partDecisions(quads).map((p) => p.decision);
    await resetEnrichment(harvestedDecisions(quads));
    await resetEnrichment(parts, PART_ENRICHED_PROPERTIES);
    await insertQuads(quads);
    const governingBodyLinks = await deriveGoverningBodyAbstract();
    const municipalities = await findMunicipalities(pageSessions(quads));
    const owningBodyLinks = await linkOwningBody(quads, municipalities);
    const sourceLinks = await linkSources(quads, url);
    // All documents, including those of part decisions
    const removedResources = await removePreviousProcessing(referencedDocuments(quads));
    const job = await createPdfToEnrichedJob(url, municipalities);
    enqueueJob({ ...enrichmentItem(url, quads, municipalities), job: job.job });
    res.status(201).send({ url, graph: TARGET_GRAPH, triples: quads.length, governingBodyLinks, municipalities, owningBodyLinks, sourceLinks, partDecisions: parts, removedResources, ...job });
  } catch (e) {
    console.error(e);
    res.status(500).send({ error: e.message });
  }
});

// Starts checking the latest job for the URL again, e.g. after a restart emptied the queue. The
// page is fetched again for its decisions, part decisions and documents, and to link its agenda
// items and decisions to it as their source. When the job succeeded already, the decisions are
// enriched right away.
app.post("/enrich", async function (req, res) {
  const url = (req.query.url || req.body?.url || '').trim();
  if (!isValidUrl(url)) {
    return res.status(400).send({ error: 'A valid http(s) "url" parameter is required' });
  }

  let quads;
  try {
    quads = withoutSkippedSubjects(await fetchRdfaQuads(url));
  } catch (e) {
    console.error(e);
    return res.status(502).send({ error: `Could not fetch or parse ${url}: ${e.message}` });
  }

  try {
    await linkSources(quads, url);
    const latest = await findLatestJob(url);
    if (!latest) {
      return res.status(404).send({ error: `No ${JOB_OPERATION} job found for ${url}` });
    }
    const municipalities = await findMunicipalities(pageSessions(quads));
    const item = { ...enrichmentItem(url, quads, municipalities), job: latest.job };
    if (latest.status === STATUS_SUCCESS) {
      jobQueue.delete(item.job);
      const result = await enrichDecisions(item);
      const parts = await enrichPartDecisions(item);
      return res.send({ url, job: item.job, status: latest.status, ...result, parts });
    }
    if (latest.status === STATUS_CANCELED) {
      return res.status(409).send({ url, job: item.job, status: latest.status, error: 'The job was canceled' });
    }
    enqueueJob(item);
    res.status(202).send({ url, job: item.job, status: latest.status, queued: true });
  } catch (e) {
    console.error(e);
    res.status(500).send({ error: e.message });
  }
});

// Backfills ext:governingBodyAbstract for sessions harvested before it was derived
app.post("/governing-body-abstract", async function (req, res) {
  try {
    const governingBodyLinks = await deriveGoverningBodyAbstract();
    res.send({ graph: TARGET_GRAPH, governingBodyLinks });
  } catch (e) {
    console.error(e);
    res.status(500).send({ error: e.message });
  }
});

app.use(errorHandler);
