const {
  BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES,
  DIRECT_DATABASE_ENDPOINT,
  PARALLEL_CALLS,
  BATCH_SIZE,
  SLEEP_BETWEEN_BATCHES,
  INGEST_GRAPH,
} = require("./config.js");
const { batchedUpdate } = require("./utils");
const endpoint = BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES
  ? DIRECT_DATABASE_ENDPOINT
  : process.env.MU_SPARQL_ENDPOINT; //Defaults to mu-auth

/**
 * Ingest delta function. return true if it succeeds, false otherwise
 *
 */
async function ingest(lib, delta, triplestoreEndpoint) {
  try {
    console.log(`Using ${triplestoreEndpoint} to insert triples`);
    const { deletes, inserts } = delta;
    const deleteStatements = deletes.map(
      (o) => `${o.subject} ${o.predicate} ${o.object}.`,
    );
    await batchedUpdate(
      lib,
      deleteStatements,
      INGEST_GRAPH,
      SLEEP_BETWEEN_BATCHES,
      BATCH_SIZE,
      {},
      triplestoreEndpoint,
      "DELETE",
    );
    const insertStatements = inserts.map(
      (o) => `${o.subject} ${o.predicate} ${o.object}.`,
    );

    await batchedUpdate(
      lib,
      insertStatements,
      INGEST_GRAPH,
      SLEEP_BETWEEN_BATCHES,
      BATCH_SIZE,
      {},
      triplestoreEndpoint,
      "INSERT",
    );
    return true;
  } catch (e) {
    console.log(
      `Failed to ingest delta message: ${e}, delta: ${JSON.stringify(delta)}`,
    );
    return false;
  }
}

/**
 * Dispatch the fetched information to a target graph.
 * @param { mu, muAuthSudo, fetch } lib - The provided libraries from the host service.
 * @param { termObjectChangeSets: { deletes, inserts } } data - The fetched changes sets, which objects of serialized Terms
 *          [ {
 *              graph: "<http://foo>",
 *              subject: "<http://bar>",
 *              predicate: "<http://baz>",
 *              object: "<http://boom>^^<http://datatype>"
 *            }
 *         ]
 * @return {void} Nothing
 */
async function dispatch(lib, data) {
  const { mu, chunk } = lib;
  const { termObjectChangeSets } = data;

  const chunks = chunk(termObjectChangeSets, PARALLEL_CALLS);

  for (const chunkedArray of chunks) {
    await Promise.all(
      chunkedArray.map(async (delta) => {
        if (BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES) {
          console.warn(`Service configured to skip MU_AUTH!`);
        }
        let succeeded = await ingest(lib, delta, endpoint);
        if (
          !succeeded &&
          !BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES &&
          DIRECT_DATABASE_ENDPOINT
        ) {
          console.log("attempt with a direct call to virtuoso this time...");
          await ingest(lib, delta, DIRECT_DATABASE_ENDPOINT);
        }
      }),
    );
  }
}

module.exports = {
  dispatch,
};
