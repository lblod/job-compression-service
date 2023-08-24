import { gzip } from './gzip';
import {
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
} from '../constants';

import { updateTaskStatus, appendTaskError } from './task';

import { updateSudo as update } from '@lblod/mu-auth-sudo';
import { uuid, sparqlEscapeUri, sparqlEscapeString } from 'mu';

export async function run(task) {
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    await gzip(task);
    await appendTaskResultGraph(task, graphContainer, task.inputContainers[0]);

    await updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch (e) {
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

async function appendTaskResultGraph(task, container, graphUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(graphUri)}.

        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr);

}
