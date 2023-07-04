import { app, errorHandler } from 'mu';
import { Delta } from "./lib/delta";
import {
  STATUS_SCHEDULED,
  TASK_COMPRESSING
} from './constants';
import { run } from './lib/pipeline-compressing';
import { isTask, loadTask } from './lib/task';
import bodyParser from 'body-parser';

app.use(bodyParser.json({
  type: function(req) {
    return /^application\/json/.test(req.get('content-type'));
  }
}));

app.get('/', function(_, res) {
  res.send('Hello harvesting-compressing');
});

app.post('/delta', async function(req, res, next) {
  try {
    const entries = new Delta(req.body).getInsertsFor('http://www.w3.org/ns/adms#status', STATUS_SCHEDULED);
    if (!entries.length) {
      console.log('Delta dit not contain potential tasks that are interesting, awaiting the next batch!');
      return res.status(204).send();
    }

    for (let entry of entries) {
      if (! await isTask(entry)) continue;
      const task = await loadTask(entry);

      if (isCompressingTask(task)) {
        await run(task);
      }
    }

    return res.status(200).send().end();

  } catch (e) {
    console.log(`Something unexpected went wrong while handling delta task!`);
    console.error(e);
    return next(e);
  }
});

function isCompressingTask(task) {
  return task.operation == TASK_COMPRESSING;
}

app.use(errorHandler);
