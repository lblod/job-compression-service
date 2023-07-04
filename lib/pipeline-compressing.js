import { gzip } from './gzip';
import {
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
} from '../constants';

import { updateTaskStatus, appendTaskError } from './task';


export async function run(task) {
  try {
    await updateTaskStatus(task, STATUS_BUSY);
    await gzip(task);
    await updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch (e) {
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}


