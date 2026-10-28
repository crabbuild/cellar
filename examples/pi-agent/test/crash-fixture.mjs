// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Only the process qualification loads this entry; production app.ts has no fault controls.
import {appendFile, writeFile} from 'node:fs/promises';
import {application} from '../dist/application.js';
import {configuredProvider, fixtureProvider} from '../dist/provider.js';
import {filesystemProvider} from './filesystem-fixture.mjs';
const provider = process.env.CELLAR_PI_TEST_LIVE === '1' ? configuredProvider()
  : process.env.CELLAR_PI_FILESYSTEM === '1' ? filesystemProvider() : fixtureProvider();
const stream = provider.stream;
provider.stream = async (...args) => {
  // Freeze before provider dispatch when qualifying the acceptance boundary;
  // alarms may start while the submitting HTTP response is still outstanding.
  if (process.env.CELLAR_PI_PAUSE_AT === 'accepted') await new Promise(() => {});
  await appendFile(process.env.CELLAR_PI_CALLS, 'model\n');
  if (process.env.CELLAR_PI_PAUSE_AT === 'provider') {
    await writeFile(process.env.CELLAR_PI_MARKER, 'provider');
    await new Promise(() => {});
  }
  return stream(...args);
};
let modelCheckpoints = 0;
let toolCheckpoints = 0;
export default application(provider, async point => {
  if (point === 'completed') await appendFile(process.env.CELLAR_PI_COMPLETIONS, 'completed\n');
  if (point === 'model') modelCheckpoints++;
  if (point === 'tool') toolCheckpoints++;
  if (point === process.env.CELLAR_PI_PAUSE_AT
    || (point === 'model' && modelCheckpoints === 2 && process.env.CELLAR_PI_PAUSE_AT === 'final-model')
    || (point === 'tool' && ((toolCheckpoints === 1 && process.env.CELLAR_PI_PAUSE_AT === 'filesystem')
      || (toolCheckpoints === 5 && process.env.CELLAR_PI_PAUSE_AT === 'filesystem-read')))) {
    await writeFile(process.env.CELLAR_PI_MARKER, point);
    await new Promise(() => {});
  }
});
