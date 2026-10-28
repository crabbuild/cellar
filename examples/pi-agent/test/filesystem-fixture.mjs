// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {fixtureProvider, completed, message} from '../dist/provider.js';

// Only the provider is scripted. Pi performs argument validation, sequential
// tool execution and message construction with the production journal adapter.
export function filesystemProvider() {
  const provider = fixtureProvider();
  provider.stream = (model, context) => {
    const replies = context.messages.filter(item => item.role === 'toolResult');
    const names = new Set(replies.map(item => item.toolName));
    const call = (name, args, id = `call-${name}`) => ({type: 'toolCall', name, arguments: args, id});
    let content;
    if (!names.has('writeFile')) {
      content = [call('writeFile', {path: '/workspace.txt', content: 'first contents'})];
    } else if (!names.has('readFile')) {
      content = [call('readFile', {path: '/workspace.txt'}), call('listFiles', {path: '/'}),
        call('searchFiles', {path: '/', pattern: 'first'})];
    } else if (!names.has('editFile')) {
      const original = JSON.parse(replies.find(item => item.toolCallId === 'call-readFile').content[0].text);
      const edit = {path: '/workspace.txt', inode: original.inode, version: original.version,
        offset: '0', delete_bytes: '5', insert: 'other'};
      content = [call('editFile', edit), call('editFile', {...edit, insert: 'wrong'}, 'call-stale-edit'),
        call('renameFile', {path: '/workspace.txt', destination: '/result.txt'})];
    } else if (!names.has('deleteFile')) {
      content = [call('readFile', {path: '/result.txt'}, 'call-read-renamed'), call('deleteFile', {path: '/result.txt'})];
    } else {
      content = [{type: 'text', text: 'Read, listed, searched, edited, renamed and deleted the cell file. The stale edit was refused.'}];
    }
    return completed(message(model, content, names.has('deleteFile') ? 'stop' : 'toolUse'));
  };
  return provider;
}
