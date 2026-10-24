import {fixtureModel} from '../dist/provider.js';

// A provider fixture only; ToolLoopAgent owns parsing, validation and execution.
export function toolsModel() {
  const model = fixtureModel();
  const baseline = model.doGenerate;
  model.doGenerate = async params => {
    const result = await baseline(params);
    const replies = params.prompt.filter(message => message.role === 'tool').flatMap(message => message.content);
    const names = new Set(replies.map(part => part.toolName));
    const call = (toolName, input) => ({type: 'tool-call', toolName, toolCallId: `call-${toolName}`, input: JSON.stringify(input)});
    result.content = !names.size ? [
      call('calculate', {operation: 'multiply', a: 24, b: 7}),
      call('lookup', {topic: 'durability'}), call('increment', {amount: 13}),
      call('saveNote', {title: 'Recovery', content: 'Model decisions and local tool receipts are saved atomically.'}),
      call('createTicket', {title: 'Verify recovery', priority: 'normal'}),
    ] : !names.has('readState') ? [call('readState', {})]
      : [{type: 'text', text: '## Saved results\n\n24 × 7 is **168**. The counter is **13**. The note and one ticket are saved.\n\n| Tool | Result |\n| --- | --- |\n| Counter | 13 |\n| Calculation | 168 |'}];
    result.finishReason = {unified: names.has('readState') ? 'stop' : 'tool-calls'};
    return result;
  };
  return model;
}
