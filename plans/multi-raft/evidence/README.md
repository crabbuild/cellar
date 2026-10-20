# Implementation evidence

No runtime tasks have passed yet. Store evidence records and logs here when
implementation begins. Task evidence paths in `tasks.json` are relative to the
package directory, for example `evidence/MR-01.json`.

A record has the following fields. This is a schema illustration, not a passing
record; replace every illustrative value with the actual evidence.

```json
{
  "task_id": "MR-01",
  "source_revision": "full 40-character Git revision",
  "toolchain": "actual compiler and platform",
  "executable_digest": "sha256 of tested executable",
  "environment": "host, store version and effective configuration",
  "limitations": "specific untested conditions, or none within the stated scope",
  "tests": [{"name": "mode_isolation", "result": "passed"}],
  "commands": [{
    "argv": ["exact", "argument", "vector"],
    "exit_code": 0,
    "output": "evidence/MR-01-test-output.txt"
  }]
}
```

Include every required case and both exact manifest commands, plus relevant
regressions. Attach the actual test listing and execution output. Record source
patch, lockfile, image, workload and raw history digests in additional fields when
applicable. Keep failures and ignored-test explanations; do not mark DONE until
required cases pass.

The package checker validates record structure and coverage. It cannot attest
that a claimed external run happened or that a history oracle is correct. Review
the actual logs and release evidence. Do not store secrets in logs or this folder.
