# Workflow publication contract v1

Animus workflows that publish git state declare a single owner. Runners must
not infer publication from a workflow id such as `coding`, a phase id such as
`code-open-pr`, or the presence of git metadata.

```yaml
workflows:
  - id: coding
    phases: [prepare, implement, check, publish]
    publication:
      schema: animus.workflow-publication.v1
      version: 1
      required: true
      owner:
        kind: phase
        phase_id: publish
      cleanup: after_remote_verified

phases:
  publish:
    mode: command
    command:
      program: ./publish
      parse_json_output: true
      expected_result_kind: animus.publication-receipt.v1
    output_contract:
      kind: animus.publication-receipt.v1
```

`owner.kind` is either `runner` or `phase`. A required publication has exactly
one owner. A phase owner must occur exactly once after sub-workflow expansion,
must have a phase definition, and must declare the receipt output contract.
Command phases must additionally parse JSON and declare the same expected
result kind. Manual phases cannot publish.

`cleanup` is fail-safe:

- `retain` keeps the workspace after success.
- `after_remote_verified` permits cleanup only after the host independently
  observes the receipt commit at the remote ref. Failures retain the workspace
  and recovery ref.

The owner emits `animus.publication-receipt.v1`. The receipt fences the workflow
and qualified subject generations and includes commit/tree identity, remote and
fully qualified ref, independently observed remote SHA, recovery ref, issuer,
issue timestamp, and optional pull-request proof. The observed remote SHA and
pull-request head SHA must equal the published commit SHA.

## Compatibility

- `animus-config-protocol` 0.2.x accepts workflow publication schema v1.
- `animus-workflow-runner-protocol` 0.3.x accepts publication receipt v1 and
  exposes it on full-workflow, phase-snapshot, and single-phase results.
- Older config remains readable as `publication: null`. This disables
  publication. `workflow_publication_migration_diagnostics` returns an explicit
  `workflow.publication_unconfigured` notice; it never enables legacy behavior.
- Runner protocol 0.2 consumers can ignore the additive receipt field, but they
  are incompatible with `publication.required: true` because they cannot verify
  the proof. Hosts must fail closed on that combination.
- Unknown publication schemas or versions fail compilation. Unknown receipt
  schemas, versions, fields, or inconsistent remote proof fail verification.

This contract is additive on the JSON wire but intentionally source-breaking
for Rust struct literals, hence the crate minor-version bumps while both crates
remain pre-1.0.
