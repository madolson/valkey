# Valkey development guidelines
This document provides a general overview for writing and designing code for Valkey.
During our long development history, we've made a lot of inconsistent decisions, but we strive to get incrementally better.

## General Best practices
1. Try to limit the number of lines changed in a PR when possible.
1. Avoid adding configuration when a feature can be fully controlled by heuristics.
   We want Valkey to work correctly out of the box without much tuning.
   Configurations can be added to provide additional tuning of features.
   When the workload characteristics can't be inferred or imply a tradeoff (CPU vs memory), then provide a configuration.
1. Search for an existing issue or PR before opening one.
   Duplicate PRs for the same fix are common, and we will generally keep the one from whoever reported or diagnosed the problem and close the rest.
1. Explain the problem, not just the change.
   For a bug fix, describe how to reach the bug and which released versions are affected, since that determines what we backport to.
   For a new command, option or config, describe the use case that isn't served today.
   This is the first thing a reviewer will ask for.
1. Leave pre-existing bugs out of the PR.
   If you notice an unrelated problem while working on something, open an issue for it instead of widening the change.

## General style guidelines
Most of the style guidelines are enforced by clang format, but some additional comments are included here.

1. C style comments `/* comment */` can be used for both single and multi-line comments.
   C++ comments `//` can only be used for single line comments.
   Multi line comments should have the leading `*` align and the final `*/` should be on the same line as the last line of text.
   e.g.
```c
/* Blah Blah
 * Blah Blah. */
   ```
2. Comments should generally be used to describe behavior that is not obvious from reading the code itself.
   This includes complex behavior, why code was written the way it was and describing non-obvious behavior.
   Additionally, functions should be documented to explain all of the function's behavior without having to read the code.
1. Generally keep line lengths below 90 characters when reasonable, however there is no explicit line length enforcement.
   Use your best judgement for readability.
1. Use static functions when a function is only intended to be accessed from the same file.
   For historical reasons, some private functions are prefixed by `_`, and they are kept as is to make it easier to backport changes.
1. Use the boolean type for true/false values.
   For historical reasons, some functions used the integer type, and they are kept as is to make it easier to backport changes.
1. Don't add code that has no callers.
   Unused helpers, unused mock wrappers and metrics that nothing reads should be added in the change that needs them, not ahead of it.
1. Be careful about adding includes to headers that are included nearly everywhere.
   `src/unit/wrappers.h`, for example, is included by every gtest translation unit, so a feature header added there ends up in the whole unit test build.

## Naming conventions
Valkey has a long history of inconsistent naming conventions.
Generally follow the style of the surrounding code, but you can also always use the following conventions for variable and structure names:

- Variable names: `snake_case` or all lower case for short names  (e.g. `cached_reply` or `keylen`).
- Function names: `camelCase` or `namespace_camelCase` (e.g. `createStringObject` or `IOJobQueue_isFull`).
- Macros: `UPPER_CASE` (e.g. `MAKE_CMD`).
- Structures: `camelCase` (e.g. `user`).

## User visible surface
The implementation of a feature can be changed later, but the surface it exposes is effectively permanent.
Every change to the user visible surface is a [technical major decision](GOVERNANCE.md#technical-major-decisions) and requires a TSC vote.
Raise it in an issue.

List the full surface in the PR description: every new config, `INFO` field, `CLIENT LIST` flag, error reply, and behavior change under an existing config.

Conventions for the surface itself:

1. Configuration names are lower case and dash separated.
   Name a config after what it controls rather than the goal it serves, and don't invent terminology that doesn't appear elsewhere in the codebase.
   Boolean configs are usually just the noun, without an `-enabled` suffix.
1. Prefer not to change the meaning of an existing config.
   Weakening an existing limit as a side effect of a new feature is a surface change even when the new feature is off by default.
1. `INFO` field names are all lower case and are not prefixed with the name of their section.
   Never derive an `INFO` field name from an internal identifier such as a C symbol or struct name.
   Don't add a field that is trivially derivable from another field.
1. `INFO` section names are nouns, matching `# Replication` and `# Persistence`.
1. New commands and options need a matching entry in `src/commands/*.json`, including `since` and a `reply_schema`.
   The reply schema is validated in CI and used by the test suite when running under RESP3.

## Common pitfalls
These come up repeatedly in review.

1. Use `serverassert.h` instead of `<assert.h>` in server code.
   It prints the stack trace and server state into the log instead of dying with `SIGABRT`.
   The command line tools (`valkey-cli`, `valkey-benchmark`) are the exception and use libc `assert`.
1. `c->flag.replica` is also set for `MONITOR` clients.
   Use `getClientType(c) == CLIENT_TYPE_REPLICA` when you mean an actual replica.
1. Validate untrusted payloads when they are loaded, not when they are read.
   `RESTORE` and RDB loading already deep validate, so new structural checks belong there.
   Validating on the access path costs every read forever and surfaces the corruption long after it was accepted.
1. Commands whose result depends on server state must propagate deterministically.
   A replica or an AOF replay must not re-evaluate randomness, the clock or the current dataset.
   Rewrite into an explicit equivalent, the way `EXPIRE` propagates as `PEXPIREAT` and `SPOP` propagates as `SREM`.
1. Keep `valkey.conf` in sync with the behavior.
   Its comments are user documentation, and they are easy to leave describing the old behavior or a command spelling that no longer exists.
1. Assertions and defensive checks for states that can't be reached are usually not worth adding.
   If an invariant isn't obvious at the call site, a comment explaining why it holds is better than a check that can never fire.
1. Check what a hot path costs before adding to it.
   Extra validation, a metric or an allocation on a command path needs a measurement, not an argument that it should be cheap.

## Licensing information
When creating new source code files, use the following snippet to indicate the license:
```
/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */
```

If you are making material changes to a file that has a different license at the top, also add the above license snippet.
There isn't a well defined test for what is considered a material change, but a good rule of thumb is that material changes are more than 100 lines of code.

## Test coverage
Valkey uses two types of tests: unit and integration tests.
All contributions should include a test of some form.

Unit tests are present in the `src/unit` directory, and are intended to test individual structures or files.
For example, most changes to data structures should include corresponding unit tests.

Integration tests are located in the `tests/` directory, and are intended to test end-to-end functionality.
Adding new commands should come with corresponding integration tests.
When writing cluster mode tests, do not use the legacy `tests/cluster` framework, which has been deprecated, and instead write tests in `unit/cluster`.

## Documentation
Valkey keeps most of the user documentation in the [valkey-doc](https://github.com/valkey-io/valkey-doc) repository in a few areas:
1. Major functionality is documented in the [topics](https://github.com/valkey-io/valkey-doc/tree/main/topics) section.
1. Specific command behavior is documented in the [commands](https://github.com/valkey-io/valkey-doc/tree/main/commands) section.
   Command history is also documented in the [command json file](https://github.com/valkey-io/valkey/tree/unstable/src/commands).
1. Server info fields are documented in the [INFO](https://github.com/valkey-io/valkey-doc/blob/main/commands/info.md) command.

When a PR is opened that requires documentation to be updated, the `needs-doc-pr` should be added until the corresponding documentation PR is open.

## Backporting
Bug fixes are backported to the release branches that contain the bug, so state which versions are affected in the original PR.

1. Try to isolate a bug fix so that it can be backported.
   We do a lot of backporting as a project, and the more lines changed, the higher the chance of having to resolve merge conflicts.
   Please separate refactoring and functional changes into separate PRs, to make it easier to handle backporting.
1. Land the fix on `unstable` first, then open one PR per affected release branch and add each one to the project for that release, e.g. [Valkey 8.1](https://github.com/orgs/valkey-io/projects/14).
