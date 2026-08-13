# End-to-end smoke objective

This repository is an intentionally tiny hako smoke fixture. Treat this
document as the complete objective; do not look for an issue tracker or
invent additional work.

The objective is complete exactly when `SMOKE_RESULT.txt` exists and its
entire contents are this single newline-terminated line:

```text
hako end-to-end smoke passed
```

Do not modify or delete any existing file. Do not commit, publish, push,
open a pull request, or contact external services. The engine owns the
checkpoint commit. If the result file is absent or incorrect, plan the
single work unit that creates or corrects it. If it is already exact and
nothing else remains, the whole objective is done.
