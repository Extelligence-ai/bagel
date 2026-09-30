# Third-party notices

Bagel's own code is licensed under Apache-2.0; see [LICENSE](LICENSE).
Dependencies and bundled third-party code retain their respective licenses.
The Bagel license does not replace those licenses.

## Vendored Cloudini decoder

`src/pipeline/tasks/cloudini/vendor/cloudini_decoder.py`

Copyright 2025 Davide Faconti.

Licensed under the Apache License, Version 2.0. A copy is included beside the
decoder at `src/pipeline/tasks/cloudini/vendor/LICENSE`.
Upstream project: <https://github.com/facontidavide/cloudini>.
The original copyright and license header remains in the decoder.

## Installed dependencies

Python packages and operating-system packages carry their own license files
and notices. Preserve those files when redistributing images or installers.
In particular, Bagel's service-specific and optional parsers include copyleft
dependencies; the entire installed environment is not uniformly Apache-2.0.

These selected entries reflect the versions in `uv.lock` at the time this
notice was added, and are not a complete dependency inventory:

| Dependency | Locked version | Declared license | Dependency group |
| --- | --- | --- | --- |
| orangebox | 0.4.0 | GPL-3.0 | betaflight |
| pymavlink | 2.4.49 | LGPLv3 | ardupilot |
| asammdf | 7.4.5 | LGPLv3+ | automotive |
| python-can | 4.6.1 | LGPL-3.0-only | automotive |
| paho-mqtt | 2.1.0 | EPL-2.0 OR BSD-3-Clause | iot |

Before redistributing a built image, review the licenses and corresponding
source requirements of its actual installed packages, including base-image
packages. A lockfile or a link to a package repository alone is not a complete
source-delivery process. Update this notice when the relevant dependencies
change. This file does not certify compliance of previously published images.
