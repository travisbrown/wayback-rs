## Overview

[![Build status](https://img.shields.io/github/workflow/status/travisbrown/wayback-rs/ci.svg)](https://github.com/travisbrown/wayback-rs/actions)
[![Coverage status](https://img.shields.io/codecov/c/github/travisbrown/wayback-rs/main.svg)](https://codecov.io/github/travisbrown/wayback-rs)

This library extracts some of the non-Twitter-specific code for working with
the [Wayback Machine][wayback] out of the ✨[cancel-culture][cancel-culture]✨
project.

## Example usage

This project is primarily intended for use as a library (for example it's a dependency of ✨[cancel-culture][cancel-culture]✨),
but it also provides some simple tools for interacting with the Wayback Archive.

For example, you can use the `wbms` tool to download snapshots that match a given URL query.

```bash
$ cargo build --release --bin wbms
    ...
    Finished release [optimized] target(s) in 0.47s
$ target/release/wbms -vvv --base toad download --query "https://spottedtoad.wordpress.com/*"
08:49:45 [INFO] Resolving 1 items
08:49:45 [INFO] Resolving: https://spottedtoad.wordpress.com/2016/01/25/higenous-hogenous-birth-timings-endogenous/?share=facebook
08:49:48 [WARN] Invalid guess, re-requesting
08:49:50 [INFO] Downloading 6124 items
...
09:15:02 [INFO] Successfully downloaded: 5582
09:15:02 [INFO] Downloaded by invalid hash: 189
09:15:02 [INFO] Skipped: 5246
09:15:02 [INFO] Failed: 353
```

This command does several things. First it queries the Wayback Machine's [CDX server][cdx-server] to get a list of snapshots.
Next it identifies the targets of redirects. At this point the program will have created a `toad` directory in the current path
(named via the `--base` command) that contains one sub-directory (`errors`) and four files:

* `queries.txt`: a list of the queries that you requested
* `originals.csv`: a comma-separated table listing all of the non-redirect snapshots
* `redirects.csv`: the redirect shapshots
* `extras.csv`: the targets of the redirects

Each of the CSV files has the same format:

* URL
* Wayback Machine timestamp (`%Y%m%d%H%M%S`)
* Wayback Machine digest (Base32-encoded SHA-1)
* MIME type
* Length
* HTTP status code

The errors directory will contain a file (`error/results.csv`) that will list any errors that happened during redirect resolution.

The program meanwhile has moved on to downloading all of the snapshots.
If the content for each snapshot matches the digest provided in the CDX results, it will be saved in a new `data`
sub-directory, with the name of the file being the digest (and the extension `.gz`).

In some cases the content won't match the provided digest (for reasons I don't understand, although there seem to be some patterns).
For these snapshots, the file is saved in an `invalid` directory, with the name being the actual digest.

After downloading is complete, there will be two more files in the `errors` directory.
The `errors/invalid.csv` file will list pairs of provided and actual digests for all snapshots where these don't match.
The `errors/items.csv` file will list any other snapshots that couldn't be downloaded (if you've enabled verbose output with e.g. `-vvv`,
more detailed information about these errors will also be printed to stdout during the run).

Now we have local copies of all of the Wayback Machine snapshots for our URL query.
For a quick one-off project (like our example here), it's generally easiest just to search the compressed files directly:

```bash
$ zegrep -i hartog toad/data/*
toad/data/2AURMICRQRQUKA3UKDWBFUWRG65CNDWD.gz:<p>A <a href="/Users/jhartog/Documents/e56e818283081d7f7537b163a0e8f580.pdf">2014 study in India</a> found a similar association between neuroticism and substance dependence...
...
```

But you could also unzip them to make them easier to work with.

The tool has some other features. For example, if you use the `--twitter` flag, it will expect the query to be a comma-separated list of Twitter screen names,
which it will expand into four queries each (for both tweet and profiles pages on both the mobile and non-mobile domains).

## License

This project is licensed under the [Mozilla Public License, version 2.0][mpl-2].
See the LICENSE file for details.

[cancel-culture]: https://github.com/travisbrown/cancel-culture
[cdx-server]: https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/README.md
[mpl-2]: https://www.mozilla.org/en-US/MPL/2.0/
[wayback]: https://web.archive.org