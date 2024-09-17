#!/usr/bin/env python3
"""
This script rsyncs repos created with "koji dist-repo" and combines them with
other external repos (such as the htcondor repo), then updates the repo
definition files.  The list of repositories is pulled from distrepos.conf;
see the comments in that file for the format.

The mash-created repo layout looks like
    source/SRPMS/{*.src.rpm,repodata/,repoview/}
    x86_64/{*.rpm,repodata/,repoview/}
    x86_64/debug/{*-{debuginfo,debugsource}*.rpm,repodata/,repoview/}

The distrepo layout looks like (where <X> is the first letter of the package name)
    src/repodata/
    src/pkglist
    src/Packages/<X>/*.src.rpm
    x86_64/repodata/
    x86_64/pkglist
    x86_64/debug/pkglist
    x86_64/debug/repodata/
    x86_64/Packages/<X>/{*.rpm, *-{debuginfo,debugsource}*.rpm}

Note that the debuginfo and debugsource rpm files are mixed in with the regular files.
The "pkglist" files are text files listing the relative paths to the packages in the
repo -- this is passed to `createrepo` to put the debuginfo and debugsource RPMs into
separate repositories even though the files are mixed together.
"""

import logging
import sys
import typing as t
from configparser import ConfigParser  # TODO We shouldn't need these
from configparser import ExtendedInterpolation

from distrepos.error import ERR_EMPTY, ERR_FAILURES, ProgramError
from distrepos.params import Options, Tag, ActionType, format_tag, format_mirror, get_args, parse_config
from distrepos.tag_run import run_one_tag
from distrepos.mirror_run import update_mirrors_for_tag
from distrepos.util import acquire_lock, check_rsync, log_ml, release_lock

from datetime import datetime
from pathlib import Path

_log = logging.getLogger(__name__)
_log.addHandler(logging.StreamHandler(sys.stdout))


#
# Functions for dealing with the mirror list
#


def create_mirrorlists(options: Options, tags: t.Sequence[Tag]) -> int:
    """
    Create the files used for mirror lists

    Args:
        options: The global options for the run
        tags: The list of tags to create mirror lists for

    Returns:
        0 if all mirror creations were successful, nonzero otherwise 
    """
    # Set up the lock file
    lock_fh = None
    lock_path = ""
    if options.lock_dir:
        lock_path = options.lock_dir / "mirrors"
        lock_fh = acquire_lock(lock_path)
        if not lock_fh:
            _log.error(f"Could not lock {lock_path}")
            return ERR_FAILURES

    # Generate mirrors for each tag defined in the config file.  Tags are run in series.
    # Keep track of successes and failures.
    successful : t.List[Tag] = []
    failed : t.List[t.Tuple[Tag, str]] = []
    try:
        for tag in tags:
            ok, err = update_mirrors_for_tag(options, tag)
            if ok:
                _log.info(f"Mirrors generated for tag {tag.name}")
                successful.append(tag)
            else:
                _log.error(f"Mirrors failed to generate for for tag {tag.name}: {err}")
                failed.append((tag, err))
    except Exception as e:
        _log.error(f"Unexpected error while processing mirrors: {e}")
        return ERR_FAILURES
    finally:
        release_lock(lock_fh, lock_path)

    # Report on the results
    successful_names = [it.name for it in successful]
    if successful:
        log_ml(
            logging.INFO,
            "%d tag mirror lists generated successfully:\n  %s",
            len(successful_names),
            "\n  ".join(successful_names),
        )
    if failed:
        _log.error("%d tag mirror lists failed:", len(failed))
        for tag, err in failed:
            _log.error("  %-40s: %s", tag.name, err)
        return ERR_FAILURES
    elif not successful:
        _log.error("No tags mirror lists were generated")
        return ERR_EMPTY

    return 0


def rsync_repos(options: Options, tags: t.Sequence[Tag]) -> int:
    """
    Sync repo directories from their build source on Koji to the repo host

    Args:
        options: The global options for the run
        tags: The list of tags to rsync from Koji

    Returns:
        0 if all rsyncs were successful, nonzero otherwise 
    """
    # First check that koji hub is even reachable.  If not, there is no point
    # in proceeding further.
    _log.info("Program started")
    check_rsync(options.koji_rsync)
    _log.info("rsync check successful. Starting run for %d tags", len(tags))

    # Run each tag defined in the config file.  Tags are run in series.
    # Keep track of successes and failures.
    successful = []
    failed = []
    for tag in tags:
        _log.info("----------------------------------------")
        _log.info("Starting tag %s", tag.name)
        log_ml(
            logging.DEBUG,
            "%s",
            format_tag(
                tag,
                koji_rsync=options.koji_rsync,
                condor_rsync=options.condor_rsync,
                destroot=options.dest_root,
            ),
        )
        ok, err = run_one_tag(options, tag)
        if ok:
            _log.info("Tag %s completed", tag.name)
            successful.append(tag)
        else:
            _log.error("Tag %s failed", tag.name)
            failed.append((tag, err))

    _log.info("----------------------------------------")
    _log.info("Run completed")

    # Report on the results
    successful_names = [it.name for it in successful]
    if successful:
        log_ml(
            logging.INFO,
            "%d tags succeeded:\n  %s",
            len(successful_names),
            "\n  ".join(successful_names),
        )
    if failed:
        _log.error("%d tags failed:", len(failed))
        for tag, err in failed:
            _log.error("  %-40s: %s", tag.name, err)
        return ERR_FAILURES
    elif not successful:
        _log.error("No tags were pulled")
        return ERR_EMPTY

    return 0

def update_repo_timestamp(options: Options):
    """
    At the completion of a successful repo sync, update the time listed in the top-level
    timestamp.txt file
    """
    timestamp_txt = Path(options.dest_root) / 'osg' / 'timestamp.txt'
    timestamp_txt.parent.mkdir(parents=True, exist_ok=True)
    with open(timestamp_txt, 'w') as f:
        f.write(datetime.now().strftime("%a %d %b %Y %H:%M:%S %Z"))

#
# Main function
#
def main(argv: t.Optional[t.List[str]] = None) -> int:
    """
    Main function.   Call the functions to parse arguments and config,
    and set up logging and the parameters for each run.
    If --print-tags is specified, only print the tag definitions that were
    parsed from the config file; otherwise, do the run.

    Return the exit code of the program.  Success (0) is if at least one tag succeeded
    and no tags failed.
    """
    args = get_args(argv or sys.argv)
    config_path: str = args.config
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(config_path)

    options, taglist = parse_config(args, config)

    if args.print_tags:
        for tag in taglist:
            print(
                format_tag(
                    tag,
                    koji_rsync=options.koji_rsync,
                    condor_rsync=options.condor_rsync,
                    destroot=options.dest_root,
                )
            )
            print("------")
    if args.print_mirrors:
        for tag in taglist:
            print(
                format_mirror(
                    tag,
                    mirror_root=options.mirror_root,
                    mirror_hosts=options.mirror_hosts
                )
            )
            print("------")
    if args.print_tags or args.print_mirrors:
        return 0

    
    result = 0
    if ActionType.RSYNC in args.action:
        result = rsync_repos(options, taglist)

    if ActionType.MIRROR in args.action and not result:
        result = create_mirrorlists(options, taglist)


    # If all actions were successful, update the repo timestamp
    if not result:
        update_repo_timestamp(options)

    return result


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ProgramError as e:
        _log.error("%s", e)
        _log.debug("Traceback follows", exc_info=True)
        sys.exit(e.returncode)
