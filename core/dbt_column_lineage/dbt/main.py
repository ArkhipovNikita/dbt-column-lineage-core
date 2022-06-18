import argparse
import os
import sys

from dbt import flags
from dbt.config.profile import DEFAULT_PROFILES_DIR, read_user_config
from dbt.main import _add_selection_arguments
from dbt.tracking import do_not_track
from dbt_column_lineage.dbt.tasks.docs import DocsTask
from dbt_column_lineage.dbt.tasks.parse import ParseColumnLineageTask


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    # don't track user
    do_not_track()

    parsed = parse_args(args)

    user_config = read_user_config(flags.PROFILES_DIR)
    # origin files shouldn't be changed
    user_config.write_json = False

    # apply settings
    flags.set_from_args(parsed, user_config)
    parsed.cls.set_log_format()

    task = parsed.cls.from_args(parsed)
    task.run()


def parse_args(args, cls=argparse.ArgumentParser):
    p = cls(
        prog="dbt-column-lineage",
        description="""
            # TODO: description
            """,
        epilog="""
            Specify one of these sub-commands and you can find more help from
            there.
            """,
    )

    p.add_argument(
        "--log-format",
        choices=["text", "json", "default"],
        default=None,
        help="""Specify the log format, overriding the command's default.""",
    )

    p.add_argument(
        "--single-threaded",
        action="store_true",
        help=argparse.SUPPRESS,
    )

    p.add_argument(
        "--profiles-dir",
        default=None,
        dest="profiles_dir",
        type=str,
        help="""
        Which directory to look in for the profiles.yml file. Default = {}
        """.format(
            DEFAULT_PROFILES_DIR
        ),
    )

    subs = p.add_subparsers(title="Available sub-commands")

    base_subparser = _build_base_subparser()

    parse_sub = _build_parse_subparser(subs, base_subparser)
    docs_sub = _build_docs_subparser(subs, base_subparser)

    _add_common_arguments(parse_sub, docs_sub)
    _add_selection_arguments(parse_sub)

    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)

    if getattr(parsed, "project_dir", None) is not None:
        expanded_user = os.path.expanduser(parsed.project_dir)
        parsed.project_dir = os.path.abspath(expanded_user)

    return parsed


def _build_base_subparser():
    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.add_argument(
        "--project-dir",
        default=None,
        type=str,
        help="""
        Which directory to look in for the dbt_project.yml file.
        Default is the current working directory and its parents.
        """,
    )

    base_subparser.add_argument(
        "--profile",
        required=False,
        type=str,
        help="""
        Which profile to load. Overrides setting in dbt_project.yml.
        """,
    )

    base_subparser.add_argument(
        "-t",
        "--target",
        default=None,
        type=str,
        help="""
        Which target to load for the given profile
        """,
    )

    base_subparser.set_defaults(defer=None, state=None)
    return base_subparser


def _build_parse_subparser(subparsers, base_subparser):
    parse_sub = subparsers.add_parser("parse", parents=[base_subparser])
    parse_sub.set_defaults(cls=ParseColumnLineageTask)

    return parse_sub


def _build_docs_subparser(subparsers, base_subparser):
    parse_sub = subparsers.add_parser("docs", parents=[base_subparser])
    parse_sub.set_defaults(cls=DocsTask)

    return parse_sub


def _add_common_arguments(*subparsers):
    for sub in subparsers:
        sub.add_argument(
            "--threads",
            type=int,
            required=False,
            help="""
            Specify number of threads to use while executing models. Overrides
            settings in profiles.yml.
            """,
        )
