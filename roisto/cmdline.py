# -*- coding: utf-8 -*-
"""Command-line parsing."""

import argparse

import yaml

_DEFAULT_CONFIG_FILENAME = 'config.yaml'


def _load_yaml_file(filename):
    """Load the given YAML file and return the corresponding dict."""
    with open(filename, 'rt', encoding='utf-8') as handle:
        return yaml.load(handle)


def _create_parser():
    """Create the parser for command-line parsing."""
    parser = argparse.ArgumentParser(
        description='Poll PubTrans databases and send MONO messages via MQTT.')
    parser.add_argument(
        '-c',
        '--config',
        type=_load_yaml_file,
        default=_DEFAULT_CONFIG_FILENAME,
        metavar='YAML_FILE',
        help='configuration file in YAML and UTF-8 (default: {filename})'.
        format(filename=_DEFAULT_CONFIG_FILENAME))
    return parser


def parse_cmdline():
    """Parse the command-line."""
    parser = _create_parser()
    return parser.parse_args()
