#!/bin/sh
set -e

ls -l .
flake8 --config=common/flake8.cfg
uv run -m unittest discover