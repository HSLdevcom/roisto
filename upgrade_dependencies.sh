#!/bin/sh
# Upgrade dependencies.

for f in requirements/*.in; do
    pip-compile --upgrade "${f}"
done
