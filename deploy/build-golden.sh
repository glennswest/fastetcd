#!/usr/bin/env bash
# What stormcentral runs for a `tree` component: `deploy/build-golden.sh
# <name> OUT`, in the fetched checkout at the commit (stormcentral's README,
# "tree"; `run_recipe` in its goldens.rs). The recipe itself is
# deploy/build-image.sh (#257: the name stormcentral#1836 gives every
# component's own image build); this is that contract's entry point.
exec "$(dirname "$0")/build-image.sh" "$@"
