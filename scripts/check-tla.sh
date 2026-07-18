#!/bin/sh
set -eu

case "${SKIP_TLA_CHECKS:-false}" in
	1|true|yes)
		echo "Skipping TLC checks (SKIP_TLA_CHECKS=${SKIP_TLA_CHECKS}); set to false/0 to run them."
		;;
	*)
		exec "${MAKE:-make}" tla
		;;
esac
