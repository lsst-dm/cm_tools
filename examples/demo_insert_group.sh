#!/usr/bin/env bash

EXAMPLES="$(dirname -- "$(readlink -f -- "$0";)";)"

db_path="$EXAMPLES/output/cm_insert_group.db"
handler="handler.ExampleHandler"
group_handler="handler.ExampleGroupHandler"
config="example_config.yaml"
p_name="example"
c_name="test"
full_name="${p_name}/${c_name}"

export CM_DB="sqlite:///${db_path}"
export CM_PLUGINS="$EXAMPLES/examples/handlers"
export CM_CONFIGS="$EXAMPLES/examples/configs"
export CM_PROD_URL="$EXAMPLES/output/archive_insert_group"

rm -rf "$db_path" "$CM_PROD_URL"
mkdir -p "$EXAMPLES/output"

cm create

cm parse --config-name test_config --config-yaml ${config}

cm insert --production-name ${p_name}
cm insert --production-name ${p_name} --campaign-name ${c_name} --config-name test_config --config-block campaign --lsst-version dummy

cm queue --fullname ${full_name}
cm launch --fullname ${full_name}
cm fake-run --fullname ${full_name}

cm insert --production-name ${p_name} --campaign-name ${c_name} --step-name step1 --group-name extra_group --config-name test_config --config-block group --data-query "i == 11"
cm queue --fullname ${full_name}
cm launch --fullname ${full_name}
cm fake-run --fullname ${full_name}
cm accept --fullname ${full_name}

cm print-table --table campaign
cm print-table --table step
cm print-table --table group
cm print-table --table workflow
