#!/usr/bin/env bash

EXAMPLES="$(dirname -- "$(readlink -f -- "$0";)";)"

db_path="$EXAMPLES/output/cm_insert_group.db"
handler="handler.ExampleHandler"
group_handler="handler.ExampleGroupHandler"
config="example_config.yaml"
p_name="example"
c_name="test"

export CM_DB="sqlite:///${db_path}"
export CM_PLUGINS="$EXAMPLES/handlers"
export CM_CONFIGS="$EXAMPLES/configs"
export CM_PROD_URL="$EXAMPLES/output/archive_insert_group"

rm -rf "$db_path" "$CM_PROD_URL"
mkdir -p "$EXAMPLES/output"

cm create
cm insert --level production --production-name ${p_name}
cm insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config}
cm prepare --level campaign --production-name ${p_name} --campaign-name ${c_name}

cm queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
cm launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
cm fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}

cm insert --level group --production-name ${p_name} --campaign-name ${c_name} --step-name step1 --group-name extra_group --handler ${group_handler} --config-yaml ${config} --data-query "i == 11"
cm prepare --level group --production-name ${p_name} --campaign-name ${c_name} --step-name step1 --group-name extra_group
cm queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
cm launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
cm fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
cm accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

cm print-table --table campaign
cm print-table --table step
cm print-table --table group
cm print-table --table workflow
