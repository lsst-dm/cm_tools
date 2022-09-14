#!/usr/bin/env bash

EXAMPLES="$(dirname -- "$(readlink -f -- "$0";)";)"

db_path="$EXAMPLES/output/cm_insert_step.db"
handler="handler.ExampleHandler"
group_handler="handler.ExampleGroupHandler"
step_handler="handler_extra_step.ExampleExtraStepHandler"
config="example_config.yaml"
p_name="example"
c_name="test"

export CM_DB="sqlite:///${db_path}"
export CM_PLUGINS="$EXAMPLES/handlers"
export CM_CONFIGS="$EXAMPLES/configs"
export CM_PROD_URL="$EXAMPLES/output/archive_insert_step"

rm -rf "$db_path" "$CM_PROD_URL"
mkdir -p "$EXAMPLES/output"

cm create
cm insert --level production --production-name ${p_name}
cm insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config}

cm queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
cm launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
cm fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
cm insert --level step --production-name ${p_name} --campaign-name ${c_name} --step-name extra_step --handler ${step_handler} --config-yaml ${config}
cm accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

# cm queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
# cm launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
# cm fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
# cm accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

cm print-table --table campaign
cm print-table --table step
cm print-table --table group
cm print-table --table workflow
