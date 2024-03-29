#!/usr/bin/env bash

EXAMPLES="$(dirname -- "$(readlink -f -- "$0";)";)"

db_path="$EXAMPLES/output/cm_insert_step.db"
handler="handler.ExampleHandler"
group_handler="handler.ExampleGroupHandler"
step_handler="handler_extra_step.ExampleExtraStepHandler"
config="example_config.yaml"
p_name="example"
c_name="test"
full_name="${p_name}/${c_name}"

export CM_DB="sqlite:///${db_path}"
export CM_PLUGINS="$EXAMPLES/examples/handlers"
export CM_CONFIGS="$EXAMPLES/examples/configs"
export CM_PROD_URL="$EXAMPLES/output/archive_insert_step"

rm -rf "$db_path" "$CM_PROD_URL"
mkdir -p "$EXAMPLES/output"

cm create

cm parse --config-name test_config --config-yaml ${config}

cm insert --production-name ${p_name}
cm insert --production-name ${p_name} --campaign-name ${c_name} --config-name test_config --config-block campaign --lsst-version dummy

cm queue --fullname ${full_name}
cm launch --fullname ${full_name}
cm fake-run --fullname ${full_name}

cm extend --config-name test_config --config-yaml example_extra_step.yaml
cm insert-step --production-name ${p_name} --campaign-name ${c_name} --config-block extra_step
cm accept --fullname ${full_name}

cm print-table --table campaign
cm print-table --table step
cm print-table --table group
cm print-table --table workflow
