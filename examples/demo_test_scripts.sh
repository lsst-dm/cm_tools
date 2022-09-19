#!/usr/bin/env bash

EXAMPLES="$(dirname -- "$(readlink -f -- "$0";)";)"
db_path="$EXAMPLES/output/cm_${test_suffix}.db"
handler="handler.ExampleHandler"
config="example_config.yaml"
p_name="example"
c_name="test"
export CM_DB="sqlite:///${db_path}"
export CM_PLUGINS="$EXAMPLES/handlers"
export CM_CONFIGS="$EXAMPLES/configs"
export CM_PROD_URL="$EXAMPLES/output/archive_test_scripts"

rm -rf "$db_path" "$CM_PROD_URL"
mkdir -p "$EXAMPLES/output"

cm create

cm parse --config-name test_config --config-yaml ${config}

cm insert --level production --production-name ${p_name}
cm insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --config-name test_config --config-block campaign
cm fake-script --level campaign --production-name ${p_name} --campaign-name ${c_name} --script-name prepare

cm queue --level campaign --fullname ${p_name}/${c_name}
cm launch --level campaign --fullname ${p_name}/${c_name}

cm fake-run --level group --fullname ${p_name}/${c_name}/step1/group_4 --status failed
cm fake-run --level campaign --fullname ${p_name}/${c_name}
cm rollback --level workflow --fullname ${p_name}/${c_name}/step1/group_4/w00 --status ready

cm queue --level campaign --fullname ${p_name}/${c_name}
cm launch --level campaign --fullname ${p_name}/${c_name}
cm fake-run --level campaign --fullname ${p_name}/${c_name}
cm accept --level campaign --fullname ${p_name}/${c_name}

cm print-table --table campaign
cm print-table --table step
cm print-table --table group
cm print-table --table workflow
cm print-table --table job
cm print-table --table script
