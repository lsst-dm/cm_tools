#!/usr/bin/env bash

EXAMPLES="$(dirname -- "$(readlink -f -- "$0";)";)"

db_path="$EXAMPLES/output/cm_rejected.db"
handler="handler.ExampleHandler"
config="example_config.yaml"
p_name="example"
c_name="test"

export CM_DB="sqlite:///${db_path}"
export CM_PLUGINS="$EXAMPLES/handlers"
export CM_CONFIGS="$EXAMPLES/configs"
export CM_PROD_URL="$EXAMPLES/output/archive_rejected"

rm -rf "$db_path" "$CM_PROD_URL"
mkdir -p "$EXAMPLES/output"

cm create
cm insert --level production --production-name ${p_name}
cm insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config}

cm queue --level campaign --fullname ${p_name}/${c_name}
cm launch --level campaign --fullname ${p_name}/${c_name}
cm fake-run --level campaign --fullname ${p_name}/${c_name}

cm reject --level group --fullname ${p_name}/${c_name}/step1/group_4
cm accept --level campaign --fullname ${p_name}/${c_name}
cm supersede --level group --fullname ${p_name}/${c_name}/step1/group_4
cm accept --level campaign --fullname ${p_name}/${c_name}

cm print-table --table campaign
cm print-table --table step
cm print-table --table group
cm print-table --table workflow
