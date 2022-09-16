#!/usr/bin/env bash

EXAMPLES="$(dirname -- "$(readlink -f -- "$0";)";)"

db_path="$EXAMPLES/output/cm.db"
handler="handler.ExampleHandler"
config="example_config.yaml"
p_name="example"
c_name="test"

export CM_DB="sqlite:///${db_path}"
export CM_PLUGINS="$EXAMPLES/handlers"
export CM_CONFIGS="$EXAMPLES/configs"
export CM_PROD_URL="$EXAMPLES/output/archive"

rm -rf "$db_path" "$CM_PROD_URL"
mkdir -p "$EXAMPLES/output"

cm create

cm parse --config-name test_config --config-yaml ${CM_CONFIGS}/${config}

cm insert --level production --production-name ${p_name}
cm insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --config-name test_config --config-block campaign

#cm queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

#cm queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

#cm queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
#cm accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

cm print-table --table campaign
cm print-table --table step
cm print-table --table group
cm print-table --table workflow
cm print-table --table job
