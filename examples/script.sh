\rm cm.db

p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="python ${CM_TOOLS_DIR}/bin.src/cm.py"

${command} --action create 
${command} --action insert --level 0 --production_name ${p_name} --handler ${handler} --config_yaml ${config}
${command} --action insert --recurse --level 1 --production_name ${p_name} --campaign_name ${c_name} --handler ${handler} --config_yaml ${config}
${command} --action prepare --recurse --level 2 --production_name ${p_name} --campaign_name ${c_name}

${command} --action print_table --level 0
${command} --action print_table --level 1
${command} --action print_table --level 2
${command} --action print_table --level 3
${command} --action print_table --level 4
