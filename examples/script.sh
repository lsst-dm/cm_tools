\rm cm.db

p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="${CM_TOOLS_DIR}/bin.src/cm"

${command} create
${command} insert --level production --production_name ${p_name} --handler ${handler} --config_yaml ${config}
${command} insert --recurse --level campaign --production_name ${p_name} --campaign_name ${c_name} --handler ${handler} --config_yaml ${config}

${command} prepare --recurse --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} check --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} queue --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} launch --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} fake_run --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} check --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} accept --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} check --level group --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} accept --level group --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} check --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step1
${command} accept --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step1

${command} prepare --recurse --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} check --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} queue --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} launch --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} fake_run --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} check --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} accept --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} check --level group --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} accept --level group --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} check --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step2
${command} accept --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step2

${command} prepare --recurse --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} check --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} queue --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} launch --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} fake_run --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} check --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} accept --level workflow --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} check --level group --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} accept --level group --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} check --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step3
${command} accept --level step --production_name ${p_name} --campaign_name ${c_name} --step_name step3

${command} check --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} accept --level campaign --production_name ${p_name} --campaign_name ${c_name}

${command} print_table --level production
${command} print_table --level campaign
${command} print_table --level step
${command} print_table --level group
${command} print_table --level workflow
