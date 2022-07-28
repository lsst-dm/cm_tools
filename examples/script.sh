\rm cm.db
\rm -rf archive

p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"

${command} create
${command} insert --level production --production_name ${p_name}
${command} insert --level campaign --production_name ${p_name} --campaign_name ${c_name} --handler ${handler} --config_yaml ${config}
${command} prepare --level campaign --production_name ${p_name} --campaign_name ${c_name}

${command} queue --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} launch --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} fake_run --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} accept --level campaign --production_name ${p_name} --campaign_name ${c_name}

${command} queue --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} launch --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} fake_run --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} accept --level campaign --production_name ${p_name} --campaign_name ${c_name}

${command} queue --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} launch --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} fake_run --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} accept --level campaign --production_name ${p_name} --campaign_name ${c_name}

${command} print_table --table campaign
${command} print_table --table step
${command} print_table --table group
${command} print_table --table workflow
