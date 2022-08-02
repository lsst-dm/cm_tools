\rm cm.db
\rm -rf archive

p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"

${command} create
${command} insert --level production --production-name ${p_name}
${command} insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config}
${command} prepare --level campaign --production-name ${p_name} --campaign-name ${c_name}

${command} queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

${command} queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

${command} queue --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} launch --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} accept --level campaign --production-name ${p_name} --campaign-name ${c_name}

${command} print-table --table campaign
${command} print-table --table step
${command} print-table --table group
${command} print-table --table workflow
