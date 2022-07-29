\rm cm.db
\rm -rf archive_test

p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"

${command} create
${command} insert --level production --production_name ${p_name}
${command} insert --level campaign --production_name ${p_name} --campaign_name ${c_name} --handler ${handler} --config_yaml ${config}
${command} daemon --production_name ${p_name} --campaign_name ${c_name} &
