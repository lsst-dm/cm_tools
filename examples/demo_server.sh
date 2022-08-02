\rm cm.db
\rm -rf archive_test

p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"

${command} create
${command} insert --level production --production-name ${p_name}
${command} insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config}
${command} daemon --production-name ${p_name} --campaign-name ${c_name} &
