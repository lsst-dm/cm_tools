test_suffix="server"
p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"
db_path="cm_${test_suffix}.db"
db="sqlite:///${db_path}"
prod_base_url="archive_${test_suffix}"

${command} create --db ${db}
${command} insert --level production --production-name ${p_name} --db ${db}
${command} insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config} --db ${db}
${command} daemon --production-name ${p_name} --campaign-name ${c_name} --db ${db} &
