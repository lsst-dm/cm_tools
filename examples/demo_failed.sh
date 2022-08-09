test_suffix="failed"
p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"
db_path="cm_${test_suffix}.db"
db="sqlite:///${db_path}"
prod_base_url="archive_${test_suffix}"

\rm ${db_path}
\rm -rf ${prod_base_url}

${command} create --db ${db}
${command} insert --level production --production-name ${p_name} --db ${db}
${command} insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config} --prod-base-url ${prod_base_url} --db ${db}
${command} prepare --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}

${command} queue --level campaign --fullname ${p_name}/${c_name} --db ${db}
${command} launch --level campaign --fullname ${p_name}/${c_name} --db ${db}

${command} fake-run --level group --fullname ${p_name}/${c_name}/step1/group_4 --db ${db} --status failed
${command} fake-run --level campaign --fullname ${p_name}/${c_name} --db ${db}
${command} accept --level campaign --fullname ${p_name}/${c_name} --db ${db}
${command} supersede --level group --fullname ${p_name}/${c_name}/step1/group_4 --db ${db}
${command} accept --level campaign --fullname ${p_name}/${c_name} --db ${db}

${command} print-table --table campaign --db ${db}
${command} print-table --table step --db ${db}
${command} print-table --table group --db ${db}
${command} print-table --table workflow --db ${db}
${command} print-table --table job --db ${db}
