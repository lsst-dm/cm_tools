p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"
db_path="cm_insert_group.db"
db="sqlite:///${db_path}"
prod_base_url="archive_insert_group"

group_handler="lsst.cm.tools.example.handler.ExampleGroupHandler"

\rm ${db_path}
\rm -rf ${prod_base_url}

${command} create --db ${db}
${command} insert --level production --production-name ${p_name} --db ${db}
${command} insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config} --prod-base-url ${prod_base_url} --db ${db}
${command} prepare --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}

${command} queue --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} launch --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}

${command} insert --level group --production-name ${p_name} --campaign-name ${c_name} --step-name step1 --group-name extra_group --handler ${group_handler} --config-yaml ${config} --data-query "i == 11" --db ${db}
${command} prepare --level group --production-name ${p_name} --campaign-name ${c_name} --step-name step1 --group-name extra_group --db ${db}
${command} queue --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} launch --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} accept --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}

${command} print-table --table campaign --db ${db}
${command} print-table --table step --db ${db}
${command} print-table --table group --db ${db}
${command} print-table --table workflow --db ${db}
