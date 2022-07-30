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
${command} insert --level production --production_name ${p_name} --db ${db}
${command} insert --level campaign --production_name ${p_name} --campaign_name ${c_name} --handler ${handler} --config_yaml ${config} --prod_base_url ${prod_base_url} --db ${db}
${command} prepare --level campaign --production_name ${p_name} --campaign_name ${c_name} --db ${db}

${command} queue --level campaign --production_name ${p_name} --campaign_name ${c_name} --db ${db}
${command} launch --level campaign --production_name ${p_name} --campaign_name ${c_name} --db ${db}
${command} fake_run --level campaign --production_name ${p_name} --campaign_name ${c_name} --db ${db}

${command} insert --level group --production_name ${p_name} --campaign_name ${c_name} --step_name step1 --group_name extra_group --handler ${group_handler} --config_yaml ${config} --data_query "i == 11" --db ${db}
${command} prepare --level group --production_name ${p_name} --campaign_name ${c_name} --step_name step1 --group_name extra_group --db ${db}
${command} queue --level campaign --production_name ${p_name} --campaign_name ${c_name} --db ${db}
${command} launch --level campaign --production_name ${p_name} --campaign_name ${c_name} --db ${db}
${command} fake_run --level campaign --production_name ${p_name} --campaign_name ${c_name} --db ${db}
${command} accept --level campaign --production_name ${p_name} --campaign_name ${c_name} --db ${db}

${command} print_table --table campaign --db ${db}
${command} print_table --table step --db ${db}
${command} print_table --table group --db ${db}
${command} print_table --table workflow --db ${db}
