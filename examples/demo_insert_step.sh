p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"
db_path="cm_insert_step.db"
db="sqlite:///${db_path}"
prod_base_url="archive_insert_step"

step_handler="lsst.cm.tools.example.handler_extra_step.ExampleExtraStepHandler"

\rm ${db_path}
\rm -rf ${prod_base_url}

${command} create --db ${db}
${command} insert --level production --production-name ${p_name} --db ${db}
${command} insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config} --prod-base-url ${prod_base_url} --db ${db}
${command} prepare --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}

${command} queue --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} launch --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} insert --level step --production-name ${p_name} --campaign-name ${c_name} --step-name extra_step --handler ${step_handler} --config-yaml ${config} --db ${db}
${command} accept --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}

#${command} prepare --level group --production-name ${p_name} --campaign-name ${c_name} --step-name extra_step
#${command} queue --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
#${command} launch --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
#${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
#${command} accept --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}

${command} print-table --table campaign --db ${db}
${command} print-table --table step --db ${db}
${command} print-table --table group --db ${db}
${command} print-table --table workflow --db ${db}
