p_name="example"
c_name="test"
handler="lsst.cm.tools.example.handler.ExampleHandler"
config="examples/example_config.yaml"
command="cm"
db_path="cm_test_scripts.db"
db="sqlite:///${db_path}"
prod_base_url="archive_test_scripts"

\rm ${db_path}
\rm -rf ${prod_base_url}

${command} create --db ${db}
${command} insert --level production --production-name ${p_name} --db ${db}
${command} insert --level campaign --production-name ${p_name} --campaign-name ${c_name} --handler ${handler} --config-yaml ${config} --prod-base-url ${prod_base_url} --db ${db}
${command} prepare --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} fake-script --level campaign --production-name ${p_name} --campaign-name ${c_name} --script_name prepare --db ${db}


#${command} queue --level campaign --fullname ${p_name}/${c_name} --db ${db}
#${command} launch --level campaign --fullname ${p_name}/${c_name} --db ${db}

#${command} fake-run --level group --fullname ${p_name}/${c_name}/step1/group_4 --db ${db} --status failed
#${command} fake-run --level campaign --fullname ${p_name}/${c_name} --db ${db}
#${command} rollback --level workflow --fullname ${p_name}/${c_name}/step1/group_4/w00 --db ${db} --status ready
#${command} prepare --level workflow --fullname ${p_name}/${c_name}/step1/group_4/w00 --db ${db}

#${command} queue --level campaign --fullname ${p_name}/${c_name} --db ${db}
#${command} launch --level campaign --fullname ${p_name}/${c_name} --db ${db}
#${command} fake-run --level campaign --fullname ${p_name}/${c_name} --db ${db}
#${command} accept --level campaign --fullname ${p_name}/${c_name} --db ${db}

${command} print-table --table campaign --db ${db}
${command} print-table --table step --db ${db}
${command} print-table --table group --db ${db}
${command} print-table --table workflow --db ${db}
${command} print-table --table job --db ${db}
${command} print-table --table script --db ${db}
