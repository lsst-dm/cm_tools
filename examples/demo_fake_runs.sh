test_suffix="server"
p_name="example"
c_name="test"
command="cm"
db_path="cm_${test_suffix}.db"
db="sqlite:///${db_path}"

${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name} --db ${db}
${command} print-table --table campaign --db ${db}
${command} print-table --table step --db ${db}
${command} print-table --table group --db ${db}
${command} print-table --table workflow --db ${db}
