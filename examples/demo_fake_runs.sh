p_name="example"
c_name="test"
command="cm"

${command} fake-run --level campaign --production-name ${p_name} --campaign-name ${c_name}
${command} print-table --table campaign
${command} print-table --table step
${command} print-table --table group
${command} print-table --table workflow
