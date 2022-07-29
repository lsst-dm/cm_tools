p_name="example"
c_name="test"
command="cm"

${command} fake_run --level campaign --production_name ${p_name} --campaign_name ${c_name}
${command} print_table --table campaign
${command} print_table --table step
${command} print_table --table group
${command} print_table --table workflow
