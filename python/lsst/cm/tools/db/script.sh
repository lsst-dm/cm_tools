\rm cm.db
python sqlalch_interface.py --action create 
python sqlalch_interface.py --action insert --level 0 --production_name zoe --handler lsst.cm.prod.dp0p2.production.DP0p2_Handler --config_yaml dp_02_production.yaml
python sqlalch_interface.py --action insert --recurse --level 1 --production_name zoe --campaign_name fox --handler lsst.cm.prod.dp0p2.production.DP0p2_Handler --config_yaml dp_02_production.yaml
python sqlalch_interface.py --action prepare --recurse --level 2 --production_name zoe --campaign_name fox

python sqlalch_interface.py --action print_table --level 0
python sqlalch_interface.py --action print_table --level 1
python sqlalch_interface.py --action print_table --level 2
python sqlalch_interface.py --action print_table --level 3
python sqlalch_interface.py --action print_table --level 4
