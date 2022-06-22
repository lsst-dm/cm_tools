\rm cm.db
python sqlalch_interface.py --action create 
python sqlalch_interface.py --action insert --production_name zoe
python sqlalch_interface.py --action insert --production_name bob
python sqlalch_interface.py --action insert --production_name jane
python sqlalch_interface.py --action insert --production_name jane --campaign_name red
python sqlalch_interface.py --action insert --production_name jane --campaign_name blue
python sqlalch_interface.py --action insert --production_name zoe --campaign_name green
python sqlalch_interface.py --action insert --production_name zoe --campaign_name teal
python sqlalch_interface.py --action insert --production_name zoe --campaign_name teal --step_name step_0
python sqlalch_interface.py --action insert --production_name zoe --campaign_name teal --step_name step_1
python sqlalch_interface.py --action insert --production_name zoe --campaign_name teal --step_name step_1 --group_name 00000 
python sqlalch_interface.py --action insert --production_name zoe --campaign_name teal --step_name step_1 --group_name 00001
python sqlalch_interface.py --action insert --production_name zoe --campaign_name teal --step_name step_1 --group_name 00001 --workflow

