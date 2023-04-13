#!/usr/bin/env bash

EXAMPLES="$(dirname -- "$(readlink -f -- "$0";)";)"

db_path="$EXAMPLES/output/cm.db"
handler="handler.ExampleHandler"
config="example_config.yaml"

p_name="example"
c_name="test"
full_name="${p_name}/${c_name}"

export CM_DB="sqlite:///${db_path}"
export CM_PLUGINS="$EXAMPLES/examples/handlers"
export CM_CONFIGS="$EXAMPLES/examples/configs"
export CM_PROD_URL="$EXAMPLES/output/archive"
export CM_PROFILE=1

rm -rf "$db_path" "$CM_PROD_URL"
mkdir -p "$EXAMPLES/output"
\rm -rf *.prof

python -m cProfile -s cumulative -o create.prof `which cm` create

python -m cProfile -s cumulative -o parse.prof `which cm` parse --config-name test_config --config-yaml ${config}

python -m cProfile -s cumulative -o insert_prod.prof `which cm` insert --production-name ${p_name}
python -m cProfile -s cumulative -o insert_camp.prof `which cm` insert --production-name ${p_name} --campaign-name ${c_name} --config-name test_config --config-block campaign --lsst-version dummy

python -m cProfile -s cumulative -o queue_1.prof `which cm` queue --fullname ${full_name}
python -m cProfile -s cumulative -o launch_1.prof `which cm` launch --fullname ${full_name}
python -m cProfile -s cumulative -o fake_1.prof `which cm` fake-run --fullname ${full_name}
python -m cProfile -s cumulative -o accept_1.prof `which cm` accept --fullname ${full_name}

python -m cProfile -s cumulative -o queue_2.prof `which cm` queue --fullname ${full_name}
python -m cProfile -s cumulative -o launch_2.prof `which cm` launch --fullname ${full_name}
python -m cProfile -s cumulative -o fake_2.prof `which cm` fake-run --fullname ${full_name}
python -m cProfile -s cumulative -o accept_2.prof `which cm` accept --fullname ${full_name}

python -m cProfile -s cumulative -o queue_3.prof `which cm` queue --fullname ${full_name}
python -m cProfile -s cumulative -o launch_3.prof `which cm` launch --fullname ${full_name}
python -m cProfile -s cumulative -o fake_3.prof `which cm` fake-run --fullname ${full_name}
python -m cProfile -s cumulative -o accept_3.prof `which cm` accept --fullname ${full_name}

python -m cProfile -s cumulative -o print_camp.prof `which cm` print-table --table campaign
python -m cProfile -s cumulative -o print_step.prof `which cm` print-table --table step
python -m cProfile -s cumulative -o print_group.prof `which cm` print-table --table group
python -m cProfile -s cumulative -o print_workflow.prof `which cm` print-table --table workflow
python -m cProfile -s cumulative -o print_job.prof `which cm` print-table --table job
