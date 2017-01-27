#!/bin/bash -e

echo "Running tests"
run_with_version()
{
    v=$1
    folder=$2
    results="testing.v$v.logs"
    
    for json in $(echo $folder/*json)
    do
	echo $json
	if [[ -f "${json%.json}.py" ]]
	then	    
	    python$v client.py -r submit -a auth.info -j $json -s ${json%.json}.py | tee -a $results
	else
	    python$v client.py -r submit -a auth.info -j $json                     | tee -a $results
	fi
    done

    grep -i "Job_id" $results
}


python3 setup.py install

# Check if we are on bastion
# We can do submission tests only from bastion, therefore skip the submit/cancel/status tests unless you are on bastion

if [[ "$(curl -m 2 -s http://169.254.169.254/latest/meta-data/public-hostname)" == "ec2-52-86-208-63.compute-1.amazonaws.com" ]]
then
    echo "On bastion"
    run_with_version 2 ./samples/pass/
    run_with_version 3
else    
    echo "Cannot run any remote tests right now from non-bastion nodes"
    echo "Done"
fi
