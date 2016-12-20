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



run_with_version 2 ./samples/pass/
run_with_version 3

echo "Done"
