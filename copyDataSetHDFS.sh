#!/bin/bash

source_dir="/home/cloudera/Desktop/dataset/"
target_dir="/user/cloudera/input/"

# List all files in the directory and reverse the order
files=($(ls -r "$source_dir"/*.csv))


while true; do
    for file in "${files[@]}"; do
        if [ -f "$file" ]; then
            # Get the filename without the path
            filename=$(basename "$file")

    	    # Copy the file to HDFS
            hadoop fs -copyFromLocal "$file" "${target_dir}${filename}"

	    # Check if the copy was successful
    	    if [ $? -eq 0 ]; then
        	echo "Successfully copied $file to HDFS"
    	    else
        	echo "Error copying $file to HDFS"
    	    fi
	    sleep 20
            
        fi
    done    
done


