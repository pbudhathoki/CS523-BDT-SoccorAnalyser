#!/bin/bash

source_dir="/home/cloudera/Desktop/dataset/"
target_dir="/home/cloudera/workspace/BDT-FinalProject/input/"

while true; do
    for file in "$source_dir"/*.csv; do
        if [ -f "$file" ]; then
            cp "$file" "$target_dir"
	    echo "Copied $file to $target_dir"
	    sleep 20
            
        fi
    done    
done
