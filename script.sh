#!/bin/bash

# prompt user
echo 'Ready to clean the data? [1/0]' 
read cleancontinue

#if user picks 1, run script
if [ $cleancontinue -eq 1 ]
then 
    echo "Cleaning Data"
    python dev/cleanse_data.py
    echo "Done cleaning data"

    # get first line of dev and prod changelogs
    dev_version=$(head -n 1 dev/changelog.md)
    prod_version=$(head -n 1 prod/changelog.md)

    #delimit 1st line of changelog by space
    read -a splitversion_dev <<< $dev_version
    read -a splitversion_prod <<< $prod_version

    # get version numbers
    dev_version=${splitversion_dev[1]}
    prod_version=${splitversion_prod[1]}

    # if version numbers are different, ask and move dev files to prod
    if [ "$prod_version" != "$dev_version" ]
    then 
        echo 'There are new changes. Would you like to move dev files to prod? [1/0]'
        read scriptcontinue
    else 
        scriptcontinue=0
    fi
else 
    echo 'Come back when you are ready'
fi

# If user selects 1, copy files from dev to prod
if [ $scriptcontinue -eq 1 ]
then 
    for filename in dev/*
    do 
        if [ $filename == "dev/cademycode_cleansed.db" ] || [ $filename == "dev/cademycode_cleansed.csv" ] || [ $filename == "dev/changelog.md" ]
        then
            cp $filename prod
            echo "Copying " $filename
        else    
            echo "Not Copying " $filename
        fi
    done
else
    echo 'Come back when you are ready'
fi
