#!/bin/bash

export COURSEDIR=/home/training/training_materials/devsh
export COURSEDATA=/home/training/training_materials/devsh/data

main() {
    echo -n "[setup.sh] Course setup started at: " && date

    setup_devsh
    setup_db
    setup_hive
    setup_firefox
    
    echo -n "[setup.sh] Course setup completed at: " && date
}

setup_devsh() {

    echo "[setup.sh] Setting up course environment variables" 
    echo export DEVSH=$COURSEDIR >> ~/.bashrc    
    echo export DEVDATA=$COURSEDATA  >> ~/.bashrc
    
}

# No longer needed in new exercise environment, 
# left here in case we need it for a VM at some point
#setup_maven() {

    #echo "[setup.sh] Installing and configuring Maven $MAVEN"
    
    # Configure maven -- set M2 env variable, and add maven to path
    #echo "export M2_HOME=/usr/local/apache-maven/$MAVEN" >> ~/.bashrc
    #echo 'export M2=$M2_HOME/bin' >> ~/.bashrc
    #echo 'export PATH=$M2:$PATH' >> ~/.bashrc
        
    # Create user's Maven home directory
    #mkdir -p /home/training/.m2
    
    # Copy in the pre-cached repository for the course so Maven won't need to download anything
    # Note: To recreate devsh-maven-repository.tgz file:
    # 1 - remove the existing .m2 directory: "rm -rf /home/training/.m2"
    # 2 - build all Scala and Java project examples and solutions: "mvn clean package" 
    # (this will re-download all required libraries, will take a long time)
    # 3 - tar/gzip the .m2/repository directory: "tar czvf devsh-maven-repository.tgz"
    #tar --directory /home/training/.m2/ -xzf /var/tmp/deployments/installers/devsh-maven-repository.tgz
    
    # Copy in settings to turn maven's offline property to true so that only precached libraries are used
    #mv /var/tmp/deployments/maven2-settings.xml /home/training/.m2/settings.xml



setup_db() {
   echo "[setup.sh] Setting up the devsh_loudacre database in MySQL"

   mysql --user=training --password=training -e "DROP DATABASE IF EXISTS devsh_loudacre;"
   mysql --user=training --password=training -e "CREATE DATABASE devsh_loudacre;"
   mysql --user=training --password=training devsh_loudacre < $COURSEDATA/loudacre.sql
}

setup_hive() {
    echo "[setup.sh] Setting up Hive/Impala database for demo/exercises"
    
    # Create DevSH database
    impala-shell -q "CREATE DATABASE IF NOT EXISTS devsh"

    # Load accounts data into default location for Hive tables
    hdfs dfs -put $COURSEDATA/static_data/accounts/ /user/hive/warehouse/devsh.db/

    # Set up the table to reference the data
    impala-shell -f $COURSEDIR/scripts/create-accounts.sql
}

# Copy in bookmarks for firefox
setup_firefox() {
    echo "[setup.sh] Setting up Firefox bookmarks"
    
    firefoxdir=/home/training/.mozilla/firefox/training.default

    if [[ -e $firefoxdir ]];
      then mv $firefoxdir $firefoxdir.save.`date -Iseconds`; 
    fi
    cp -r $COURSEDIR/scripts/firefox-training.default $firefoxdir
}

main