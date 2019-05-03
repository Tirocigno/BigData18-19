
#!/bin/bash

sqoop import --connect jdbc:oracle:thin:@137.204.78.85:1521:SISINF --username amordenti --password amordenti --hive-import --hive-database bigdata_group66 --hive-table PPE --split-by zipcode --target-dir /user/lsemprini/bigdata_test2 --query "select * from (select * from amordenti.PPE) where \$CONDITIONS"