#mvn clean package
hadoop fs -rm -f -r /user/root/output/out4

hadoop fs -rm -f -r /user/root/data/temp{0..20}

hadoop fs -rm -f -r /user/root/data/ntemp{0..20}
