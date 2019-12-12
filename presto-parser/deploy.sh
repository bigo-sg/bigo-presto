mvn deploy:deploy-file -DgroupId=bigo.sg \
    -DartifactId=presto-parser \
    -Dversion=bigo-324 \
    -Dpackaging=jar \
    -Dfile=target/presto-parser-324.jar \
    -DgeneratePom=true \
    -DrepositoryId=releases \
    -Durl=http://maven.bigo.sg:8081/repository/maven-releases/
