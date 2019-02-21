mkdir -p out/test
find -name "*.java" > sources.txt
javac -d out/test -cp libs/jsqlparser-1.0.0.jar @sources.txt
java -classpath out/test:libs/jsqlparser-1.0.0.jar dubstep.Main < $1
