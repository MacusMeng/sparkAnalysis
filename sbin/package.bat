@echo off
echo [INFO] Package the jar in target dir.

cd %~dp0
cd ..

set MAVEN_OPTS=%MAVEN_OPTS% -XX:MaxPermSize=256m
call mvn clean scala:compile compile package -Dmaven.test.skip=true

cd sbin
pause