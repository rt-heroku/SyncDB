worker: java $JAVA_OPTS -classpath "/etc:/target/repo/org/postgresql/postgresql/9.4.1211.jre7/postgresql-9.4.1211.jre7.jar:/target/repo/com/heroku/syncdbs/sync-dbs/1.0-SNAPSHOT/sync-dbs-1.0-SNAPSHOT.jar" \
  -Dapp.name="main" \
  -Dapp.pid="$$" \
  -Dapp.repo="$REPO" \
  -Dbasedir="$BASEDIR" \
  com.heroku.syncdbs.Main
