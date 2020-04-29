# Assumes that `stage` has already been run in sbt
run-staged:
	./target/universal/stage/bin/dynamite

run-staged-local:
	./target/universal/stage/bin/dynamite --endpoint http://localhost:8080

run-staged-help:
	./target/universal/stage/bin/dynamite --help
