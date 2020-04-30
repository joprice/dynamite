# Assumes that `stage` has already been run in sbt
run:
	./target/universal/stage/bin/dynamite

run-local-endpoint:
	./target/universal/stage/bin/dynamite --endpoint http://localhost:8080

run-local:
	./target/universal/stage/bin/dynamite --local

run-help:
	./target/universal/stage/bin/dynamite --help
