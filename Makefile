FORCE: ;

test: FORCE
	@./gradlew test

dist: FORCE
	@./gradlew jar

doom: FORCE
	@./gradlew --stop
