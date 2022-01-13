prepare:
	mkdir -p bin/

build: prepare
	go build -o bin/playback ./simulator/playback/

simulate: build
	bin/playback -dryrun -lean simulator/playback/dal09_blobs_sample.csv