fuzz: fuzzer-fuzz.zip
	go run github.com/dvyukov/go-fuzz/go-fuzz
.PHONY: fuzz

fuzzer-fuzz.zip:
	go run github.com/dvyukov/go-fuzz/go-fuzz-build 

clean:
	rm -rf fuzzer-fuzz.zip crashers corpus suppressions
