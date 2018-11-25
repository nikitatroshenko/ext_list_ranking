BLOCK_SIZE=256
MEMO_SIZE=204800
MERGE_RANK=8

CFLAGS=-g -DDEFAULT_MEMORY_SIZE=$(MEMO_SIZE) -DDEFAULT_BLOCK_SIZE=$(BLOCK_SIZE) -D_LOCAL_TEST -DDEFAULT_MERGE_RANK=$(MERGE_RANK)  -DONLINE_JUDGE -O2 -static -Wall -Wextra -x c++ --std=c++11

all: executables

test: executables
	./test-case.bash 8
	./test-case.bash 512
	./test-case.bash 262144
	./test-case.bash 524288
	./test-case.bash 33333
	./test-case.bash 1250000
	bash -c 'for i in {1..10}; do ./test-case.bash 1250000 --random; done'

executables: test_gen.out ext_join.out

test_gen.out: test_gen.cpp
	g++ $(CFLAGS) -o test_gen.out test_gen.cpp

ext_join.out: main.cpp
	g++ $(CFLAGS) -o ext_join.out main.cpp

clean:
	rm -f *.out
