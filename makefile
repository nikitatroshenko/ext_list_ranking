BLOCK_SIZE=256
MEMO_SIZE=204800
MERGE_RANK=8

CFLAGS=-DDEFAULT_MEMORY_SIZE=$(MEMO_SIZE) -DDEFAULT_BLOCK_SIZE=$(BLOCK_SIZE) -D_LOCAL_TEST=1 -DDEFAULT_MERGE_RANK=$(MERGE_RANK)

all: executables

test: executables
	bash -c 'for i in {1..10}; do ./test-case.bash 19999 --random; done'

executables: test_gen.out ext_join.out

test_gen.out: test_gen.cpp
	g++ $(CFLAGS) -o test_gen.out test_gen.cpp

ext_join.out: main.cpp
	g++ -g -DONLINE_JUDGE -O2 -static -Wall -Wextra -x c++ --std=c++14  -o ext_join.out main.cpp

clean:
	rm -f *.out
