
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: kvstore.o correctness.o skiplist.o bloomfilter.o

persistence: kvstore.o persistence.o skiplist.o bloomfilter.o

check: kvstore.o check.o  skiplist.o bloomfilter.o

clean:
	-rm -f correctness persistence *.o
