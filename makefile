CPPCC=g++
CC=g++ 
CPPFLAGS= -std=c++11 -g -O0 -D_FILE_OFFSET_BITS=64
CFLAGS= 

OBJS=src/diskinterface.o src/filesystem.o
INCLUDES=-I ./3rdparty/ -I ./src/
TEST_OBJS=tests/test-diskinterface.o tests/test-filesystem.o tests/test-syscall.o

all: test myfs

test: ${TEST_OBJS} ${OBJS} tests/test-main.o
	${CPPCC} ${CPPFLAGS} -o test tests/test-main.o ${TEST_OBJS} ${OBJS} ${INCLUDES}

myfs: ${OBJS} myfs.o
	${CPPCC} ${CPPFLAGS} -o myfs myfs.o ${OBJS} ${INCLUDES} -lfuse

%.o: %.c
	# @echo CC $@
	${CC} -c ${CFLAGS} $< -o $@ ${INCLUDES}

%.o: %.cpp
	# @echo CPPCC $@
	${CPPCC} -c ${CPPFLAGS} $< -o $@ ${INCLUDES}

clean:
	rm -f filesystem
	find . -type f -name '*.o' -delete
