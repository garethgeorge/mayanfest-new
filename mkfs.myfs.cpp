#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <mutex>
#include <limits.h>
#include <memory>
#include <unistd.h>
#include <libgen.h>
#include <math.h>
#include <iostream>

#include "filesystem.hpp"

const int USER_OPT_COUNT = 2;

int main(int argc, char *argv[]) {
	if (argc - 1 != USER_OPT_COUNT) {
		fprintf(stdout, "Expected argument: <backing file> <file size in bytes>\n");
		return 1;
	}

	const char *backing_file_path = argv[1];
	const unsigned long long file_size_in_bytes = strtol(argv[2], NULL, 10);

	const size_t CHUNK_SIZE = 4096;
	const size_t CHUNK_COUNT = file_size_in_bytes / CHUNK_SIZE;

	fprintf(stdout, "initializing the disk.\n");

	std::unique_ptr<FileSystem> fs = nullptr;
	std::unique_ptr<Disk> disk = nullptr;
	SuperBlock *superblock = nullptr;

	int fh = open(backing_file_path, O_RDWR | O_CREAT | S_IRUSR | S_IWUSR);
	truncate(backing_file_path, file_size_in_bytes);
	if (fh == -1) {
		fprintf(stdout, "failed to get a handle on the requestd file: %s\n", backing_file_path);
		return 1;
	}
	
	fprintf(stdout, "disk size in chunks is %d, chunk size %d, total size %llu\n", CHUNK_COUNT, CHUNK_SIZE, CHUNK_COUNT * CHUNK_SIZE);
	disk = std::unique_ptr<Disk>(new Disk(CHUNK_COUNT, CHUNK_SIZE, MAP_FILE | MAP_SHARED, fh));
	fs = std::unique_ptr<FileSystem>(new FileSystem(disk.get()));
	fs->superblock->init(0.1);
	superblock = fs->superblock.get();

	fs = nullptr;
	disk = nullptr;

	fprintf(stdout, "disk successfully initialized");
}
