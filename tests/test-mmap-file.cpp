#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "catch.hpp"

#include "diskinterface.hpp"
#include "filesystem.hpp"

TEST_CASE( "Should be able to construct a filesystem in a memory mapped file", "[mmap]" ) {

	constexpr uint64_t CHUNK_COUNT = 4096;
	constexpr uint64_t CHUNK_SIZE = 4096;
	{
		int fh = open("disk.myanfest", O_RDWR | O_CREAT, 0666);
		truncate("disk.myanfest", CHUNK_COUNT * CHUNK_SIZE);

		std::unique_ptr<Disk> disk(new Disk(CHUNK_COUNT, CHUNK_SIZE, MAP_FILE | MAP_SHARED, fh));
		std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
		fs->superblock->init(0.1);
		fs = nullptr;
		disk = nullptr;
		close(fh);
	}

	{
		fprintf(stdout, "remapping the meory mapped file...\n");
		int fh = open("disk.myanfest", O_RDWR | O_CREAT);
		std::unique_ptr<Disk> disk(new Disk(CHUNK_COUNT, CHUNK_SIZE, MAP_FILE | MAP_SHARED, fh));
		std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
		fs->superblock->load_from_disk();
		fs = nullptr;
		disk = nullptr;
		close(fh);
	}
}

TEST_CASE("INodes can be used to store and read directories on a mmap'd disk", "[mmap]") {
	constexpr uint64_t CHUNK_COUNT = 4096;
	constexpr uint64_t CHUNK_SIZE = 512;

	truncate("disk.myanfest", CHUNK_COUNT * CHUNK_SIZE);
	int fh = open("disk.myanfest", O_RDWR | O_CREAT);

	SECTION("can write a thousand files, each of which contains a single number base 10 encoded") {
		
		std::unique_ptr<Disk> disk(new Disk(CHUNK_COUNT, CHUNK_SIZE, MAP_FILE | MAP_SHARED, fh));
		std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
		fs->superblock->init(0.1);
		
		std::shared_ptr<INode> inode_dir = fs->superblock->inode_table->alloc_inode();
		IDirectory directory(*inode_dir);
		directory.initializeEmpty();

		for (int i = 0; i < 100; ++i) {
			char file_name[255];
			
			sprintf(file_name, "file-%d\0", i);

			char file_contents[255];
			sprintf(file_contents, "the contents of this file is: %d\n", i);
			std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
			REQUIRE(inode->write(0, file_contents, strlen(file_contents) + 1) == strlen(file_contents) + 1);

			directory.add_file(file_name, *inode);
		}

		// step 1: confirm that the number of directories matches the # we would expect
		{
			std::unique_ptr<IDirectory::DirEntry> entry = nullptr;
			size_t count = 0;
			while (entry = directory.next_entry(entry)) {
				count++;
			}

			REQUIRE(count == 100);
		}

		fs = nullptr;
		disk = nullptr;

		disk = std::unique_ptr<Disk>(new Disk(CHUNK_COUNT, CHUNK_SIZE, MAP_FILE | MAP_SHARED, fh));
		fs = std::unique_ptr<FileSystem>(new FileSystem(disk.get()));
		fs->superblock->load_from_disk();

		// step 2: read back each file 1 at a time checking that its contents matches the expected, and then removing it
		for (int i = 0; i < 100; ++i) {
			char file_name[255];
			char file_contents[255];
			char file_contents_expected[255];
			memset(file_contents, 0, sizeof(file_contents));
			
			sprintf(file_name, "file-%d\0", i);
			sprintf(file_contents_expected, "the contents of this file is: %d\n", i);

			// read the file contents
			auto direntry = directory.get_file(file_name);
			auto file_inode = fs->superblock->inode_table->get_inode(direntry->data.inode_idx);
			file_inode->read(0, file_contents, file_inode->data.file_size);

			REQUIRE(strcmp(file_contents_expected, file_contents) == 0);

			REQUIRE(directory.remove_file(file_name)->data.inode_idx == direntry->data.inode_idx);
		}
		
		// step 2: confirm that the number of directories matches the # we would expect (0 b/c we removed them all)
		{
			std::unique_ptr<IDirectory::DirEntry> entry = nullptr;
			size_t count = 0;
			while (entry = directory.next_entry(entry)) {
				count++;
			}

			REQUIRE(count == 0);
		}
	}
}
