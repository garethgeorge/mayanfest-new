#include <iostream>
#include <cstdlib>
#include <ctime>
#include <vector>

#include "catch.hpp"

#include "diskinterface.hpp"
#include "filesystem.hpp"

const auto get_random_buffer = [](size_t size, bool nullTerminate = false) -> std::vector<char> {
	std::vector<char> buf;
	buf.resize(size + (nullTerminate ? 1 : 0));
	for (size_t i = 0; i < size;) {
		size_t j = 0;
		size_t r = rand();
		while (j < 6 && i < size) {
			buf[i] = ('a' + r % 26);
			r /= 26;

			j++;
			i++;
		}
	}
	if (nullTerminate) {
		buf[size] = '\0';
	}
	return std::move(buf);
};


TEST_CASE( "Making a filesystem should work", "[filesystem]" ) {
	constexpr uint64_t CHUNK_COUNT = 4096;
	constexpr uint64_t CHUNK_SIZE = 4096;

	std::unique_ptr<Disk> disk(new Disk(CHUNK_COUNT, CHUNK_SIZE));
	{
		std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
		fs->superblock->init(0.1);
		fs = nullptr;
	}

	{
		std::cout << "Load the filesystem from the disk" << std::endl;
		std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
		fs->superblock->load_from_disk();
		fs = nullptr;
	}
}

TEST_CASE("INode read/write test", "[filesystem][readwrite][readwrite.orderly]") {
	const auto test_inode = [](int offset, int length) {
		std::unique_ptr<Disk> disk(new Disk(1024, 512));
		std::vector<char> read_back;

		std::vector<char> to_write = get_random_buffer(length);
		int64_t inode_idx = 0;

		to_write.push_back('\0');
		{
			std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
			fs->superblock->init(0.1);

			read_back.resize(to_write.size());
			
			std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
			inode_idx = inode->inode_table_idx;

			inode->superblock = fs->superblock.get();
			REQUIRE(inode->superblock->disk == disk.get());

			REQUIRE(inode->write(offset, &(to_write[0]), length) == length);
			REQUIRE(inode->read(offset, &(read_back[0]), length) == length);

			REQUIRE(strcmp(&to_write[0], &read_back[0]) == 0);

			// NOTE: the order that these are nulled in IS important
			inode = nullptr;
			fs = nullptr;
		}

		{
			std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
			fs->superblock->load_from_disk();
			std::shared_ptr<Chunk> page0 = disk->get_chunk(0);

			std::vector<char> read_back1;
			read_back1.resize(to_write.size());

			std::shared_ptr<INode> inode = fs->superblock->inode_table->get_inode(inode_idx);
			inode->superblock = fs->superblock.get();
			REQUIRE(inode->superblock->disk == disk.get());
			
			REQUIRE(inode->read(offset, &(read_back1[0]), length) == length);
			REQUIRE(strcmp(&to_write[0], &read_back1[0]) == 0);

			inode = nullptr;
			fs = nullptr;
		}
	};

	SECTION("Can write strings of length 1 - 10000") {
		for (int i = 0; i < 10000; ++i) {
			test_inode(0, i);
		}
	}

	SECTION("Can write strings of length 100 at offsets 1 - 10000") {
		for (int i = 0; i < 10000; ++i) {
			test_inode(i, 100);
		}
	}

	SECTION("Can write strings of length 1000 at offsets 1 - 10000") {
		for (int i = 0; i < 10000; ++i) {
			test_inode(i, 1000);
		}
	}

	SECTION("Can write strings of length 2000 at offsets 1 - 10000") {
		for (int i = 0; i < 10000; ++i) {
			test_inode(i, 2000);
		}
	}
}

TEST_CASE("INode read/write test with random patterns", "[filesystem][readwrite][readwrite.random]") {
	const auto test_inode = [](INode& inode, int offset, int length) {
		std::vector<char> to_write;

		for (int i = 0; i < length; ++i) {
			to_write.push_back('a' + (rand() % 26));
		}
		to_write.push_back('\0');

		std::vector<char> read_back;
		read_back.resize(to_write.size());

		REQUIRE(inode.write(offset, &(to_write[0]), length) == length);
		REQUIRE(inode.read(offset, &(read_back[0]), length) == length);
		
		REQUIRE(strcmp(&to_write[0], &read_back[0]) == 0);
	};

	SECTION("An aggressively random test ;) -- I'm a firin mah lazors") {
		std::unique_ptr<Disk> disk(new Disk(100 * 1024, 512));
		std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
		fs->superblock->init(0.1);
		
		uint64_t seed = std::time(0);
		fprintf(stdout, "SRAND SEED WAS 0x%x\n", seed);
		srand(seed);

		int64_t bytes_to_write = (uint64_t) (disk->size_bytes() * 0.8); // good margin to write

		while (bytes_to_write > 0) {

			std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
			REQUIRE(inode->superblock->disk == disk.get());

			for (int j = rand() % 100; j > 0; --j) { // write up to 8 segments to the same inode
				uint64_t bytes = rand() % 5000;
				uint64_t offset = rand() % 25000;
				test_inode(*inode, offset, bytes);
				bytes_to_write -= (bytes / disk->chunk_size() + 1) * disk->chunk_size();
				//std::cout << "wrote " << bytes << " bytes at offset " << offset << std::endl;
			}
		}
	}
}



TEST_CASE("INode write all, then readback all, reconstruct disk, and then do it again!!!", "[filesyste][readwrite][readwrite.rwrecon]") {
	std::unique_ptr<Disk> disk(new Disk(10 * 1024, 512));
	std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
	fs->superblock->init(0.1);

	const size_t FILE_SIZE = 250 * 1024; // 100 kb
	std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
	assert(inode->superblock == fs->superblock.get());

	std::unique_ptr<char[]> mem_file(new char[FILE_SIZE]);
	std::unique_ptr<char[]> mem_file_readback(new char[FILE_SIZE]);
	std::memset((void *)mem_file.get(), 0, FILE_SIZE);
	std::memset((void *)mem_file_readback.get(), 0, FILE_SIZE);

	for (size_t idx = 0; idx < 20000; ++idx) {
		size_t size = rand() % (16 * 1024);
		size_t offset = rand() % (FILE_SIZE - size);
		// std::cout << "writing size: " << size << " bytes at offset: " << offset << std::endl;
		std::vector<char> buffer = get_random_buffer(size);
		REQUIRE(inode->write(offset, &(buffer[0]), size) == size);
		std::memcpy((void *)(mem_file.get() + offset), &(buffer[0]), size);
	}

	// NOTE: YOU MUST WRITE INODES BACK OUT WHEN YOU ARE DONE WITH THEM 
	inode->data.file_size = FILE_SIZE;
	uint64_t inode_table_idx = inode->inode_table_idx;
	inode = nullptr;

	inode = fs->superblock->inode_table->get_inode(inode_table_idx);

	REQUIRE(inode->read(0, mem_file_readback.get(), FILE_SIZE) == FILE_SIZE);
	REQUIRE(std::memcmp(mem_file.get(), mem_file_readback.get(), FILE_SIZE) == 0);
}

TEST_CASE("Smaller version of INode write all, then readback all, reconstruct disk, and then do it again!!! but using a very high base offset", "[filesyste][readwrite][readwrite.rwrecon]") {
	const auto get_random_buffer = [](size_t size, bool nullTerminate = false) -> std::vector<char> {
		std::vector<char> buf;
		buf.reserve(size + 1);
		for (size_t i = 0; i < size; ++i) {
			buf.push_back('a' + rand() % 26);
		}
		buf.push_back('\0');
		return std::move(buf);
	};

	std::unique_ptr<Disk> disk(new Disk(10 * 1024, 512));
	std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
	fs->superblock->init(0.1);

	const size_t BASE_OFFSET = 1024 * 1024;
	const size_t FILE_SIZE = 25 * 1024; // 100 kb
	std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
	assert(inode->superblock == fs->superblock.get());

	std::unique_ptr<char[]> mem_file(new char[FILE_SIZE]);
	std::unique_ptr<char[]> mem_file_readback(new char[FILE_SIZE]);
	std::memset((void *)mem_file.get(), 0, FILE_SIZE);
	std::memset((void *)mem_file_readback.get(), 0, FILE_SIZE);

	for (size_t idx = 0; idx < 1000; ++idx) {
		size_t size = rand() % (1 * 1024);
		size_t offset = rand() % (FILE_SIZE - size) + BASE_OFFSET;
		// std::cout << "writing size: " << size << " bytes at offset: " << offset << std::endl;
		std::vector<char> buffer = get_random_buffer(size);
		REQUIRE(inode->write(offset, &(buffer[0]), size) == size);
		std::memcpy((void *)(mem_file.get() + offset - BASE_OFFSET), &(buffer[0]), size);
	}

	REQUIRE(inode->read(BASE_OFFSET, mem_file_readback.get(), FILE_SIZE) == FILE_SIZE);
	REQUIRE(std::memcmp(mem_file.get(), mem_file_readback.get(), FILE_SIZE) == 0);

	// rebuild the filesystem and make sure that it is still the same :) 
	uint64_t inode_idx = inode->inode_table_idx;
	inode = nullptr;
	fs = nullptr;
	fs = std::unique_ptr<FileSystem>(new FileSystem(disk.get()));
	fs->superblock->load_from_disk();

	inode = fs->superblock->inode_table->get_inode(inode_idx);
	REQUIRE(inode->read(BASE_OFFSET, mem_file_readback.get(), FILE_SIZE) == FILE_SIZE);
	REQUIRE(std::memcmp(mem_file.get(), mem_file_readback.get(), FILE_SIZE) == 0);
}

// /*
// 	TODO: test behavior with random disk sizes
// */

TEST_CASE("INode write overlapping strings", "[INode]"){
 	constexpr uint64_t CHUNK_COUNT = 1024;
 	constexpr uint64_t CHUNK_SIZE = 1024;

 	std::unique_ptr<Disk> disk(new Disk(CHUNK_COUNT, CHUNK_SIZE));
 	std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
 	fs->superblock->init(0.1);

	SECTION("INodes can be written with overlapping strings and read back"){
		std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
		REQUIRE(fs->superblock->disk == disk.get());
		REQUIRE(inode->superblock->disk == disk.get());
		
		char str1[] = "ab";
		char str2[] = "cd";
		char buf[4];
		std::memset(buf, 0, 4);
		inode->write(0, str1, sizeof(str1) - 1);
		inode->write(1, str2, sizeof(str2) - 1);
		inode->read(0, buf, 3);
		REQUIRE(strcmp(buf, "acd") == 0);
	}

	SECTION("INodes can be re-written with overlapping strings and read back"){
		std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
		REQUIRE(fs->superblock->disk == disk.get());
		inode->superblock = fs->superblock.get();
		REQUIRE(inode->superblock->disk == disk.get());
		
		char str1[] = "ab";
		char str2[] = "cd";
		char buf[4];
		std::memset(buf, 0, 4);
		inode->write(0, str1, sizeof(str1) - 1);
		inode->write(1, str2, sizeof(str2) - 1);
		//rewrite
		inode->write(0, str1, sizeof(str1) - 1);
		inode->write(1, str2, sizeof(str2) - 1);

		inode->read(0, buf, 3);
		REQUIRE(strcmp(buf, "acd") == 0);
	}

	SECTION("INodes can be written with overlapping strings beginning at random offsets and read back"){
		std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
		REQUIRE(fs->superblock->disk == disk.get());
		inode->superblock = fs->superblock.get();
		REQUIRE(inode->superblock->disk == disk.get());
		
		char str1[] = "abcd";
		char str2[] = "efgh";
		char buf[6];
		std::memset(buf, 0, 6);
		inode->write(1022, str1, sizeof(str1) - 1);
		inode->write(1023, str2, sizeof(str2) - 1);

		inode->read(1022, buf, 5);
		REQUIRE(strcmp(buf, "aefgh") == 0);
	}

	SECTION("INodes can be re-written with overlapping strings beginning at random offsets and read back"){
		std::shared_ptr<INode> inode = fs->superblock->inode_table->alloc_inode();
		REQUIRE(fs->superblock->disk == disk.get());
		inode->superblock = fs->superblock.get();
		REQUIRE(inode->superblock->disk == disk.get());
		
		char str1[] = "abcd";
		char str2[] = "efgh";
		char buf[6];
		std::memset(buf, 0, 6);
		inode->write(1022, str1, sizeof(str1) - 1);
		inode->write(1023, str2, sizeof(str2) - 1);
		// rewrite
		inode->write(1022, str1, sizeof(str1) - 1);
		inode->write(1023, str2, sizeof(str2) - 1);

		inode->read(1022, buf, 5);
		REQUIRE(strcmp(buf, "aefgh") == 0);
	}
}

TEST_CASE("INodes can be used to store and read directories", "[filesystem][idirectory]") {
	std::unique_ptr<Disk> disk(new Disk(10 * 1024, 512));
	std::unique_ptr<FileSystem> fs(new FileSystem(disk.get()));
	fs->superblock->init(0.1);

	SECTION("Can write a SINGLE file to a directory") {
		std::shared_ptr<INode> inode_dir = fs->superblock->inode_table->alloc_inode();
		std::shared_ptr<INode> inode_file = fs->superblock->inode_table->alloc_inode();
		inode_file->write(0, "hello there!!!", sizeof("hello there!!!"));

		IDirectory directory(*inode_dir);
		directory.initializeEmpty();
		directory.add_file("hello_world", *inode_file);
		directory.flush();
	}

	SECTION("Can write a SINGLE file to a directory AND get it back") {
		std::shared_ptr<INode> inode_dir = fs->superblock->inode_table->alloc_inode();
		std::shared_ptr<INode> inode_file = fs->superblock->inode_table->alloc_inode();
		inode_file->write(0, "hello there!!!", sizeof("hello there!!!"));

		IDirectory directory(*inode_dir);
		directory.initializeEmpty();
		directory.add_file("hello_world", *inode_file);
		directory.flush();

		std::unique_ptr<IDirectory::DirEntry> entry = directory.next_entry(nullptr);
		REQUIRE(entry != nullptr);
		REQUIRE(entry->data.inode_idx == inode_file->inode_table_idx);
		REQUIRE(strcmp(entry->filename, "hello_world") == 0);
	}

	SECTION("Can write TWO files to a directory AND get them back") {
		std::shared_ptr<INode> inode_dir = fs->superblock->inode_table->alloc_inode();
		std::shared_ptr<INode> inode_file = fs->superblock->inode_table->alloc_inode();
		std::shared_ptr<INode> inode_file2 = fs->superblock->inode_table->alloc_inode();
		inode_file->write(0, "hello there1!!!", sizeof("hello there1!!!"));
		inode_file2->write(0, "hello there2!!!", sizeof("hello there2!!!"));

		IDirectory directory(*inode_dir);
		directory.initializeEmpty();
		directory.add_file("hello_world", *inode_file);
		directory.add_file("hello_world2", *inode_file2);
		directory.flush();

		std::unique_ptr<IDirectory::DirEntry> entry = directory.next_entry(nullptr);
		REQUIRE(entry->data.inode_idx == inode_file->inode_table_idx);
		REQUIRE(strcmp(entry->filename, "hello_world") == 0);

		std::unique_ptr<IDirectory::DirEntry> entry2 = directory.next_entry(entry);
		REQUIRE(entry2->data.inode_idx == inode_file2->inode_table_idx);
		REQUIRE(strcmp(entry2->filename, "hello_world2") == 0);
	}

	SECTION("Can write TWO files to a directory AND get them back BY NAME and then remove them both") {
		std::shared_ptr<INode> inode_dir = fs->superblock->inode_table->alloc_inode();
		std::shared_ptr<INode> inode_file = fs->superblock->inode_table->alloc_inode();
		std::shared_ptr<INode> inode_file2 = fs->superblock->inode_table->alloc_inode();
		inode_file->write(0, "hello there!!!", sizeof("hello there!!!"));
		inode_file2->write(0, "hello there!!!", sizeof("hello there!!!"));

		IDirectory directory(*inode_dir);
		directory.initializeEmpty();
		directory.add_file("hello_world", *inode_file);
		directory.add_file("hello_world2", *inode_file2);

		std::unique_ptr<IDirectory::DirEntry> entry = directory.get_file("hello_world");
		REQUIRE(entry->data.inode_idx == inode_file->inode_table_idx);
		REQUIRE(strcmp(entry->filename, "hello_world") == 0);

		std::unique_ptr<IDirectory::DirEntry> entry2 = directory.get_file("hello_world2");
		REQUIRE(entry2->data.inode_idx == inode_file2->inode_table_idx);
		REQUIRE(strcmp(entry2->filename, "hello_world2") == 0);

		// TEST REMOVING A FILE
		REQUIRE(directory.remove_file("hello_world") != nullptr);
		REQUIRE(directory.get_file("hello_world") == nullptr);

		// TEST REMOVING A FILE
		REQUIRE(directory.remove_file("hello_world2") != nullptr);
		REQUIRE(directory.get_file("hello_world2") == nullptr);
	}

	SECTION("can write a thousand files, each of which contains a single number base 10 encoded") {
		
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
