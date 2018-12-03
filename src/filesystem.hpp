#ifndef FILESYSTEM_HPP
#define FILESYSTEM_HPP

#include <bitset>
#include <array>
#include <vector>
#include <memory>
#include <cstdint>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <cassert>

#include "diskinterface.hpp"

using Size = uint64_t;

struct FileSystem;
struct INode;
struct INodeTable;
struct SuperBlock;

struct FileSystemException : public StorageException {
	FileSystemException(const std::string &message) : StorageException(message) { };
};

struct SuperBlock;

struct SegmentController {
	std::mutex segment_controller_lock;
	//std::vector<std::mutex> single_segment_locks;
	Disk* disk;
	SuperBlock * superblock;
	uint64_t data_offset;
	uint64_t segment_size;
	uint64_t num_segments;
	uint64_t current_segment;
	uint64_t current_chunk;
	uint64_t num_free_segments;
	uint64_t free_segment_stat_offset;

	uint64_t get_segment_usage(uint64_t segment_number);

	void set_segment_usage(uint64_t segment_number, uint64_t segment_usage);

	uint64_t get_segment_chunk_to_inode(uint64_t segment_number, uint64_t chunk_number);

	void set_segment_chunk_to_inode(uint64_t segment_number, uint64_t chunk_number, uint64_t inode_number);

	void clear_all_segments();

	//Find a new free segment
	void set_new_free_segment();

	//NOTE: only call clean under lock
	void clean();

	uint64_t alloc_next(uint64_t inode_number);

	void free_chunk(std::shared_ptr<Chunk> chunk_to_free);
};

struct SuperBlock {
	Disk *disk = nullptr;
	const uint64_t superblock_size_chunks = 1;
	const uint64_t disk_size_bytes;
	const uint64_t disk_size_chunks;
	const uint64_t disk_chunk_size;

	uint64_t disk_block_map_offset = 0; // chunk in which the disk block map starts
	uint64_t disk_block_map_size_chunks = 0; // number of chunks in disk block map
	std::unique_ptr<DiskBitMap> disk_block_map;

	uint64_t inode_table_inode_count = 0; // number of inodes in the inode_table
	uint64_t inode_table_offset = 0; // chunk in which the inode table starts
	uint64_t inode_table_size_chunks = 0; // number of chunks in the inode table
	std::unique_ptr<INodeTable> inode_table;

	uint64_t data_offset = 0; //where free chunks begin
	uint64_t root_inode_index = 0;

	SegmentController segment_controller;
	uint64_t segment_size_chunks = 0;
	uint64_t num_segments = 0;
	uint64_t num_free_segments = 0;

	SuperBlock(Disk *disk);

	void init(double inode_table_size_rel_to_disk);
	void load_from_disk();

	std::shared_ptr<Chunk> allocate_chunk(uint64_t inode_number) {
		//Allocate the next chunk, does error handling internally
		uint64_t chunk_index = segment_controller.alloc_next(inode_number);
		std::shared_ptr<Chunk> chunk = this->disk->get_chunk(chunk_index);
		
		// zero the newly allocated chunk before we return it
		chunk->memset(chunk->data, 0, this->disk_chunk_size); 

		return std::move(chunk);
	}
};


struct FileSystem {
	Disk *disk;			
	std::unique_ptr<SuperBlock> superblock;

	// the file system, once constructed, takes ownership of the disk
	FileSystem(Disk *disk) : disk(disk), superblock(new SuperBlock(disk)) {
	}

	void printForDebug();
};

struct INode;

struct INodeTable {
	std::recursive_mutex lock;

	SuperBlock *superblock = nullptr;
	uint64_t inode_table_size_chunks = 0; // size of the inode table including used_inodes bitmap + ilist 
	uint64_t inode_table_offset = 0; // this actually winds up being the offset of the used_inodes bitmap
	uint64_t inode_ilist_offset = 0; // this ends up storing the calculated real offset of the inodes
	uint64_t inode_count = 0;
	uint64_t inodes_per_chunk = 0;

	SharedObjectCache<uint64_t, INode> inodecache;
	std::unique_ptr<DiskBitMap> used_inodes;
	// struct INode ilist[10]; //TODO: change the size

	// size and offset are in chunks
	INodeTable(SuperBlock *superblock, uint64_t offset_chunks, uint64_t inode_count);

	void format_inode_table();

	// returns the size of the entire table in chunks
	uint64_t size_chunks();
	uint64_t size_inodes() {
		return inode_count;
	}

	// TODO: have these calls block when an inode is in use
	std::shared_ptr<INode> alloc_inode();
	
	std::shared_ptr<INode> get_inode(uint64_t idx);
	
	// stores the inode back to the inode table
	void update_inode(const INode &node); 

	// releases the slot used by this inode
	// needs to actually be a 'unique' shared ptr to the inode 
	// TODO: figure out a better way to do this
	void free_inode(std::shared_ptr<INode> node);
};

struct INode {
	static constexpr uint64_t DIRECT_ADDRESS_COUNT = 8;
	static constexpr uint64_t INDIRECT_ADDRESS_COUNT = 1;
	static constexpr uint64_t DOUBLE_INDIRECT_ADDRESS_COUNT = 1;
	static constexpr uint64_t TRIPPLE_INDIRECT_ADDRESS_COUNT = 1;
	static constexpr uint64_t ADDRESS_COUNT = DIRECT_ADDRESS_COUNT + INDIRECT_ADDRESS_COUNT + DOUBLE_INDIRECT_ADDRESS_COUNT + TRIPPLE_INDIRECT_ADDRESS_COUNT;
	static const uint64_t INDIRECT_TABLE_SIZES[4];

	static constexpr uint8_t FLAG_IF_DIR = 1;
	static constexpr uint8_t FLAG_IF_REG = 2;

	struct INodeData {
		// we store the data in a subclass so that it can be serialized independently 
		// from data structures that INode needs to keep when loaded in memory
		uint64_t UID = 0; // user id
		uint64_t GID = 0; // group id
		uint64_t last_accessed = 0;
		uint64_t last_modified = 0; //last modified timestamp
		uint64_t file_size = 0; //size of file
		//uint64_t reference_count = 0; //reference count to the inode
		uint64_t addresses[ADDRESS_COUNT] = {0}; //8 direct
		uint16_t permissions = 0644;
		uint8_t file_type = 0;
	};
	
	std::mutex lock;
	uint64_t inode_table_idx = 0;
	INodeData data;
	SuperBlock *superblock = nullptr;	

	~INode() {
		if (this->superblock != nullptr) {
			// stores the data for this inode back into the inode table now that it is 
			// having its destructor called
			this->superblock->inode_table->update_inode(*this);
		}
	}

	std::shared_ptr<Chunk> resolve_indirection(uint64_t chunk_number, bool createIfNotExists);
	void update_chunk_locations(const std::unordered_map<uint64_t, uint64_t> &mapping);

	static uint64_t get_file_size();

	// NOTE: read is NOT const, it will allocate chunks when reading inodes 
	// that have not been written but that ARE within the size of the file,
	// TODO: possibly be smart about this
	uint64_t read(uint64_t starting_offset, char *buf, uint64_t n);
	uint64_t write(uint64_t starting_offset, const char *buf, uint64_t n);
	void release_chunks(); // use this before removing an inode from the inode table

	std::string to_string();

	void set_type(mode_t type){
	    switch(type){
		case S_IFDIR:
		    this->data.file_type = FLAG_IF_DIR;
		    break;
		case S_IFREG:
		    this->data.file_type = FLAG_IF_REG;
		    break;
		default:
		    throw FileSystemException("Invalid File Type");
	    }
	}

	mode_t get_type(){
		switch(this->data.file_type){
		case FLAG_IF_DIR:
			return S_IFDIR;
		case FLAG_IF_REG:
			return S_IFREG;
		default:
			throw FileSystemException("Invalid File Type");
		}
	}
};


/*
	TODO: implement cleaning of a directory
*/
struct IDirectory {
	INode* inode;
public:
	struct DirEntry {
		std::string filename;
		uint64_t inode_idx = 0;
	};

	IDirectory(INode &inode);

	void flush() {};

	void initializeEmpty();

	std::unique_ptr<DirEntry> add_file(const char *filename, const INode &child);

	std::unique_ptr<DirEntry> get_file(const char *filename);

	std::unique_ptr<DirEntry> remove_file(const char *filename);

	std::vector<std::unique_ptr<DirEntry>> get_files();
};

#endif
