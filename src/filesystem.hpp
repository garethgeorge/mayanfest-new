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

#include "diskinterface.hpp"

using Size = uint64_t;

struct FileSystem;
struct INode;
struct INodeTable;
struct SuperBlock;

struct FileSystemException : public StorageException {
	FileSystemException(const std::string &message) : StorageException(message) { };
};

struct SegmentController {
	std::mutex segment_controller_lock;
	//std::vector<std::mutex> single_segment_locks;
	Disk* disk;
	uint64_t data_offset;
	uint64_t segment_size;
	uint64_t num_segments;
	uint64_t current_segment;
	uint64_t current_chunk;
	uint64_t num_free_segments;

	uint64_t get_segment_usage(uint64_t segment_number) {
		std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + segment_number * segment_size);
		return *((uint64_t*)chunk->data);
	}

	void set_segment_usage(uint64_t segment_number, uint64_t segment_usage) {
		std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + segment_number * segment_size);
		*((uint64_t*)chunk->data) = segment_usage;
	}

	uint64_t get_segment_chunk_to_inode(uint64_t segment_number, uint64_t chunk_number) {
		std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + segment_number * segment_size);
		return ((uint64_t*)chunk->data)[chunk_number];
	}

	void set_segment_chunk_to_inode(uint64_t segment_number, uint64_t chunk_number, uint64_t inode_number) {
		std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + segment_number * segment_size);
		((uint64_t*)chunk->data)[chunk_number] = inode_number;
	}

	void clear_all_segments() {
		for(int i = 0; i < num_segments; i++) {
			std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + i * segment_size);
			std::memset(chunk->data, 0, chunk->size_bytes);
		}
		num_free_segments = num_segments;
	}

	//Find a new free segment
	void set_new_free_segment() {
		for(int i = 0; i < num_segments; i++) {
			if(get_segment_usage(i) == 0) {
				current_segment = i;
				current_chunk = 1;
				num_free_segments -= 1;
				return;
			}
		}
		current_segment = -1;
	}

	//TODO: locking
	uint64_t clean() {
		std::vector<uint64_t> segments_to_clean;
		uint64_t new_segment;

		for(int i = 0; i < num_segments; i++) {
			if(get_segment_usage(i) == 0) {
				new_segment = i;
				break;
			}
		}

		uint64_t num_chunks_to_combine = 0;
		int i = 0;
		while(i < num_segments) {
			//only grab non empty segments to clean
			uint64_t usage = get_segment_usage(i);
			if(usage != 0) {
				//try to add this segment to the clean up list
				//Remember to reserve one chunk for meta data
				if(num_chunks_to_combine + usage <= segment_size - 1) {
					segments_to_clean.push_back(i);
					num_chunks_to_combine += usage;
				} else {
					//if it doesn't fit, break if we have more than one segment to clean
					if(segments_to_clean.size() > 1) {
						break;
					} else {
						//otherwise keep the smaller of the two and look for more things to clean.
						if(usage < num_chunks_to_combine) {
							num_chunks_to_combine = usage;
							segments_to_clean.pop_back();
							segments_to_clean.push_back(i);
						}
					}
				}
			}
			i++;
		}
		
		//TODO: change above logic to grab two and do a partial clean (COUNT FREE SEGMENTS), then try cleaning again
		if(segments_to_clean.size() == 1) {
			throw FileSystemException("Could not find free enough segments to clean");
		}

		ASSERT(num_chunks_to_combine > 0);
		ASSERT(num_chunks_to_combine <= segment_size - 1);

		//create the new segment first
		set_segment_usage(new_segment, num_chunks_to_combine);
		uint64_t write_head = 1;
		for(uint64_t sn : segments_to_clean) {
			//hold this for performance
			std::shared_ptr<Chunk> metadata_chunk = disk->get_chunk(data_offset + sn * segment_size);

			//loop over the segment and grab all of the actual data
			for(uint64_t cn = 1, cn < segment_size, cn ++) {
				uint64_t inode_num = get_segment_chunk_to_inode(sn, cn);
				if(inode_num != 0) {
					set_segment_chunk_to_inode(new_segment, write_head, inode_num);
					std::shared_ptr<Chunk> to_read = disk->get_chunk(data_offset + sn * segment_size + cn);
					std::shared_ptr<Chunk> to_write = disk->get_chunk(data_offset + new_segment * segment_size + write_head);
					memcpy((void*)to_write->data, (void*)to_read->data, to_read->size_bytes);
					write_head += 1;
				}
			}
		}

		//update pointers

		//remove the old data
		for(uint64_t sn : segments_to_clean) {
			std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + sn * segment_size);
			std::memset(chunk->data, 0, chunk->size_bytes);
		}

		//update number of free segments
		num_free_segments += segments_to_clean.size() - 1;
	}

	uint64_t alloc_next(uint64_t inode_number) {
		//lock the segment controller, releases automatically at function exit
		std::lock_guard<std::mutex> lock(segment_controller_lock);

		//make sure we still have chunks available in this segment
		if(current_chunk == segment_size) {
			set_new_free_segment();
		}

		//TODO: Try cleaning first?
		clean();

		//throw exception if disk full
		if(current_segment == -1) {
			throw FileSystemException("FileSystem out of space -- unable to allocate a new chunk");
		}

		//increment segment usage
		uint64_t usage = get_segment_usage(current_segment);
		if(usage == 0) {
			num_free_segments -= 1;
		}
		set_segment_usage(current_segment, get_segment_usage(current_segment) + 1);	

		//set the inode mapping
		set_segment_chunk_to_inode(current_segment, current_chunk, inode_number);

		//compute absolute index of current chunk
		uint64_t ret = data_offset + current_segment * segment_size + current_chunk;

		//update current chunk

		// fprintf(stdout, "allocated chunk %d in segment %d for inode %d, absolute chunk id: %d\n"
		// 	"\tusage: %d out of %d\n", 
		// 	current_chunk, current_segment, inode_number, ret,
		// 	get_segment_usage(current_segment), segment_size - 1);
		
		current_chunk++;

		return ret;
	}

	void free_chunk(std::shared_ptr<Chunk> chunk_to_free) {
		if (!chunk_to_free.unique()) {
			throw FileSystemException("FileSystem free chunk failed -- the chunk passed was not 'unique', something else is using it");
		}
		//lock the segment controller
		std::lock_guard<std::mutex> lock(segment_controller_lock);
		//get the segment and relative chunk number
		uint64_t segment_number = (chunk_to_free->chunk_idx - data_offset) / segment_size;
		uint64_t chunk_number = chunk_to_free->chunk_idx - (segment_number * segment_size) - data_offset;
		//clear the inode mapping
		set_segment_chunk_to_inode(segment_number, chunk_number, 0);
		//decrement segment usage
		uint64_t usage = get_segment_usage(segment_number);
		set_segment_usage(segment_number, usage - 1);
		if(usage - 1 == 0) {
			num_free_segments += 1;
		}
	}
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


	SuperBlock(Disk *disk);

	void init(double inode_table_size_rel_to_disk);
	void load_from_disk();

	std::shared_ptr<Chunk> allocate_chunk(uint64_t inode_number) {
		//Allocate the next chunk, does error handling internally
		uint64_t chunk_index = segment_controller.alloc_next(inode_number);
		std::shared_ptr<Chunk> chunk = this->disk->get_chunk(chunk_index);
		
		// zero the newly allocated chunk before we return it
		std::memset(chunk->data, 0, this->disk_chunk_size); 

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
private:
	struct DirHeader {
		uint64_t record_count = 0;
		uint64_t deleted_record_count = 0;

		uint64_t dir_entries_tail = 0;
		uint64_t dir_entries_head = 0;
	};

	DirHeader header;
	INode* inode;
public:

	struct DirEntry {
		struct DirEntryData {
			uint64_t next_entry_ptr = 0;
			uint64_t filename_length = 0;
			uint64_t inode_idx = 0;
		};

		DirEntry(INode *inode) : inode(inode) { };

		uint64_t offset = 0;
		INode* inode;
		DirEntryData data;
		char *filename = nullptr;

		~DirEntry() {
			if (filename != nullptr) {
				free(filename);
			}
		}

		// returns the size of the thing it read
		uint64_t read_from_disk(size_t offset);

		// only pass filename if you want to update it
		uint64_t write_to_disk(size_t offset, const char *filename);
	};

	IDirectory(INode &inode);

	void flush();

	void initializeEmpty();

	std::unique_ptr<DirEntry> add_file(const char *filename, const INode &child);

	std::unique_ptr<DirEntry> get_file(const char *filename);

	std::unique_ptr<DirEntry> remove_file(const char *filename);

	std::unique_ptr<DirEntry> next_entry(const std::unique_ptr<DirEntry>& entry);
};


#endif
