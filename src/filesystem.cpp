#include <bitset>
#include <array>
#include <vector>
#include <memory>
#include <cassert>
#include <sstream>

#include "diskinterface.hpp"
#include "filesystem.hpp"

// #define DEBUG

using Size = uint64_t;

const uint64_t INode::INDIRECT_TABLE_SIZES[4] = {DIRECT_ADDRESS_COUNT, INDIRECT_ADDRESS_COUNT, DOUBLE_INDIRECT_ADDRESS_COUNT, TRIPPLE_INDIRECT_ADDRESS_COUNT};

uint64_t INode::read(uint64_t starting_offset, char *buf, uint64_t bytes_to_write) {
	const uint64_t chunk_size = this->superblock->disk_chunk_size;
    int64_t n = bytes_to_write;
    uint64_t bytes_written = bytes_to_write;

    if (starting_offset + bytes_to_write > this->data.file_size) {
        // TODO: test this error case
        if (starting_offset > this->data.file_size) 
            return 0;

        bytes_to_write = this->data.file_size - starting_offset;
    }
    
    // room to write for the first chunk
    const uint64_t room_first_chunk = chunk_size - starting_offset % chunk_size;
    uint64_t bytes_write_first_chunk = room_first_chunk;
    if (n < room_first_chunk) {
        bytes_write_first_chunk = n;
    }

    {
        std::shared_ptr<Chunk> chunk = this->resolve_indirection(starting_offset / chunk_size, false);
        if (chunk == nullptr) {
            std::memset(buf, 0, bytes_write_first_chunk);
        } else {
            std::lock_guard<std::mutex> g(chunk->lock);
            std::memcpy(buf, chunk->data + starting_offset % chunk_size, bytes_write_first_chunk);
        }
        
        buf += bytes_write_first_chunk;
        n -= bytes_write_first_chunk;
    }
    

    if (n == 0) { // early return if we wrote less than a chunk
        return bytes_to_write;
    }

    // fix the starting offset
    starting_offset += bytes_write_first_chunk;
    assert(starting_offset % chunk_size == 0);

    while (n > chunk_size) {
        std::shared_ptr<Chunk> chunk = this->resolve_indirection(starting_offset / chunk_size, false);
        if (chunk == nullptr) {
            std::memset(buf, 0, chunk_size);
        } else {
            std::lock_guard<std::mutex> g(chunk->lock);
            std::memcpy(buf, chunk->data, chunk_size);
        }

        buf += chunk_size;
        n -= chunk_size;
        starting_offset += chunk_size;
    }
    
    {
        std::shared_ptr<Chunk> chunk = this->resolve_indirection(starting_offset / chunk_size, false);
        if (chunk == nullptr) {
            std::memset(buf, 0, n);
        } else {
            std::lock_guard<std::mutex> g(chunk->lock);
            std::memcpy(buf, chunk->data, n);
        }
    }

    return bytes_written;
}

uint64_t INode::write(uint64_t starting_offset, const char *buf, uint64_t bytes_to_write) {
    //clean whenever we have less than this percentage of disk free
    const double threshold = 0.25;

    if(this->superblock->segment_controller.num_free_segments <= this->superblock->segment_controller.num_segments * threshold) {
        this->superblock->segment_controller.clean();
    }

    const uint64_t original_starting_offset = starting_offset;
    const uint64_t chunk_size = this->superblock->disk_chunk_size;
    int64_t n = bytes_to_write;
    try {
        // room to write for the first chunk
        const uint64_t room_first_chunk = chunk_size - starting_offset % chunk_size;
        uint64_t bytes_write_first_chunk = room_first_chunk;
        if (n < room_first_chunk) {
            bytes_write_first_chunk = n;
        }

        {
            std::shared_ptr<Chunk> chunk = this->resolve_indirection(starting_offset / chunk_size, true);
            std::lock_guard<std::mutex> g(chunk->lock);
            assert(bytes_write_first_chunk <= chunk_size);
            assert(starting_offset % chunk_size + bytes_write_first_chunk <= chunk_size);
            chunk->memcpy(chunk->data + (starting_offset % chunk_size), buf, bytes_write_first_chunk);
            buf += bytes_write_first_chunk;
            n -= bytes_write_first_chunk;
        }
        
        if (n == 0) { // early return if we wrote less than a chunk
            // make sure the filesize at the end is correct no matter what happens
            if (original_starting_offset + bytes_to_write > this->data.file_size) {
                this->data.file_size = original_starting_offset + bytes_to_write;
            }
            return bytes_to_write;
        }

        // fix the starting offset
        starting_offset += bytes_write_first_chunk;
        assert(starting_offset % chunk_size == 0);

        while (n > chunk_size) {
            std::shared_ptr<Chunk> chunk = this->resolve_indirection(starting_offset / chunk_size, true);
            std::lock_guard<std::mutex> g(chunk->lock);
            chunk->memcpy(chunk->data, buf, chunk_size);
            buf += chunk_size;
            n -= chunk_size;
            starting_offset += chunk_size;
        }
        
        {
            assert(n <= chunk_size);
            std::shared_ptr<Chunk> chunk = this->resolve_indirection(starting_offset / chunk_size, true);
            std::lock_guard<std::mutex> g(chunk->lock);
            chunk->memcpy(chunk->data, buf, n);
        }
    } catch (const FileSystemException& e) {
        // make sure the filesize at the end is correct no matter what happens
        if (original_starting_offset + bytes_to_write - n > this->data.file_size) {
            this->data.file_size = original_starting_offset + bytes_to_write - n;
        }
        throw e;
    }

    // make sure the filesize at the end is correct no matter what happens
    if (original_starting_offset + bytes_to_write > this->data.file_size) {
        this->data.file_size = original_starting_offset + bytes_to_write;
    }

    return bytes_to_write;
}

// static std::shared_ptr<Chunk> resolve_indirection_helper(
//     INode *inode, 
//     uint64_t *indirect_table, 
//     const uint64_t chunk_number,
//     const uint64_t indirection,
//     const uint64_t indirect_address_count,
//     bool createIfNotExists
// ) {
//     uint64_t next_chunk_loc = indirect_table[chunk_number / indirect_address_count];
//     const uint64_t num_chunk_address_per_chunk = inode->superblock->disk_chunk_size / sizeof(uint64_t);

//     // go through the process of getting the chunk
//     std::shared_ptr<Chunk> chunk = nullptr;
//     if (createIfNotExists) {
//         chunk = inode->superblock->allocate_chunk(inode->inode_table_idx);
//         if (next_chunk_loc == 0) {
//             std::shared_ptr<Chunk> inherit_from = inode->superblock->disk->get_chunk(next_chunk_loc);
//             std::memcpy(chunk->data, inherit_from->data, chunk->size_bytes);
//         } else {
//             std::memset(chunk->data, 0, chunk->size_bytes);
//         }
//     } else {
//         if (next_chunk_loc == 0) {
//             return nullptr;
//         }
//         chunk = inode->superblock->disk->get_chunk(next_chunk_loc);
//     }
//     assert(chunk != nullptr);

//     // set it back into the indirect table of the parent chunk or whatever :) we update it!
//     indirect_table[chunk_number] = chunk->chunk_idx;

//     if (indirection == 0) {
//         assert(indirect_address_count == 0);
//         return chunk;
//     } else {
//         uint64_t *indirect_table = (uint64_t *)chunk->data;
//         return resolve_indirection_helper(
//             inode, 
//             indirect_table, 
//             chunk_number % indirect_address_count, 
//             indirection - 1,
//             indirect_address_count / num_chunk_address_per_chunk, 
//             createIfNotExists
//         );
//     }
// }

// std::shared_ptr<Chunk> INode::resolve_indirection(uint64_t chunk_number, bool createIfNotExists) {
//     const uint64_t num_chunk_address_per_chunk = superblock->disk_chunk_size / sizeof(uint64_t);
//     uint64_t indirect_address_count = 1;

// #ifdef DEBUG
//     fprintf(stdout, "INode::resolve_indirection for chunk_number %llu (inode no: %llu)\n", chunk_number, this->inode_table_idx);
// #endif 

//     uint64_t *indirect_table = this->data.addresses; 
//     for(uint64_t indirection = 0; indirection < sizeof(INDIRECT_TABLE_SIZES) / sizeof(uint64_t); indirection++){
// #ifdef DEBUG 
//         fprintf(stdout, 
//             "INode::resolve_indirection looking for chunk_number %llu"
//             " at indirect table level %llu\n", chunk_number, indirection);
// #endif 

//         if(chunk_number < (indirect_address_count * INDIRECT_TABLE_SIZES[indirection])) {
//             return resolve_indirection_helper(
//                 this, 
//                 indirect_table,
//                 chunk_number, 
//                 indirection, 
//                 indirect_address_count, 
//                 createIfNotExists
//             );
//         }

//         chunk_number -= (indirect_address_count * INDIRECT_TABLE_SIZES[indirection]);
//         indirect_table += INDIRECT_TABLE_SIZES[indirection];
//         indirect_address_count *= num_chunk_address_per_chunk;
//     }

//     if (createIfNotExists) {
//         throw FileSystemException("Indirect table does not have enough space for a chunk at that high of an offset");
//     }
//     return nullptr;
// }

std::shared_ptr<Chunk> INode::resolve_indirection(uint64_t chunk_number, bool createIfNotExists) {
    const uint64_t num_chunk_address_per_chunk = superblock->disk_chunk_size / sizeof(uint64_t);
    uint64_t indirect_address_count = 1;

#ifdef DEBUG
    fprintf(stdout, "INode::resolve_indirection for chunk_number %llu (inode no: %llu)\n", chunk_number, this->inode_table_idx);
#endif 

    uint64_t *indirect_table = data.addresses; 
    for(uint64_t indirection = 0; indirection < sizeof(INDIRECT_TABLE_SIZES) / sizeof(uint64_t); indirection++){
#ifdef DEBUG 
        fprintf(stdout, 
            "INode::resolve_indirection looking for chunk_number %llu"
            " at indirect table level %llu\n", chunk_number, indirection);
#endif 

        if(chunk_number < (indirect_address_count * INDIRECT_TABLE_SIZES[indirection])){
            size_t indirect_table_idx = chunk_number / indirect_address_count;
            // chunk_number / indirect_address_count + INDIRECT_TABLE_SIZES[indirection];
            uint64_t next_chunk_loc = indirect_table[indirect_table_idx];
#ifdef DEBUG 
            fprintf(stdout, "Determined that the chunk is in fact located in the table at level %llu\n", indirection);
            fprintf(stdout, "Looked up the indirection table at index %llu and found chunk id %llu\n"
                            "\tside note: indirect address count at this level is %llu\n", 
                    indirect_table_idx,
                    next_chunk_loc,
                    indirect_address_count
                );
#endif
            if (!createIfNotExists && next_chunk_loc == 0) {
                return nullptr;
            }

            if (createIfNotExists) {
                std::shared_ptr<Chunk> newChunk = this->superblock->allocate_chunk(this->inode_table_idx);
#ifdef DEBUG 
                fprintf(stdout, "next_chunk_loc was 0, so we created new "
                    "chunk id %zu/%llu and placed it in the table\n", 
                    newChunk->chunk_idx, 
                    this->superblock->disk->size_chunks());
#endif
                if (next_chunk_loc != 0) {
                    std::shared_ptr<Chunk> oldChunk = this->superblock->disk->get_chunk(next_chunk_loc);
                    newChunk->memcpy((void *)newChunk->data, (void *)oldChunk->data, newChunk->size_bytes, oldChunk);
                    this->superblock->segment_controller.free_chunk(std::move(oldChunk));
                } else {
                    newChunk->memset((void *)newChunk->data, 0, newChunk->size_bytes);
                }
                indirect_table[indirect_table_idx] = newChunk->chunk_idx;
                next_chunk_loc = newChunk->chunk_idx;
                
#ifdef DEBUG
                fprintf(stdout, "the real next_chunk_loc is %llu\n", next_chunk_loc);
#endif
            }

#ifdef DEBUG
            fprintf(stdout, "chasing chunk through the indirection table:\n");
#endif 
            std::shared_ptr<Chunk> chunk = superblock->disk->get_chunk(next_chunk_loc);
            // TODO: implement locking on this chunk, this will be HARD HARD HARD because of all the places
            // the reference to the chunk is changed

            while (indirection != 0) {
                indirect_address_count /= num_chunk_address_per_chunk;
#ifdef DEBUG 
                fprintf(stdout, "\tcurrent indirect level is: %llu, indirect block id is: %llu\n", indirection, chunk->chunk_idx);
#endif 
                uint64_t *lookup_table = (uint64_t *)chunk->data;
                next_chunk_loc = lookup_table[chunk_number / indirect_address_count];

#ifdef DEBUG 
                fprintf(stdout, "\tfound next_chunk_loc %llu in table at index %llu\n"
                    "\t\tside note: indirect address count is %llu\n", 
                    next_chunk_loc, 
                    chunk_number / indirect_address_count,
                    indirect_address_count);
#endif

                if (!createIfNotExists && next_chunk_loc == 0) {
                    return nullptr;
                }

                if (createIfNotExists) {
                    std::shared_ptr<Chunk> newChunk = this->superblock->allocate_chunk(this->inode_table_idx);
                    if (next_chunk_loc != 0) {
                        std::shared_ptr<Chunk> oldChunk = this->superblock->disk->get_chunk(next_chunk_loc);
                        newChunk->memcpy((void *)newChunk->data, (void *)oldChunk->data, newChunk->size_bytes, oldChunk);
                        this->superblock->segment_controller.free_chunk(std::move(oldChunk));
                    } else {
                        newChunk->memset((void *)newChunk->data, 0, newChunk->size_bytes);
                    }
                    next_chunk_loc = newChunk->chunk_idx;
                    lookup_table[chunk_number / indirect_address_count] = newChunk->chunk_idx;
#ifdef DEBUG 
                    fprintf(stdout, "\tnext_chunk_loc was 0, so we created new "
                        "chunk id %zu/%llu and placed it in the table\n", 
                        newChunk->chunk_idx, this->superblock->disk->size_chunks());
#endif 
                }

                chunk = superblock->disk->get_chunk(next_chunk_loc);
                chunk_number %= indirect_address_count;

                indirection--;
            }

#ifdef DEBUG 
            fprintf(stdout, "found chunk with id %zu, parent disk %llx\n", chunk->chunk_idx, (unsigned long long)chunk->parent);
#endif 

            return chunk;
        }
        chunk_number -= (indirect_address_count * INDIRECT_TABLE_SIZES[indirection]);
        indirect_table += INDIRECT_TABLE_SIZES[indirection];
        indirect_address_count *= num_chunk_address_per_chunk;
    }

    if (createIfNotExists) {
        fprintf(stdout, "\tERROR! THIS SHOULD NEVER HAPPEN. INODE INDIRECTION TABLE RAN OUT OF SPACE. TELL A PROGRAMMER\n");
        throw FileSystemException("INode indirection table ran out of space");
    }
    return nullptr;
}

static uint64_t update_indirect_locations(INode *inode, const std::unordered_map<uint64_t, uint64_t> &mapping, const uint64_t old_chunk_idx, const uint64_t indirection) {
    const uint64_t num_chunk_address_per_chunk = inode->superblock->disk_chunk_size / sizeof(uint64_t);

    uint64_t chunk_idx = old_chunk_idx;
    auto entry = mapping.find(chunk_idx);
    if (entry != mapping.end()) {
        chunk_idx = (*entry).second; // we update the chunk idx
    }

    if (indirection > 0) {
        // get a copy of our indirect chunk
        std::shared_ptr<Chunk> chunk = inode->superblock->disk->get_chunk(chunk_idx);
        uint64_t *indirect_page = (uint64_t *)chunk->data;

        for (size_t idx = 0; idx < num_chunk_address_per_chunk; idx++) {
            if (indirect_page[idx] != 0) {
                indirect_page[idx] = update_indirect_locations(inode, mapping, indirect_page[idx], indirection - 1);
            }
        }
    }

    return chunk_idx;
};

void INode::update_chunk_locations(const std::unordered_map<uint64_t, uint64_t> &mapping) {
    uint64_t *indirect_table = this->data.addresses; 

    for(uint64_t indirection = 0; indirection < sizeof(INDIRECT_TABLE_SIZES) / sizeof(uint64_t); indirection++){
        for (uint64_t offset = 0; offset < INDIRECT_TABLE_SIZES[indirection]; ++offset) {
            const uint64_t chunk_idx = indirect_table[offset];
            if (chunk_idx != 0) {
                indirect_table[offset] = update_indirect_locations(this, mapping, chunk_idx, indirection);
            }
        }
        indirect_table += INDIRECT_TABLE_SIZES[indirection]; // shift down the pointer to the indirection table as we slide through it
    }
}

void INode::release_chunks() {
    fprintf(stdout, "INode is releasing its allocated chunks: free'd chunks... ");
    uint64_t rough_chunk_count = this->data.file_size / this->superblock->disk->chunk_size() + 1;
    for (size_t idx = 0; idx < rough_chunk_count; ++idx) {
        std::shared_ptr<Chunk> chunk = resolve_indirection(idx, false);
        if (chunk == nullptr) 
            continue ;
        fprintf(stdout, "%d, ", chunk->chunk_idx);
        this->superblock->segment_controller.free_chunk(std::move(chunk));
    }
    fprintf(stdout, ".\n");
}

std::string INode::to_string() {
    std::stringstream out;
    out << "INODE... " << std::endl;
    for(int i = 0; i < ADDRESS_COUNT; i++) {
        out << i << ": " << data.addresses[i] << std::endl;
    }
    out << "END INODE" << std::endl;
    return out.str();
}

INodeTable::INodeTable(SuperBlock *superblock, uint64_t offset, uint64_t inode_count) : superblock(superblock) {
    this->inode_count = inode_count;
    this->inode_table_offset = offset;
    this->inodes_per_chunk = superblock->disk_chunk_size / sizeof(INode::INodeData);

    // allocate this at the beginning of the table 
    this->used_inodes = std::unique_ptr<DiskBitMap>(
        new DiskBitMap(superblock->disk, this->inode_table_offset, inode_count)
    );
    
    this->inode_ilist_offset = this->inode_table_offset + this->used_inodes->size_chunks();
    
    
    this->inode_table_size_chunks = this->used_inodes->size_chunks() + inode_count / inodes_per_chunk + 1;
}

void INodeTable::format_inode_table() {
    // no inodes are used initially
    this->used_inodes->clear_all();
}

// returns the size of the entire table in chunks
uint64_t INodeTable::size_chunks() {
    return inode_table_size_chunks;
}

std::shared_ptr<INode> INodeTable::alloc_inode() {
    std::lock_guard<std::recursive_mutex> g(this->lock);

    DiskBitMap::BitRange range = this->used_inodes->find_unset_bits(1);
    if (range.bit_count != 1) {
        throw FileSystemException("INodeTable out of inodes -- no free inode available for allocation");
    }
    
    std::shared_ptr<INode> inode(new INode);
    inode->superblock = this->superblock;
    inode->inode_table_idx = range.start_idx;

    this->inodecache.put(inode->inode_table_idx, inode); 
    used_inodes->set(inode->inode_table_idx);
    
    return inode;
}

std::shared_ptr<INode> INodeTable::get_inode(uint64_t idx) {
    std::lock_guard<std::recursive_mutex> g(this->lock);

    if (idx >= inode_count) 
        throw FileSystemException("INode index out of bounds");
    if (!used_inodes->get(idx)) 
        throw FileSystemException("INode at index is not currently in use. You can't have it.");
    
    if (auto inode = this->inodecache.get(idx)) {
        return inode;
    }

    std::shared_ptr<INode> inode(new INode);
    uint64_t chunk_idx = inode_ilist_offset + idx / inodes_per_chunk;
    uint64_t chunk_offset = idx % inodes_per_chunk;
    std::shared_ptr<Chunk> chunk = superblock->disk->get_chunk(chunk_idx);
    std::memcpy((void *)(&(inode->data)), chunk->data + sizeof(INode::INodeData) * chunk_offset, sizeof(INode::INodeData));
    inode->superblock = this->superblock;
    inode->inode_table_idx = idx;
    return inode;
}

void INodeTable::update_inode(const INode& inode) {
    std::lock_guard<std::recursive_mutex> g(this->lock);
    
    if (inode.inode_table_idx >= inode_count) 
        throw FileSystemException("INode index out of bounds");
    if (!used_inodes->get(inode.inode_table_idx)) 
        throw FileSystemException("INode at index is not currently in use. You can not update it.");

    uint64_t chunk_idx = inode_ilist_offset + inode.inode_table_idx / inodes_per_chunk;
    uint64_t chunk_offset = inode.inode_table_idx % inodes_per_chunk;
    std::shared_ptr<Chunk> chunk = superblock->disk->get_chunk(chunk_idx);

    assert((Byte *)(chunk->data + sizeof(INode::INodeData) * chunk_offset + sizeof(INode::INodeData)) < chunk->data + chunk->size_bytes);

    chunk->memcpy((void *)(chunk->data + sizeof(INode::INodeData) * chunk_offset), (void *)(&(inode.data)), sizeof(INode::INodeData));
}

void INodeTable::free_inode(std::shared_ptr<INode> inode) {
    std::lock_guard<std::recursive_mutex> g(this->lock);

    if (!inode.unique()) {
        throw FileSystemException("To free an inode you must hand a UNIQUE reference that no other thread currently holds to free_inode");
        // you may optionally spin until you can acquire a unique reference to the inode in order to remove it
    }

    if (inode->inode_table_idx >= inode_count) 
        throw FileSystemException("INode index out of bounds");
    
    uint64_t index = inode->inode_table_idx;
    inode = nullptr;

    used_inodes->clr(index);
}

SuperBlock::SuperBlock(Disk *disk) 
    : disk(disk), disk_size_bytes(disk->size_bytes()), 
    disk_size_chunks(disk->size_chunks()),
    disk_chunk_size(disk->chunk_size()) {
}

void SuperBlock::init(double inode_table_size_rel_to_disk) {
    uint64_t offset = this->superblock_size_chunks; // sspace reserved for the superblock's header

    if (this->disk->size_chunks() < 16 || disk->size_chunks() * (1.0 - inode_table_size_rel_to_disk) < 16) {
        throw new FileSystemException("Requested size of superblock, inode table, and bitmap will potentially exceed disk size");
    }

    // zero out the disk
    // this->disk->zero_fill();

    // initialize the disk block map
    {
        this->disk_block_map = std::unique_ptr<DiskBitMap>(
            new DiskBitMap(this->disk, offset, disk->size_chunks()));
        this->disk_block_map->clear_all();
        // set the properties on the superblock for the blockmap
        this->disk_block_map_offset = offset;
        this->disk_block_map_size_chunks = this->disk_block_map->size_chunks();
        offset += this->disk_block_map->size_chunks();
    }
    
    // initialize the inode table
    {   
        uint64_t inodes_per_chunk = disk->chunk_size() / sizeof(INode);
        uint64_t inode_count_to_request = (unsigned long)(inode_table_size_rel_to_disk * disk->size_chunks()) * inodes_per_chunk;
        
        this->inode_table_inode_count = inode_count_to_request;
        this->inode_table = std::unique_ptr<INodeTable>(
            new INodeTable(this, offset, inode_count_to_request));
        this->inode_table->format_inode_table();
        this->inode_table_offset = offset;
        this->inode_table_size_chunks = this->inode_table->size_chunks();
        offset += this->inode_table->size_chunks();
    }

    // give ourselves an extra margin of 1 chunk
    offset++;

    //set all metadata chunk bits to `used' a la Thomas
    for(uint64_t bit_i = 0; bit_i < offset; ++bit_i) {
        disk_block_map->set(bit_i);
    }

    this->data_offset = offset;

     //segment the free data block space
    this->num_segments = 0;
    segment_size_chunks = 2 * (disk_chunk_size / sizeof(uint64_t));
    while(this->num_segments < 20) {
        segment_size_chunks /= 2;
        this->num_segments = (disk_size_chunks - data_offset - 1) / segment_size_chunks;
    }

    if (this->num_segments == 0) {
        throw FileSystemException("Num segments is equal to zero, this should never happen.");
    }

    segment_controller.disk = disk;
    segment_controller.superblock = this;
    segment_controller.data_offset = data_offset;
    segment_controller.segment_size = segment_size_chunks;
    segment_controller.num_segments = num_segments;
    segment_controller.free_segment_stat_offset = 13;
    segment_controller.clear_all_segments();
    segment_controller.set_new_free_segment();

    // fprintf(stdout, "loaded segment_controller with options:\n"
    //     "\tdata offset: %llu\n" 
    //     "\tsegment_size: %llu\n"
    //     "\tnum_segments: %llu\n",
    //     segment_controller.data_offset, 
    //     segment_controller.segment_size,
    //     segment_controller.num_segments);

    //setup root directory
    std::shared_ptr<INode> inode = this->inode_table->alloc_inode();
    IDirectory root_dir(*inode);
    root_dir.initializeEmpty();
    root_dir.add_file(".", *inode);
    root_dir.add_file("..", *inode);
    inode->set_type(S_IFDIR);
    this->root_inode_index = inode->inode_table_idx;
    //serialize to disk
    {
        auto sb_chunk = disk->get_chunk(0);
        Byte* sb_data = sb_chunk->data;
        uint64_t *data_slots = (uint64_t *)sb_data;

        uint64_t offset = 0;

        data_slots[0] = superblock_size_chunks;
        data_slots[1] = disk_size_bytes;
        data_slots[2] = disk_size_chunks;
        data_slots[3] = disk_chunk_size;
        data_slots[4] = disk_block_map_offset;
        data_slots[5] = disk_block_map_size_chunks;
        data_slots[6] = inode_table_offset;
        data_slots[7] = inode_table_size_chunks;
        data_slots[8] = inode_table_inode_count;
        data_slots[9] = data_offset;
        data_slots[10] = segment_size_chunks;
        data_slots[11] = this->num_segments;
        data_slots[12] = root_inode_index;
        //the segment controller will be able to write to this offset on disk, currently 13
        data_slots[segment_controller.free_segment_stat_offset] = segment_controller.num_free_segments;

        disk->flush_chunk(*sb_chunk);
    }
}

void SuperBlock::load_from_disk() {
    std::cout << "ENTERING LOAD_FROM_DISK" << std::endl;
    auto sb_chunk = disk->get_chunk(0);
    auto sb_data = sb_chunk->data;
    uint64_t *data_slots = (uint64_t *)sb_data;
    //superblock_size_chunks = *(uint64_t *)(sb_data + offset);
    //TODO: throw an error code that filesystem was corrupted instead /////////////////////////////////////////////

    assert(superblock_size_chunks == data_slots[0]);
    assert(disk_size_bytes == data_slots[1]);
    assert(disk_size_chunks == data_slots[2]);
    assert(disk_chunk_size == data_slots[3]);
    disk_block_map_offset = data_slots[4];
    disk_block_map_size_chunks = data_slots[5];
    inode_table_offset = data_slots[6];
    inode_table_size_chunks = data_slots[7];
    inode_table_inode_count = data_slots[8];
    data_offset = data_slots[9];
    segment_size_chunks = data_slots[10];
    this->num_segments = data_slots[11];
    root_inode_index = data_slots[12];


    std::cout << "We don't need to do this next part, but here we go" << std::endl;

    // initialize the disk block map
    {
        this->disk_block_map = std::unique_ptr<DiskBitMap>(
            new DiskBitMap(this->disk, this->disk_block_map_offset, disk->size_chunks()));
        // this->disk_block_map->clear_all();
        // set the properties on the superblock for the blockmap
        assert(this->disk_block_map_size_chunks == this->disk_block_map->size_chunks());

        if (this->disk_block_map_offset != disk_block_map_offset || 
            this->disk_block_map_size_chunks != disk_block_map_size_chunks) {
            throw FileSystemException("The disk blockmap became corrupted when attempting to load it");
        }
    }
    
    std::cout << "continuing with stuff we do need, inodes" << std::endl;

    // initialize the inode table
    {
        this->inode_table = std::unique_ptr<INodeTable>(
            new INodeTable(this, this->inode_table_offset, inode_table_inode_count));
        // this->inode_table->format_inode_table();
        this->inode_table_size_chunks = this->inode_table->size_chunks();

        if (this->inode_table_offset != this->inode_table->inode_table_offset || 
            this->inode_table_size_chunks != this->inode_table->size_chunks()) {
            throw FileSystemException("The inode table became corrupted when attempting to load it");
        }
    }

    std::cout << "Initializing segment controller" << std::endl;

    assert(this->num_segments != 0);
    // initialize the segment controller 
    segment_controller.disk = disk;
    segment_controller.superblock = this;
    segment_controller.data_offset = this->data_offset;
    segment_controller.segment_size = segment_size_chunks;
    segment_controller.num_segments = this->num_segments;
    segment_controller.free_segment_stat_offset = 13;
    segment_controller.num_free_segments = data_slots[segment_controller.free_segment_stat_offset];

    //Attempt at per segment locking
    /*for(int i = 0; i < num_segments; i++) {
        segment_controller.single_segment_locks.emplace_back();
    }*/

    //prepare for writes!
    segment_controller.set_new_free_segment();

    // fprintf(stdout, "loaded segment_controller with options:\n"
    //     "\tdata offset: %llu\n" 
    //     "\tsegment_size: %llu\n"
    //     "\tnum_segments: %llu\n",
    //     segment_controller.data_offset, 
    //     segment_controller.segment_size,
    //     segment_controller.num_segments);

    // finally, these two values should add up
    uint64_t offset = this->data_offset;

    // also check that the disk bit map marks every chunk up to the data offset as in use
    for(uint64_t bit_i = 0; bit_i < offset; ++bit_i) {
        if (!disk_block_map->get(bit_i)) {
            throw FileSystemException("disk bit map should hold every bit in superblock marked as 'in use' why is this not the case?");
        }
    }

    std::cout << "EXITING LOAD FROM DISK" << std::endl;
}

void FileSystem::printForDebug() {
  //TODO: write this function
  throw FileSystemException("thomas you idiot...");
}


/*
    DIRECTORY IMPLEMENTATION
*/

// IDirectory::IDirectory(INode &inode) : inode(&inode) {
//     this->inode->read(0, (char *)&header, sizeof(DirHeader));
// }

// uint64_t IDirectory::DirEntry::read_from_disk(size_t offset) {
//     this->offset = offset;

//     this->inode->read(offset, (char *)(&(this->data)), sizeof(DirEntryData));
//     offset += sizeof(DirEntryData);
    
//     if (this->filename != nullptr) {
//         free(this->filename);
//     }
//     this->filename = (char *)malloc(this->data.filename_length + 1);
//     std::memset(this->filename, 0, this->data.filename_length + 1);
//     this->inode->read(offset, this->filename, data.filename_length);
//     offset += this->data.filename_length;

//     return offset;
// }

// uint64_t IDirectory::DirEntry::write_to_disk(size_t offset, const char *filename) {
//     this->offset = offset;
//     this->inode->write(offset, (char *)(&(this->data)), sizeof(DirEntryData));
//     offset += sizeof(DirEntryData);
    
//     if (filename != nullptr) {
//         assert(data.filename_length != 0);
//         assert(data.filename_length == strlen(filename));
//         this->inode->write(offset, filename, data.filename_length);
//     }

//     offset += data.filename_length;

//     return offset;
// }

// void IDirectory::flush() { // flush your changes 
//     inode->write(0, (char *)&header, sizeof(DirHeader));
// }

// void IDirectory::initializeEmpty() {
//     header = DirHeader();
//     inode->write(0, (char *)&header, sizeof(DirHeader));
// }

// std::unique_ptr<IDirectory::DirEntry> IDirectory::add_file(const char *filename, const INode &child) {
//     if (this->get_file(filename) != nullptr) {
//         return nullptr;
//     }

//     if (header.dir_entries_head == 0) {
//         // then it is the first and only element in the linked list!
//         std::unique_ptr<DirEntry> entry(new DirEntry(this->inode));
//         entry->data.filename_length = strlen(filename);
//         entry->data.inode_idx = child.inode_table_idx;
//         entry->filename = strdup(filename);
        
//         // returns the offset after the write of the entry
//         size_t next_offset = entry->write_to_disk(sizeof(DirHeader), entry->filename);
//         header.dir_entries_head = sizeof(DirHeader);
//         header.dir_entries_tail = sizeof(DirHeader);
//         header.record_count++;
//         // finally, flush ourselves to the disk
        
//         this->flush();
//         return std::move(entry);
//     } else {

//         DirEntry last_entry(this->inode);
//         size_t next_offset = last_entry.read_from_disk(header.dir_entries_tail);
//         last_entry.data.next_entry_ptr = next_offset;
//         last_entry.write_to_disk(header.dir_entries_tail, nullptr);

//         std::unique_ptr<DirEntry> new_entry(new DirEntry(this->inode));
//         new_entry->data.filename_length = strlen(filename);
//         new_entry->data.inode_idx = child.inode_table_idx;
//         new_entry->filename = strdup(filename);
//         new_entry->write_to_disk(next_offset, new_entry->filename);

//         header.dir_entries_tail = next_offset;
//         header.record_count++;

//         this->flush();
//         return std::move(new_entry);
//     }
// }

// std::unique_ptr<IDirectory::DirEntry> IDirectory::get_file(const char *filename) {
//     std::unique_ptr<DirEntry> entry = nullptr;

//     while (entry = this->next_entry(entry)) {
//         if (strcmp(entry->filename, filename) == 0) {
//             return entry;
//         }
//     }

//     return nullptr;
// }

// std::unique_ptr<IDirectory::DirEntry> IDirectory::remove_file(const char *filename) {
//     std::unique_ptr<DirEntry> last_entry = nullptr;
//     std::unique_ptr<DirEntry> entry = nullptr;

//     size_t count = 0;
//     while (true) {
//         last_entry = std::move(entry);
//         entry = this->next_entry(last_entry);
//         if (entry == nullptr)
//             break ;

//         if (strcmp(entry->filename, filename) == 0) {

//             if (last_entry == nullptr) {
//                 header.dir_entries_head = entry->data.next_entry_ptr;
//                 if (entry->data.next_entry_ptr == 0) {
//                     header.dir_entries_tail = 0;
//                 }
//             } else {
//                 last_entry->data.next_entry_ptr = entry->data.next_entry_ptr;
//                 last_entry->write_to_disk(last_entry->offset, nullptr);

//                 if (last_entry->data.next_entry_ptr == 0) {
//                     header.dir_entries_tail = last_entry->offset;
//                 }
//             }
            
//             header.deleted_record_count++;
//             header.record_count--;
//             this->flush(); // make sure we flush out the changes to the header
//             return entry;
//         }

//         if (count++ > 50) {
//             break ;
//         }
//     }

//     return nullptr;
// }

// std::unique_ptr<IDirectory::DirEntry> IDirectory::next_entry(const std::unique_ptr<IDirectory::DirEntry>& entry) {
//     std::unique_ptr<DirEntry> next(new DirEntry(this->inode));
//     if (entry == nullptr) {
//         if (header.record_count == 0)
//             return nullptr;

//         next->read_from_disk(this->header.dir_entries_head);
//     } else {
//         if (entry->data.next_entry_ptr == 0) 
//             return nullptr; // reached the end of the linked list

//         next->read_from_disk(entry->data.next_entry_ptr);
//     }

//     return next;
// }

/*
    IDirectory reimplementation
*/
struct RawRecord {
    uint64_t flag;
    char filename[255];
    uint64_t inode_idx;
};

IDirectory::IDirectory(INode &inode) : inode(&inode) {
}

void IDirectory::initializeEmpty() {
}

std::unique_ptr<IDirectory::DirEntry> IDirectory::add_file(const char *filename, const INode &child) {
    const uint64_t file_size_bytes = inode->data.file_size;
    const uint64_t file_size_records = file_size_bytes / sizeof(RawRecord);
    assert(file_size_bytes % sizeof(RawRecord) == 0);
    std::unique_ptr<Byte[]> read_buffer_unique(new Byte[file_size_bytes]);
    Byte* read_buffer = read_buffer_unique.get();
    
    RawRecord write_record;
    //read in the entire directory to memory
    this->inode->read(0, (char *)read_buffer, file_size_bytes);
    bool found_slot = false;
    uint64_t index = 0;

    //locate an empty record slot, if any
    for(index = 0; index < file_size_records; index++) {
        if(read_buffer[index * sizeof(RawRecord)] == 0) {
            found_slot = true;
            break;
        }
    }

    //create the new record in memory
    write_record.flag = 1;
    strncpy(write_record.filename, filename, 255);
    write_record.inode_idx = child.inode_table_idx;

    //write the new record to the found slot, or to a newly appended slot
    if(found_slot) {
        inode->write(index * sizeof(RawRecord), (const char *)&write_record, sizeof(RawRecord));
    } else {
        inode->write(file_size_bytes, (const char *)&write_record, sizeof(RawRecord));
    }

    //return a dir entry
    std::unique_ptr<DirEntry> ret(new DirEntry);
    ret->filename = std::string(filename, 0, 255);
    ret->inode_idx = child.inode_table_idx;

    return ret;
}

std::unique_ptr<IDirectory::DirEntry> IDirectory::get_file(const char *filename) {
    const uint64_t file_size_bytes = inode->data.file_size;
    const uint64_t file_size_records = file_size_bytes / sizeof(RawRecord);
    std::unique_ptr<Byte[]> read_buffer_unique(new Byte[file_size_bytes]);
    RawRecord* records = (RawRecord *)read_buffer_unique.get();

    //read in the entire directory to memory
    inode->read(0, (char *)records, file_size_bytes);

    //locate the file, if exists
    for(uint64_t index = 0; index < file_size_records; index++) {
        if(records[index].flag != 0 && strncmp(records[index].filename, filename, 255) == 0) {
            std::unique_ptr<DirEntry> ret(new DirEntry);
            ret->filename = std::string(records[index].filename, 0, 255);
            ret->inode_idx = records[index].inode_idx;
            return ret;
        }
    }

    return nullptr;
}

std::unique_ptr<IDirectory::DirEntry> IDirectory::remove_file(const char *filename) {
    const uint64_t file_size_bytes = inode->data.file_size;
    const uint64_t file_size_records = file_size_bytes / sizeof(RawRecord);
    std::unique_ptr<Byte[]> read_buffer_unique(new Byte[file_size_bytes]);
    RawRecord* records = (RawRecord *)read_buffer_unique.get();

    //read in the entire directory to memory
    inode->read(0, (char *)records, file_size_bytes);

    //locate the file, if exists
    for(uint64_t index = 0; index < file_size_records; index++) {
        if(records[index].flag != 0 && strncmp(records[index].filename, filename, 255) == 0) {
            std::unique_ptr<DirEntry> ret(new DirEntry);
            ret->filename = std::string(records[index].filename, 0, 255);
            ret->inode_idx = records[index].inode_idx;
            records[index].flag = 0;
            inode->write(sizeof(RawRecord) * index, (char *)&(records[index]), sizeof(RawRecord));
            return ret;
        }
    }

    return nullptr;
}

std::vector< std::unique_ptr<IDirectory::DirEntry> > IDirectory::get_files() {
    std::vector<std::unique_ptr<IDirectory::DirEntry> > out;
    const uint64_t file_size_bytes = inode->data.file_size;
    const uint64_t file_size_records = file_size_bytes / sizeof(RawRecord);
    std::unique_ptr<Byte[]> read_buffer_unique(new Byte[file_size_bytes]);
    RawRecord* records = (RawRecord *)read_buffer_unique.get();

    //read in the entire directory to memory
    inode->read(0, (char *)records, file_size_bytes);

    //locate the file, if exists
    for(uint64_t index = 0; index < file_size_records; index++) {
        if(records[index].flag != 0) {
            std::unique_ptr<DirEntry> ret(new DirEntry);
            ret->filename = std::string(records[index].filename, 0, 255);
            ret->inode_idx = records[index].inode_idx;
            out.push_back(std::move(ret));
        }
    }
    
    return out;
}


/*
* Segment Controller
*/

uint64_t SegmentController::get_segment_usage(uint64_t segment_number) {
    std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + segment_number * segment_size);
    return *((uint64_t*)chunk->data);
}

void SegmentController::set_segment_usage(uint64_t segment_number, uint64_t segment_usage) {
    assert(segment_usage <= segment_size);
    std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + segment_number * segment_size);

    uint64_t old_usage = *((uint64_t*)chunk->data);
    if (old_usage == 0 && segment_usage != 0) {
        num_free_segments--;
        std::shared_ptr<Chunk> chunk = disk->get_chunk(0);
        ((uint64_t*)chunk->data)[free_segment_stat_offset] = num_free_segments;
    } else if (old_usage != 0 && segment_usage == 0) {
        num_free_segments++;
        std::shared_ptr<Chunk> chunk = disk->get_chunk(0);
        ((uint64_t*)chunk->data)[free_segment_stat_offset] = num_free_segments;
    }
    *((uint64_t*)chunk->data) = segment_usage;
}

uint64_t SegmentController::get_segment_chunk_to_inode(uint64_t segment_number, uint64_t chunk_number) {
    std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + segment_number * segment_size);
    return ((uint64_t*)chunk->data)[chunk_number];
}

void SegmentController::set_segment_chunk_to_inode(uint64_t segment_number, uint64_t chunk_number, uint64_t inode_number) {
    assert(inode_number <= superblock->inode_table_inode_count);
    std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + segment_number * segment_size);
    ((uint64_t*)chunk->data)[chunk_number] = inode_number;
}

void SegmentController::clear_all_segments() {
    for(int i = 0; i < num_segments; i++) {
        std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + i * segment_size);
        std::memset(chunk->data, 0, chunk->size_bytes);
        //std::cout << "Zeroed " << i << " of " << num_segments << " segments" << std::endl;
    }
    num_free_segments = num_segments;
    std::shared_ptr<Chunk> chunk = disk->get_chunk(0);
    ((uint64_t*)chunk->data)[free_segment_stat_offset] = num_free_segments;
}

//Find a new free segment
void SegmentController::set_new_free_segment() {
    for(int i = 0; i < num_segments; i++) {
        if(get_segment_usage(i) == 0) {
            current_segment = i;
            current_chunk = 1;
            return;
        }
    }
    current_segment = -1;
}

void SegmentController::clean() {
    // lock the segment controller
    std::lock_guard<std::mutex> lock(segment_controller_lock);

    std::vector<uint64_t> segments_to_clean;
    //initialized poorly so we catch later
    uint64_t new_segment1 = num_segments + 1;
    uint64_t new_segment2 = num_segments + 1;

    int i = 0;

    if(num_free_segments == 0) {
        return ;
    }

    //get two clean segments
    for(i; i < num_segments; i++) {
        if(get_segment_usage(i) == 0 && current_segment != i) {
            new_segment1 = i;
            break;
        }
    }
    i += 1;
    for(i; i < num_segments; i++) {
        if(get_segment_usage(i) == 0 && current_segment != i) {
            new_segment2 = i;
            break;
        }
    }

    //Fail silently when disk is almost full
    if(new_segment1 > num_segments || new_segment2 > num_segments) {
        return ;
    }

    uint64_t num_chunks_to_combine = 0;
    i = 0;
    std::cout << std::endl << std::endl << std::endl;
    while(i < num_segments) {
        //only grab non empty segments to clean
        uint64_t usage = get_segment_usage(i);
        std::cout << usage << " ";
        if(usage != 0 && usage != segment_size - 1 && current_segment != i) {
            //try to add this segment to the clean up list
            //Remember to reserve one chunk for meta data
            if(num_chunks_to_combine + usage <= 2 * (segment_size - 1) ) {
                segments_to_clean.push_back(i);
                num_chunks_to_combine += usage;
            } else {
                break;
                // //if it doesn't fit, break if we have more than one segment to clean
                // if(segments_to_clean.size() > 1) {
                //     break;
                // } else {
                //     //otherwise keep the smaller of the two and look for more things to clean.
                //     if(usage < num_chunks_to_combine) {
                //         num_chunks_to_combine = usage;
                //         segments_to_clean.pop_back();
                //         segments_to_clean.push_back(i);
                //     }
                // }
            }
        }
        i++;
    }
    std::cout << std::endl << std::endl << std::endl;
    
    //=================================================== DEBUG ===================================================
    std::cout << "ATTEMPTING TO CLEAN SEGMENTS ";
    for(uint64_t sn : segments_to_clean) {
        std::cout << sn << ", ";
    }
    std::cout << std::endl;
    std::cout << "TOTAL USAGE TO BE CLEANED: " << num_chunks_to_combine << std::endl;
    std::cout << "USAGES: ";
    for(uint64_t sn : segments_to_clean) {
        std::cout << get_segment_usage(sn) << ", ";
    }
    std::cout << std::endl;
    std::cout << "CLEANING INTO SEGMENTS " << new_segment1 << ", " << new_segment2 << ", usage = " << get_segment_usage(new_segment1) << get_segment_usage(new_segment2) << std::endl;
    //=============================================================================================================

    //Fail silently if we could not find at least two segments to clean into one
    //This can occur when we still have free segments available, wait to fail until unable to set a new free segment for writing
    if(segments_to_clean.size() <= 1) {
        std::cout << "NOTHING TO CLEAN" << std::endl;
        throw FileSystemException("Disk is full");
        return ;
    }

    assert(num_chunks_to_combine > 0);
    assert(num_chunks_to_combine <= 2 * (segment_size - 1));

    //create the new segment first
    uint64_t usage1 = num_chunks_to_combine;
    if(usage1 > segment_size - 1) {
        usage1 = segment_size - 1;
    }
    uint64_t usage2 = num_chunks_to_combine - usage1;

    set_segment_usage(new_segment1, usage1);
    set_segment_usage(new_segment2, usage2);
    uint64_t write_head = 1;

    uint64_t current_new_segment = new_segment1;

    //track which inodes are touched
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t> > inode_changes_to_apply;

    for(uint64_t sn : segments_to_clean) {
        //hold this for performance
        //std::shared_ptr<Chunk> metadata_chunk = disk->get_chunk(data_offset + sn * segment_size);

        //loop over the segment and grab all of the actual data
        //std::cout << "STARTING ON SEGMENT " << sn << std::endl;
        for(uint64_t cn = 1; cn < segment_size; cn++) {
            uint64_t inode_num = get_segment_chunk_to_inode(sn, cn);
            //std::cout << "\tCHUNK " << cn << " OWNED BY " << inode_num << std::endl;
            if(inode_num != 0) {
                //check if we need to switch free segments
                if(write_head == usage1 + 1) {
                    write_head = 1;
                    current_new_segment = new_segment2;
                }
                //update the inode mapping in the new segment
                set_segment_chunk_to_inode(current_new_segment, write_head, inode_num);
                //copy the data over
                uint64_t abs_old_chunk_idx = data_offset + sn * segment_size + cn;
                uint64_t abs_new_chunk_idx = data_offset + current_new_segment * segment_size + write_head;
                std::shared_ptr<Chunk> to_read = disk->get_chunk(abs_old_chunk_idx);
                std::shared_ptr<Chunk> to_write = disk->get_chunk(abs_new_chunk_idx);
                to_write->memcpy((void*)to_write->data, (void*)to_read->data, to_read->size_bytes);

                //add the inode remapping to our to do list
                inode_changes_to_apply[inode_num][abs_old_chunk_idx] = abs_new_chunk_idx;

                //go to the next chunk
                write_head += 1;
            }
        }
    }

    //update pointers
    for(auto & thing : inode_changes_to_apply) {
        superblock->inode_table->get_inode(thing.first)->update_chunk_locations(thing.second);
    }

    //remove the old data
    for(uint64_t sn : segments_to_clean) {
        set_segment_usage(sn, 0);
        std::shared_ptr<Chunk> chunk = disk->get_chunk(data_offset + sn * segment_size);
        chunk->memset(chunk->data, 0, chunk->size_bytes);
    }

    std::cout << "SEGMENT " << new_segment1 << " USAGE NOW " << get_segment_usage(new_segment1) << std::endl;
    std::cout << "SEGMENT " << new_segment2 << " USAGE NOW " << get_segment_usage(new_segment2) << std::endl;
    std::cout << "OTHER USAGES NOW ";
    for(uint64_t sn : segments_to_clean) {
        std::cout << get_segment_usage(sn) << ", ";
    }
    std::cout << std::endl;

    std::cout << "FREE SEGMENTS NOW " << num_free_segments << std::endl;
}

uint64_t SegmentController::alloc_next(uint64_t inode_number) {
    assert(inode_number <= superblock->inode_table_inode_count);

    //lock the segment controller, releases automatically at function exit
    std::lock_guard<std::mutex> lock(segment_controller_lock);

    //make sure we still have chunks available in this segment
    if(current_chunk == segment_size) {
        set_new_free_segment();
    }

    //throw exception if disk full
    if(current_segment == -1) {
        throw FileSystemException("FileSystem out of space -- unable to allocate a new chunk");
    }

    //increment segment usage
    uint64_t usage = get_segment_usage(current_segment);
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

void SegmentController::free_chunk(std::shared_ptr<Chunk> chunk_to_free) {
    if (!chunk_to_free.unique()) {
        throw FileSystemException("FileSystem free chunk failed -- the chunk passed was not 'unique', something else is using it");
    }
    // lock the segment controller
    std::lock_guard<std::mutex> lock(segment_controller_lock);
    // get the segment and relative chunk number
    uint64_t segment_number = (chunk_to_free->chunk_idx - data_offset) / segment_size;
    assert(segment_number < this->num_segments);
    uint64_t chunk_number = chunk_to_free->chunk_idx - (segment_number * segment_size) - data_offset;
    assert(chunk_number < segment_size);
    //clear the inode mapping (Gareth's comment: clears the mapping from chunks in the segment to the inodes that reference them)
    set_segment_chunk_to_inode(segment_number, chunk_number, 0);
    //decrement segment usage
    uint64_t usage = get_segment_usage(segment_number);
    set_segment_usage(segment_number, usage - 1);
}
