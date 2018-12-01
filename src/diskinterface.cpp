#include <bitset>
#include <cassert>

#include "diskinterface.hpp"

Chunk::~Chunk() {
	// whenever the last reference to a chunk is released, we flush the chunk
	// out to the disk 
	this->parent->flush_chunk(*this);
}

std::shared_ptr<Chunk> Disk::get_chunk(Size chunk_idx) {
	std::lock_guard<std::mutex> g(lock); // acquire the lock

	if (chunk_idx > this->size_chunks()) {
		throw DiskException("chunk index out of bounds");
	}
	
	if (auto chunk_ref = this->chunk_cache.get(chunk_idx)) {
		return chunk_ref;
	}

	// initialize the new chunk
	std::shared_ptr<Chunk> chunk(new Chunk);
	chunk->parent = this; 
	chunk->size_bytes = this->chunk_size();
	chunk->chunk_idx = chunk_idx;
	chunk->data = this->data + chunk_idx * this->chunk_size();
	// chunk->data = std::unique_ptr<Byte[]>(new Byte[this->chunk_size()]);
	// std::memcpy(chunk->data.get(), this->data + chunk_idx * this->chunk_size(), 
	// 	this->chunk_size());

	// store it into the chunk cache so that it can be shared if requested again
	this->chunk_cache.put(chunk_idx, chunk); 
	return std::move(chunk);
}

void Disk::flush_chunk(const Chunk& chunk) {
	std::lock_guard<std::mutex> g(lock); // acquire the lock

	assert(chunk.size_bytes == this->chunk_size());
	assert(chunk.parent == this);

	// std::memcpy(this->data + chunk.chunk_idx * this->chunk_size(), 
	// 	chunk.data.get(), this->chunk_size());
	size_t chunk_addr = (size_t)chunk.data;
	chunk_addr &= ~(this->_mempage_size - 1);

	size_t page_count = this->_chunk_size / this->_mempage_size;
	if (this->_chunk_size % this->_mempage_size != 0) 
		page_count++;

	int sync_retval = msync((void *)chunk_addr, page_count * this->_mempage_size, MS_ASYNC);
	if (sync_retval != 0) {
		char buff[1024];
		sprintf(buff, "msync failed to synchronize the chunk segment with the disk, error code %d for chunk %d", sync_retval, chunk.chunk_idx);
		throw DiskException(buff);
	}
}

void Disk::try_close() {
	std::lock_guard<std::mutex> g(lock); // acquire the lock
	this->chunk_cache.sweep(true);
	if (this->chunk_cache.size() > 0) {
		throw DiskException("there are still chunks referenced in other parts of the program");
	}
}

Disk::~Disk() {
	if (this->data != NULL) {
		munmap(this->data, this->size_bytes());
	}
}



/*
	Disk Bit Map Methods
*/

DiskBitMap::DiskBitMap(Disk *disk, Size chunk_start, Size size_in_bits) {
	this->disk_chunk_size = disk->chunk_size();
	this->size_in_bits = size_in_bits;
	this->disk = disk;
	for (uint64_t idx = 0; idx < this->size_chunks(); ++idx) {
		auto chunk = disk->get_chunk(idx + chunk_start);
		chunk->lock.lock();
		this->chunks.push_back(std::move(chunk));
	}
}

DiskBitMap::~DiskBitMap() {
	for (auto &chunk : chunks) {
		chunk->lock.unlock();
	}
}

void DiskBitMap::clear_all() {
	for (std::shared_ptr<Chunk>& chunk : chunks) {
		std::memset(chunk->data, 0, chunk->size_bytes);
	}

	for (uint64_t idx = this->size_in_bits; idx < this->size_in_bits + 8; ++idx) {
		this->set_oob(idx);
	}
}


std::array<DiskBitMap::BitRange, 256> DiskBitMap::find_unset_cache;

static bool bitmap_init_cache(std::array<DiskBitMap::BitRange, 256>& cache) {
	for (Size idx = 0; idx < 256; ++idx) {
		std::bitset<8> byte(idx);

		for (Size j = 0; j < 8; ++j) {
			if (!byte[j]) {
				cache[idx].start_idx = j;
				Size k = 1;
				while (!byte[j + k] && j + k < 8) {
					k++;
				}
				cache[idx].bit_count = k;
				break;
			}
		}
	}
	return true;
}

static bool find_unset_cache_initialized = bitmap_init_cache(DiskBitMap::find_unset_cache);

DiskBitMap::BitRange DiskBitMap::find_unset_bits(Size length) {
	using BitRange = DiskBitMap::BitRange;
	
	// fprintf(stdout, "SCANNING BITMAP (SIZE IN BITS): %llu\n", this->size_in_bits);

	BitRange retval;
	for (Size idx = last_search_idx; idx < this->size_in_bits; idx += 8) {
		const Byte byte = (size_t)this->get_byte_for_idx(idx);
		BitRange res = find_unset_cache[byte];
		res.start_idx += idx;

		// fprintf(stdout, "SCANNING %d : %x\n", idx, byte);

		// if retval already set, the next set of bits must start immediately where the last one ends
		if (retval.bit_count != 0 && res.start_idx != retval.start_idx + retval.bit_count) {
			last_search_idx = idx;
			break ;
		}

		if (res.bit_count != 0) {
			if (retval.bit_count == 0) {
				retval = res;
			} else {
				retval.bit_count += res.bit_count;
			}
			
			if (retval.bit_count >= length) {
				last_search_idx = idx;
				break;
			}
		}
	}

	// bitcount should be limited to the length requested
	if (retval.bit_count > length) {
		retval.bit_count = length;
	}

	if (retval.bit_count == 0 && last_search_idx != 0) {
		this->last_search_idx = 0;
		return this->find_unset_bits(length);
	}

	return retval;
}

