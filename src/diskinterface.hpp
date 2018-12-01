#ifndef DISKINTERFACE_HPP
#define DISKINTERFACE_HPP

#include <stdint.h>
#include <mutex>
#include <unordered_map>
#include <string>
#include <vector>
#include <array>
#include <iostream>
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>

#include <cstring>
#include <memory>

typedef uint8_t Byte;
typedef uint64_t Size;

class Disk;

struct StorageException : public std::exception {
	const std::string message;
	StorageException(const std::string &message) : message(message) { };
};

struct DiskException : public StorageException {
	DiskException(const std::string &message) : StorageException(message) { };
};

struct Chunk {
	Disk *parent = nullptr;

	std::mutex lock;
	size_t size_bytes = 0;
	size_t chunk_idx = 0;
	Byte *data = nullptr;

	~Chunk();
};


template<typename K, typename V>
class SharedObjectCache {
private:
	size_t size_next_sweep = 16;
	std::unordered_map<K, std::weak_ptr<V>> map;
public:
	void sweep(bool force) {
		if (!force && map.size() < size_next_sweep)
			return ;

		for (auto it = this->map.cbegin(); it != this->map.cend();){
			if ((*it).second.expired()) {
				this->map.erase(it++);    // or "it = m.erase(it)" since C++11
			} else {
				++it;
			}
		}

		size_next_sweep = this->map.size() < 16 ? 16 : this->map.size();
	}

	void put(const K& k, std::weak_ptr<V> v) {
		map[k] = std::move(v);
		this->sweep(false);
	}

	std::shared_ptr<V> get(const K& k) {
		auto ref = this->map.find(k);
		if (ref != this->map.end()) {
			if (std::shared_ptr<V> v = (*ref).second.lock()) {
				return std::move(v);
			}
		} 

		return nullptr;
	}

	inline size_t size() {
		return this->map.size();
	}
};

/*
	acts as an interface onto the disk as well as a cache for chunks on disk
	in this way the same chunk can be accessed and modified in multiple places
	at the same time if this is desirable
*/
class Disk {
private:
	// properties of the class
	const Size _size_chunks;
	const Size _chunk_size;
	const size_t _mempage_size = sysconf(_SC_PAGESIZE); // get the memory page size;

	Byte* data;

	// a mutex which protects access to the disk
	std::mutex lock;

	// a cache of chunks that are loaded in
	SharedObjectCache<Size, Chunk> chunk_cache;

	// loops over weak pointers, if any of them are expired, it deletes 
	// the entries from the unordered map 
	void sweep_chunk_cache(); 
public:

	// when you just want a disk use 
	// flags: MAP_PRIVATE | MAP_ANONYMOUS
	// when you want a disk backed by a file, provide a file descriptor and set 
	// flags: MAP_FILE | MAP_SHARED
	// a good explanation of these flags can be found here: https://www.gnu.org/software/hurd/glibc/mmap.html
	Disk(Size size_chunk_ctr, Size chunk_size_ctr, 
		int flags = MAP_PRIVATE | MAP_ANONYMOUS, int fd = -1) 
		: _chunk_size(chunk_size_ctr), _size_chunks(size_chunk_ctr) {
		
		this->data = (Byte *)mmap(NULL, this->size_bytes(), PROT_READ | PROT_WRITE, flags, fd, 0);
		if (this->data == NULL) {
			throw DiskException("failed to create the memory mapped file to back the disk");
		}
	}

	void zero_fill() {
		// zero out the memory mapped file
		std::memset(this->data, 0, this->size_bytes());
	}

	inline const Size size_bytes() const {
		return _size_chunks * _chunk_size;
	}

	inline const Size size_chunks() const {
		return _size_chunks;
	}

	inline const Size chunk_size() const {
		return _chunk_size;
	}

	std::shared_ptr<Chunk> get_chunk(Size chunk_idx);

	void flush_chunk(const Chunk& chunk);

	void try_close();

	~Disk();
};

/*
	A utility class that implements a bitmap ontop of a range of chunks
*/
struct DiskBitMap {
	std::mutex block;

	Disk *disk;
	Size size_in_bits;
	std::vector<std::shared_ptr<Chunk>> chunks;
	Size last_search_idx = 0;
	Size disk_chunk_size = 0;
	
	DiskBitMap(Disk *disk, Size chunk_start, Size size_in_bits);

	~DiskBitMap();

	void clear_all();

	inline Size size_bytes() const {
		// add an extra byte which will be used for padding
		return size_in_bits / 8 + 8; // plenty of padding
	}

	inline Size size_chunks() const {
		return this->size_bytes() / disk_chunk_size + 1;
	}

	inline const Byte get_byte_for_idx(Size idx) const {
		uint64_t byte_idx = idx / 8;
		Byte *data = this->chunks[byte_idx / disk_chunk_size]->data;
		return data[byte_idx % disk_chunk_size];
	}

	inline Byte &get_byte_for_idx(Size idx) {
		uint64_t byte_idx = idx / 8;
		Byte *data = this->chunks[byte_idx / disk_chunk_size]->data;
		return data[byte_idx % disk_chunk_size];
	}

	inline bool get(Size idx) const {
		if (idx >= size_in_bits) {
			throw DiskException("BitMap index out of range");
		}
		Byte byte = get_byte_for_idx(idx);
		return byte & (1 << (idx % 8));
	}

	// allows setting 'out of bounds'
	inline void set_oob(Size idx) {
		Byte& byte = get_byte_for_idx(idx);
		byte |= (1 << (idx % 8));
	}

	inline void set(Size idx) {
		if (idx >= size_in_bits) {
			throw DiskException("BitMap index out of range");
		}
		Byte& byte = get_byte_for_idx(idx);
		byte |= (1 << (idx % 8));
	}

	inline void clr(Size idx) {
		if (idx >= size_in_bits) {
			throw DiskException("BitMap index out of range");
		}
		Byte& byte = get_byte_for_idx(idx);
		byte &= ~(1 << (idx % 8));
	}

	struct BitRange {
		Size start_idx = 0;
		Size bit_count = 0;

		void set_range(DiskBitMap &map) {
			for (Size idx = start_idx; idx < start_idx + bit_count; ++idx) {
				map.set(idx);
			}
		}

		void clr_range(DiskBitMap &map) {
			for (Size idx = start_idx; idx < start_idx + bit_count; ++idx) {
				map.clr(idx);
			}
		}
	};

	static std::array<BitRange, 256> find_unset_cache;
	static uint64_t find_last_byte_idx;
	
	BitRange find_unset_bits(Size length);
};


#endif
