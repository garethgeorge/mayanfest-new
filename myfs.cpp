/*
  see this for documentation on the methods https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201109/homework/fuse/fuse_doc.html
  
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall myfs.c `pkg-config fuse --cflags --libs` -o myfs
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
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
#include <signal.h>

#include "filesystem.hpp"

std::mutex lock_g;
std::unique_ptr<Disk> disk = nullptr;
std::unique_ptr<FileSystem> fs = nullptr;
SuperBlock *superblock = nullptr;

/*
	TODO: figure out why permissions are so incredibly broken right now 
	
*/


struct UnixError : public std::exception {
	const int errorcode;
	UnixError(int errorcode) : errorcode(errorcode) { };
};

bool can_read_inode(struct fuse_context *ctx, INode& inode) {
	// TODO: if we are privlidged ignore whether we are the owner of the file or not i.e. if we are root
	fprintf(stdout, "\tcan_read_inode(ctx.uid = %d, ctx.gid = %d, ctx.pid = %d, inode.data.permissions = %d, inode.data.uid = %d, inode.data.gid = %d)\n",
		ctx->uid, ctx->gid, ctx->pid,
		inode.data.permissions, inode.data.UID, inode.data.GID);
	if(ctx->uid == 0) return true;
	return 
		S_IROTH & inode.data.permissions || // anone can read
		((inode.data.UID == ctx->uid) && (S_IRUSR & inode.data.permissions)) || // owner can read
		((inode.data.GID == ctx->gid) && (S_IRGRP & inode.data.permissions)); // group can read
}

bool can_write_inode(struct fuse_context *ctx, INode& inode) {
	// TODO: if we are privlidged ignore whether we are the owner of the file or not i.e. if we are root
	fprintf(stdout, "\tcan_write_inode(ctx.uid = %d, ctx.gid = %d, ctx.pid = %d, inode.data.permissions = %d, inode.data.uid = %d, inode.data.gid = %d)\n",
		ctx->uid, ctx->gid, ctx->pid,
		inode.data.permissions, inode.data.UID, inode.data.GID);
	if(ctx->uid == 0) return true;
	return 
		S_IWOTH & inode.data.permissions || // anyone can write
		((inode.data.UID == ctx->uid) && (S_IWUSR & inode.data.permissions)) || // owner can write
		((inode.data.GID == ctx->gid) && (S_IWGRP & inode.data.permissions)); // group can write
}

bool can_exec_inode(struct fuse_context *ctx, INode& inode) {
	// TODO: if we are privlidged ignore whether we are the owner of the file or not i.e. if we are root
	fprintf(stdout, "\tcan_exec_inode(ctx.uid = %d, ctx.gid = %d, ctx.pid = %d, inode.data.permissions = %d, inode.data.uid = %d, inode.data.gid = %d)\n",
		ctx->uid, ctx->gid, ctx->pid,
		inode.data.permissions, inode.data.UID, inode.data.GID);
	return 
		S_IXOTH & inode.data.permissions || // anyone can exec
		((inode.data.UID == ctx->uid) && (S_IXUSR & inode.data.permissions)) || // owner can exec
		((inode.data.GID == ctx->gid) && (S_IXGRP & inode.data.permissions)); // group can exec
}

std::shared_ptr<INode> resolve_path(const char *path) {
	struct fuse_context *ctx = fuse_get_context();
	std::shared_ptr<INode> inode = superblock->inode_table->get_inode(superblock->root_inode_index);

	if (strlen(path) >= PATH_MAX) {
		throw UnixError(ENAMETOOLONG);
	}

	if (strcmp(path, "/") == 0) {
		// special case to handle root dir
		return inode;
	}

	assert(path[0] == '/');
	path += 1; // skip the / at the beginning 
	char path_segment[PATH_MAX]; // big buffer to hold segments of the path

	const char *seg_end = nullptr;
	while (seg_end = strstr(path, "/")) {
		if (inode->get_type() != S_IFDIR) {
			throw UnixError(ENOTDIR);
		}

		strncpy(path_segment, path, seg_end - path);
		path_segment[seg_end - path] = 0;

		IDirectory dir(*inode); // load the directory for the inode
		fprintf(stdout, "\ttrying to find path segment: %s\n", path_segment);
		std::unique_ptr<IDirectory::DirEntry> entry = dir.get_file(path_segment);
		if (entry == nullptr) {
			throw UnixError(ENOENT);
		}

		inode = superblock->inode_table->get_inode(entry->data.inode_idx);
		// if (!can_read_inode(ctx, *inode)) {
		// 	// this code might as well check that we have access to the path
		// 	fprintf(stdout, "resolve_path found that access is denied to this directory\n");
		// 	throw UnixError(EACCES);
		// }
		path = seg_end + 1;
	}

	IDirectory dir(*inode);
	std::unique_ptr<IDirectory::DirEntry> entry = dir.get_file(path);
	if (entry == nullptr)
		throw UnixError(ENOENT);

	return superblock->inode_table->get_inode(entry->data.inode_idx);
}

static int myfs_getattr(const char *path, struct stat *stbuf)
{
	struct fuse_context *ctx = fuse_get_context();
	fprintf(stdout, "myfs_getattr(%s, ...)\n", path);
	std::lock_guard<std::mutex> g(lock_g);
	int res = 0;
	try {
		std::shared_ptr<INode> inode;
		mode_t type;

		memset(stbuf, 0, sizeof(struct stat));
		inode = resolve_path(path);
		
		// stbuf->st_mode = inode->get_type() | inode->data.permissions;
		stbuf->st_mode = inode->get_type() | inode->data.permissions;
		stbuf->st_uid = inode->data.UID;
		stbuf->st_gid = inode->data.GID;
		stbuf->st_ino = inode->inode_table_idx;
		stbuf->st_size = inode->data.file_size;
		stbuf->st_nlink = 1;
		stbuf->st_atime = inode->data.last_accessed;
		stbuf->st_mtime = inode->data.last_modified;
	} catch (const UnixError &e) {
		fprintf(stdout, "\tmyfs_getattr encountered error %d\n", e.errorcode);
		return -e.errorcode;
	}
	//res = -ENOENT;

	return res;
}

static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
	std::lock_guard<std::mutex> g(lock_g);
	fprintf(stdout, "myfs_readdir(%s, ...)\n", path);

	try {
		struct fuse_context *ctx = fuse_get_context();
		fprintf(stdout, "\tuid: %d gid: %d pid: %d trying to readdir %s\n", 
			ctx->uid, ctx->gid, ctx->pid, path);
		// fprintf(stdout, "\t\tumask: %d\n", ctx->umask);

		std::shared_ptr<INode> dir_inode = resolve_path(path);
		if (!can_read_inode(ctx, *dir_inode)) {
			// NOTE: I think this should be handled elsewhere, but that is okay
			throw UnixError(EACCES);
		}

		IDirectory dir(*dir_inode);
		std::unique_ptr<IDirectory::DirEntry> entry = nullptr;
		while(entry = dir.next_entry(entry)) {
			if (filler(buf, entry->filename, NULL, 0) != 0)
				return -ENOMEM;
		}

	} catch (const UnixError& e) {
		fprintf(stdout, "\tmyfs_readdir encountered error %d\n", e.errorcode);
		return -e.errorcode;
	}

	return 0;
}

static int myfs_mknod(const char *path, mode_t mode, dev_t rdev) {
	// this is the example I found for this method: 
	// https://github.com/osxfuse/fuse/blob/master/example/fusexmp.c
	// http://man7.org/linux/man-pages/man2/mknod.2.html 
	
	struct fuse_context *ctx = fuse_get_context();

	// copy the path and pass it to dirname which strips off the end segment
	std::unique_ptr<char[]> path_cpy1(strdup(path));
	std::unique_ptr<char[]> path_cpy2(strdup(path));
	const char *name = basename(path_cpy1.get());
	const char *dir = dirname(path_cpy2.get());

	// allocate the new inode
	std::shared_ptr<INode> new_inode = nullptr;
	try {
		new_inode = superblock->inode_table->alloc_inode();	
	} catch (const FileSystemException &e) {
		// the disk is out of room, can not allocate any more inodes
		return -EDQUOT;
	}
	 
	try {
		std::shared_ptr<INode> dir_inode = resolve_path(dir);

		fprintf(stdout, "mkfs_mknod(%s, %d, ...)\n", path, mode);
		fprintf(stdout, "\tplacing node in directory: %s file name: %s\n", dir, name);
		if (!can_write_inode(ctx, *dir_inode)) {
			fprintf(stdout, "\tcan not write inode! throw EACCES\n");
			throw UnixError(EACCES);
		}

		// NOTE: the proper way to set the permissions are mode & ~umask
		// not sure why this is the case, but the man page says so
		fprintf(stdout, "\tfile owner: %d\n", ctx->uid);
		fprintf(stdout, "\tfile group: %d\n", ctx->gid);
		new_inode->data.UID = ctx->uid;
		new_inode->data.GID = ctx->gid;
		new_inode->data.permissions = (S_IRWXU | S_IRWXG | S_IRWXO) & mode;
		new_inode->data.permissions &= ~(ctx->umask);
		fprintf(stdout, "\tfile permissions: %d\n", new_inode->data.permissions);

		// set the mode correctly
		if (S_ISDIR(mode)) {
			fprintf(stdout, "\tS_ISDIR(mode %d) so we are creating a directory\n", mode);
			// properly initialize the empty directory
			new_inode->set_type(S_IFDIR);
			IDirectory dir(*new_inode);
			dir.initializeEmpty();
			dir.add_file(".", *new_inode);
			dir.add_file("..", *dir_inode);
		} else if (S_ISREG(mode)) {
			fprintf(stdout, "\tS_ISREG(mode %d) so we are creating a regular file\n", mode);
			new_inode->set_type(S_IFREG);
		} else {
			fprintf(stdout, "\tunrecognized file creation mode: %d\n", mode);
			throw UnixError(EINVAL); // todo: what is the correct error message here
		}

		// the file already exists in this directory
		IDirectory dir(*dir_inode);
		if (dir.add_file(name, *new_inode) == nullptr) {
			// the file already exists in this location :P 
			throw UnixError(EEXIST);
		}
	} catch (const UnixError &e) {
		fprintf(stdout, "\tmyfs_mknod encountered error %d\n", e.errorcode);
		// TODO: add code to release all chunks owned by the inode first
		superblock->inode_table->free_inode(std::move(new_inode));
		return -e.errorcode;
	}
	
	return 0;
}

static int myfs_mkdir(const char *path, mode_t mode) {
	fprintf(stdout, "myfs_mkdir(%s, %d -> %d)\n", path, mode, mode | S_IFDIR);
	return myfs_mknod(path, mode | S_IFDIR, 0);
}

static int myfs_open(const char *path, struct fuse_file_info *fi)
{
	// TODO: you can use fuse_get_context private data to store the inode that is retrieved here
	// for use in subsequent syscalls on the same path
	// that's exciting!

	std::lock_guard<std::mutex> g(lock_g);
	fprintf(stdout, "myfs_open(%s, ...)\n", path); 
	
	struct fuse_context *ctx = fuse_get_context();
	
	try {
		// TODO: store the inode in the fuse_file_info
		std::shared_ptr<INode> file_inode = resolve_path(path);
		if (file_inode == nullptr) {
			throw UnixError(EEXIST);
		}

		if (file_inode->get_type() == S_IFDIR) {
			throw UnixError(EISDIR);
		}

		// check for permission to open the file
		if (fi->flags & O_RDONLY && !can_read_inode(ctx, *file_inode) != 0) {
			throw UnixError(EACCES);
		}

		if (fi->flags & O_WRONLY && !can_write_inode(ctx, *file_inode) != 0) {
			throw UnixError(EACCES);
		}

		return 0; // TODO: figure out propre return value
	} catch (const UnixError &e) {
		fprintf(stdout, "\tmyfs_open encountered error %d\n", e.errorcode);
		return -e.errorcode;
	}
}

static int myfs_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	std::lock_guard<std::mutex> g(lock_g);
	fprintf(stdout, "myfs_read(%s, %d, %d, ...)\n", path, size, offset); 
	
	struct fuse_context *ctx = fuse_get_context();

	try {
		// TODO: store the inode in the fuse_file_info
		std::shared_ptr<INode> file_inode = resolve_path(path);
		if (file_inode == nullptr) {
			throw UnixError(EEXIST);
		}

		return file_inode->read(offset, buf, size);
	} catch (const UnixError &e) {
		fprintf(stdout, "\tmyfs_read encountered error %d\n", e.errorcode);
		return -e.errorcode;
	}
}

static int myfs_write(const char *path, const char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	std::lock_guard<std::mutex> g(lock_g);
	fprintf(stdout, "myfs_write(%s, %d, %d,...)\n", path, size, offset);
	
	struct fuse_context *ctx = fuse_get_context();

	try {
		// TODO: store the inode in the fuse_file_info
		std::shared_ptr<INode> file_inode = resolve_path(path);
		if (file_inode == nullptr) {
			throw UnixError(EEXIST);
		}

		try {
			return file_inode->write(offset, buf, size);
		} catch (FileSystemException &e) {
			// TODO: IMPORTANT!!! ADD CODE TO FULLY REMOVE THE PARTIALLY WRITTEN INODE AND THEN FREE THE INODE 
			throw UnixError(EDQUOT);
		}
	} catch (const UnixError &e) {
		fprintf(stdout, "\tmyfs_write encountered error %d\n", e.errorcode);
		return -e.errorcode;
	}
}

static int myfs_utimens(const char* path, const struct timespec ts[2]) {
	std::lock_guard<std::mutex> g(lock_g);
	fprintf(stdout, "myfs_utimens(%s, ts[0] = %lu, ts[1] = %lu, ...)\n", path, round(ts[0].tv_nsec / 1.0e6), round(ts[1].tv_nsec / 1.0e6)); 
	
	struct fuse_context *ctx = fuse_get_context();

	try {
		// TODO: store the inode in the fuse_file_info
		std::shared_ptr<INode> file_inode = resolve_path(path);
		if (file_inode == nullptr) {
			throw UnixError(EEXIST);
		}

		if (!can_write_inode(ctx, *file_inode)) {
			fprintf(stdout, "\tutimens permission denied to access inode\n");
			throw UnixError(EACCES);
		}

		// return the resutl of the read
		file_inode->data.last_accessed = round(ts[0].tv_nsec / 1.0e6);
		file_inode->data.last_modified = round(ts[1].tv_nsec / 1.0e6);
	} catch (const UnixError &e) {
		fprintf(stdout, "\tmyfs_utimens encountered error %d\n", e.errorcode);
		return -e.errorcode;
	}
}

static int myfs_unlink(const char *path) {
	fprintf(stdout, "myfs_unlink(%s)\n", path);
	struct fuse_context *ctx = fuse_get_context();

	std::unique_ptr<char[]> path_cpy1(strdup(path));
	std::unique_ptr<char[]> path_cpy2(strdup(path));
	const char *name = basename(path_cpy1.get());
	const char *dir = dirname(path_cpy2.get());

	try {
		
		std::shared_ptr<INode> dir_inode = resolve_path(dir);
		std::shared_ptr<INode> file_inode = resolve_path(path);
		if (file_inode == nullptr || dir_inode == nullptr) {
			throw UnixError(EEXIST);
		}

		if (!can_write_inode(ctx, *file_inode)) {
			fprintf(stdout, "\tunlink permission denied to write inode\n");
			throw UnixError(EACCES);
		}

		if (file_inode->get_type() != S_IFREG) {
			// can not unlink a directory
			throw UnixError(EISDIR);
		}

		fprintf(stdout, "\tremoving the directory entry for file: %s in dir %s\n", name, dir);
		IDirectory dir(*dir_inode);
		if (dir.remove_file(name) == nullptr) {
			fprintf(stdout, "\tPOTENTIALLY FATAL ERROR: file exists, but we were unable to remove it from the directory\n");
			throw UnixError(EEXIST);
		}

		fprintf(stdout, "\treleasing the chunks associated with that file\n");
		// then remove the associated file blocks
		try {
			file_inode->release_chunks();
		} catch (const FileSystemException &e) {
			fprintf(stdout, "\tfile system exception: %s", e.message.c_str());
			throw UnixError(EFAULT); // THIS SHOULD NEVER HAPPEN ANYWAY
		}
	} catch (const UnixError &e) {
		fprintf(stdout, "\tmyfs_unlink encountered error %d\n", e.errorcode);
		return -e.errorcode;
	}

	return 0;
}

// CHECK TO MAKE SURE THEY CAN'T REMOVE . AND ..
// static int myfs_rmdir(const char *path) {
// 	fprintf(stdout, "myfs_unlink(%s)\n", path);
// 	struct fuse_context *ctx = fuse_get_context();

// 	std::unique_ptr<char[]> path_cpy1(strdup(path));
// 	std::unique_ptr<char[]> path_cpy2(strdup(path));
// 	const char *name = basename(path_cpy1.get());
// 	const char *dir = dirname(path_cpy2.get());

// 	try {
		
// 		std::shared_ptr<INode> dir_inode = resolve_path(dir);
// 		std::shared_ptr<INode> file_inode = resolve_path(path);
// 		if (file_inode == nullptr || dir_inode == nullptr) {
// 			throw UnixError(EEXIST);
// 		}

// 		if (!can_write_inode(ctx, *file_inode)) {
// 			fprintf(stdout, "\tunlink permission denied to write inode\n");
// 			throw UnixError(EACCES);
// 		}

// 		if (file_inode->get_type() != S_IFDIR) {
// 			// can not unlink a directory
// 			throw UnixError(EISDIR);
// 		}

// 		IDirectory dir_to_remove(*dir_inode)


// 		fprintf(stdout, "\tremoving the directory entry for file: %s in dir %s\n", name, dir);
// 		IDirectory dir(*dir_inode);
// 		if (dir.remove_file(name) == nullptr) {
// 			fprintf(stdout, "\tPOTENTIALLY FATAL ERROR: file exists, but we were unable to remove it from the directory\n");
// 			throw UnixError(EEXIST);
// 		}

// 		fprintf(stdout, "\treleasing the chunks associated with that file\n");
// 		// then remove the associated file blocks
// 		try {
// 			file_inode->release_chunks();
// 		} catch (const FileSystemException &e) {
// 			fprintf(stdout, "\tfile system exception: %s", e.message.c_str());
// 			throw UnixError(EFAULT); // THIS SHOULD NEVER HAPPEN ANYWAY
// 		}
// 	} catch (const UnixError &e) {
// 		fprintf(stdout, "\tmyfs_unlink encountered error %d\n", e.errorcode);
// 		return -e.errorcode;
// 	}

// 	return 0;
// }

const int USER_OPT_COUNT = 2;
std::vector<std::string> user_options;

static int myfs_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs) {
	if (key == FUSE_OPT_KEY_NONOPT && user_options.size() != USER_OPT_COUNT) {
		user_options.push_back(arg);
		return 0;
	}
	return 1;
}

void sig_handler(int);

int main(int argc, char *argv[])
{
	signal(SIGSEGV, sig_handler);

	// parse arguments from the command line
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	fuse_opt_parse(&args, NULL, NULL, myfs_opt_proc);

	if (user_options.size() != USER_OPT_COUNT) {
		fprintf(stdout, "Expected argument: <backing file> <file size in bytes>\n");
		return 1;
	}

	const char *backing_file_path = user_options[0].c_str();
	const size_t file_size_in_bytes = strtol(user_options[1].c_str(), NULL, 10);

	const size_t CHUNK_SIZE = 4096;
	const size_t CHUNK_COUNT = file_size_in_bytes / CHUNK_SIZE;

	int fh = open(backing_file_path, O_RDWR);
	//truncate("realdisk.myanfest", CHUNK_COUNT * CHUNK_SIZE);
	disk = std::unique_ptr<Disk>(new Disk(CHUNK_COUNT, CHUNK_SIZE, MAP_FILE | MAP_SHARED, fh));
	fs = std::unique_ptr<FileSystem>(new FileSystem(disk.get()));
	//fs->superblock->init(0.1);
	fs->superblock->load_from_disk();
	superblock = fs->superblock.get();
	
	static struct fuse_operations myfs_oper;
	myfs_oper.getattr = myfs_getattr;
	myfs_oper.readdir = myfs_readdir;
	myfs_oper.open = myfs_open;
	myfs_oper.read = myfs_read;
	myfs_oper.write = myfs_write;
	myfs_oper.mknod = myfs_mknod;
	myfs_oper.mkdir = myfs_mkdir;
	myfs_oper.utimens = myfs_utimens;
	myfs_oper.unlink = myfs_unlink;
	
	return fuse_main(args.argc, args.argv, &myfs_oper, NULL);
}

void sig_handler(int sig) {
	switch (sig) {
		case SIGSEGV:
			fs = nullptr;
			disk = nullptr;
			abort();
		default:
			fprintf(stderr, "wasn't expecting that!\n");
			abort();
	}
}
