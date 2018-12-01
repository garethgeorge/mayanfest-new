#ifndef SYSCALL_HPP
#define SYSCALL_HPP

#include <filesystem.hpp>
#include <vector>
#include <string>

#define MAX_FN_SIZE 256
#define MAX_PL_SIZE 4096

/*
struct MockSyscalls {
    FileSystem * fs;

    std::vector<std::string> parse_path(const char * pathname) {
        if(*pathname != '/') {
            throw FileSystemException("relative file paths not supported");
        } else {
            pathname += 1;
        }

        if(strlen(pathname) > MAX_PL_SIZE) {
            throw FileSystemException("path name too long");
        }

        const char * delimiter;
        int iteration = 0;
        std::vector<std::string> parsed_path;
        while(delimiter = strchr(pathname, '/') ) {
            if(delimiter != pathname) {
                if(delimiter - pathname > MAX_FN_SIZE) {
                    throw FileSystemException("file name too long");
                }
                parsed_path.push_back(std::string(pathname, delimiter - pathname));
                iteration++;
            }
            pathname = delimiter + 1;
        }
        if(strlen(pathname) > 0) {
            parsed_path.push_back(std::string(pathname));
        }
        return parsed_path;
    }

    void mkfs(Disk * disk) {
	    fs = new FileSystem(disk);
	    fs->superblock->init(0.1);

        INode root_node = INode();
        root_node.superblock = fs->superblock.get();
        root_node.inode_table_idx = 0; //magic

        fs->superblock->inode_table->set_inode(0, root_node);
    }

    int mknod(const char * pathname) {
        std::vector<std::string> pp = parse_path(pathname);
        for(int i = 0; i < pp.size()-1; i++) {
            //TODO: traverse directory structure, check permissions, etc.
        }
        const char * filename = pp[pp.size()].c_str();

        //TODO: don't hardcode root as directory
        int dir_node_index = 0;
        INode dir_node = fs->superblock->inode_table->get_inode(dir_node_index);

        //Alloc a new inode for this file
        INode file_node = fs->superblock->inode_table->alloc_inode();

        //Add the new file to the directory
        IDirectory dir = IDirectory(dir_node);
        dir.add_file(filename, file_node);

        //flush inodes
        fs->superblock->inode_table->set_inode(file_node.inode_table_idx, file_node);
        fs->superblock->inode_table->set_inode(dir_node.inode_table_idx, dir_node);

        return 0;
    }

    int open(INode * inode) {

    }
};
*/

#endif