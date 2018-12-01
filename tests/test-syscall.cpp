#include "syscall.hpp"
#include "catch.hpp"
#include <vector>

/*
TEST_CASE( "Parsing path should work", "[syscall]" ) {
    MockSyscalls ms = MockSyscalls();
    std::vector<std::string> pp = ms.parse_path("/foo/bar/baz/bat/");
    REQUIRE(pp.size() == 4);
    REQUIRE(pp[0] == "foo");
    REQUIRE(pp[1] == "bar");
    REQUIRE(pp[2] == "baz");
    REQUIRE(pp[3] == "bat");

    pp = ms.parse_path("/foo/bar/baz/bat");
    REQUIRE(pp.size() == 4);
    REQUIRE(pp[0] == "foo");
    REQUIRE(pp[1] == "bar");
    REQUIRE(pp[2] == "baz");
    REQUIRE(pp[3] == "bat");

    pp = ms.parse_path("/foo/bar//baz/bat///");
    REQUIRE(pp.size() == 4);
    REQUIRE(pp[0] == "foo");
    REQUIRE(pp[1] == "bar");
    REQUIRE(pp[2] == "baz");
    REQUIRE(pp[3] == "bat");

    pp = ms.parse_path("//foo////bar//baz/bat");
    REQUIRE(pp.size() == 4);
    REQUIRE(pp[0] == "foo");
    REQUIRE(pp[1] == "bar");
    REQUIRE(pp[2] == "baz");
    REQUIRE(pp[3] == "bat");
}

TEST_CASE( "mknod should work", "[syscall]" ) {
    std::unique_ptr<Disk> disk(new Disk(10 * 1024, 512));
    MockSyscalls ms = MockSyscalls();
    ms.mkfs(disk.get());
    ms.mknod("/asdf.hello");
    
}
*/