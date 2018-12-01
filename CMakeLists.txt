# https://cmake.org/cmake-tutorial/
cmake_minimum_required(VERSION 3.10.2)
project(Mayanfest)

set (CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/CMake")

set(Mayanfest_VERSION_MAJOR 1)
set(Mayanfest_VERSION_MINOR 0)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++11 -O0 -g -pg -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++11 -O3 -march=native -pg -D_FILE_OFFSET_BITS=64")

# include cotire, an automatic build accelerator for cmake
# include(cotire)
include(FindFUSE)

# set lib as an include directory so we can use #include <filename>
include_directories("3rdparty")
include_directories("src")

# TODO: have a cmake file per directory which adds its own sources
# see https://stackoverflow.com/questions/7046956/populating-srcs-from-cmakelists-txt-in-subdirectories

# identify the mypy sources 
file(GLOB_RECURSE SRCS_Mayanfest
  "src/*.cpp"
  "src/*.hpp")

#
# create a static library for mypy sources
# 
add_library( mayanfest ${SRCS_Mayanfest} )
# cotire(mayanfest)

#
# create mypy executable
# 
# add_executable(mypy
#  src/vm_main.cpp 
#  ${SRCS_LIB}
# )

# target_link_libraries(mypy mypycore)

#
# create mypy tests
# 
file(GLOB_RECURSE SRCS_tests
  "tests/*.cpp"
  "tests/*.hpp")

add_executable(cmake_test
  ${SRCS_tests}
)

target_link_libraries(cmake_test mayanfest)

#
# create fuse binary
#

find_package(FUSE REQUIRED)

if(FUSE_FOUND)
  add_definitions(${FUSE_DEFINITIONS})
  include_directories(${FUSE_INCLUDE_DIRS})
  add_executable(myfs
    myfs.cpp
  )
  target_link_libraries(myfs ${FUSE_LIBRARIES} mayanfest)
  add_executable(mkfs.myfs
    mkfs.myfs.cpp
  )
  target_link_libraries(mkfs.myfs ${FUSE_LIBRARIES} mayanfest)
endif()

# Finally, apply coiture
# cotire(cmake_test)
