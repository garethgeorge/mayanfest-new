#!/bin/bash
rm ../build/disk.img
../build/mkfs.myfs disk.img 10000000000
sudo ../build/myfs disk.img -f ../build/mount -o allow_other
