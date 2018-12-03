#!/bin/bash
rm ../build/disk.img
../build/mkfs.myfs ../build/disk.img 10000000000
sudo ../build/myfs ../build/disk.img -f ../build/mount -o allow_other
