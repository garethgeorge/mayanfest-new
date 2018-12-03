import os
import time

mountpoint = '../build/mount'
filename = 'hello'

maxnumbytes = (32 * 1024 * 1024)
numbytes = 1024

os.mknod(os.path.join(mountpoint, filename))

while numbytes <= maxnumbytes:
    starttime = time.time()
    fd = os.open(os.path.join(mountpoint, filename), os.O_WRONLY)
    os.write(fd, 'a' * numbytes)
    os.close(fd)
    print("%8d,%.16f" % (numbytes, time.time() - starttime))
    numbytes *= 2
