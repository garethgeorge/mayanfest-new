import os
import shutil

import unittest

class IntegrationTest(unittest.TestCase):

    def setUp(self):
        ''' setup '''
        self.mountPoint = '../mountpoint'
        self.testPoint = '../../testpoint'
        os.mkdir(self.testPoint)

    def tearDown(self):
        ''' teardown '''
        shutil.rmtree(self.testPoint)
        pass

    def testSetup(self):
        ''' simple test '''
        assert self.mountPoint == '../mountpoint'
        assert self.testPoint == '../../testpoint'

    def testMkdir(self):
        ''' directory can be created '''
        dirName = 'testDir'

        mountDir = os.path.join(self.mountPoint, dirName)
        testDir = os.path.join(self.testPoint, dirName)

        os.mkdir(testDir)
        os.mkdir(mountDir)

        assert os.path.isdir(mountDir) == os.path.isdir(testDir)

    def testMkdirSame(self):
        ''' trying to create same directory twice generates error code '''
        dirName = 'testDirSame'

        mountDir = os.path.join(self.mountPoint, dirName)
        testDir = os.path.join(self.testPoint, dirName)

        os.mkdir(testDir)
        os.mkdir(mountDir)

        try:
            os.mkdir(testDir)
        except OSError, e:
            A = e.args[0]
        try:
            os.mkdir(mountDir)
        except OSError, e:
            B = e.args[0]

        assert A == B

    def testMknod(self):
        ''' nod can be created '''
        nodName = 'hello.txt'

        testNod = os.path.join(self.testPoint, nodName)
        mountNod = os.path.join(self.mountPoint, nodName)

        os.mknod(mountNod)
        os.mknod(testNod)

        assert os.path.isfile(mountNod) == os.path.isfile(testNod)

    def testMknodSame(self):
        ''' trying to create same nod twice generates error '''
        nodName = 'helloSame.txt'

        testNod = os.path.join(self.testPoint, nodName)
        mountNod = os.path.join(self.mountPoint, nodName)

        os.mknod(mountNod)
        os.mknod(testNod)

        try:
            os.mknod(testNod)
        except OSError, e:
            A = e.args[0]
        try:
            os.mknod(mountNod)
        except OSError, e:
            B = e.args[0]

        assert A == B

    def testUnlinkNod(self):
        ''' nod can be created and deleted '''
        nodName = 'testUnlink.txt'

        testNod = os.path.join(self.testPoint, nodName)
        mountNod = os.path.join(self.mountPoint, nodName)

        os.mknod(testNod)
        os.mknod(mountNod)

        os.unlink(testNod)
        os.unlink(mountNod)

    def testUnlinkNodNonExistent(self):
        ''' trying to delete non-existent nod generates error '''
        nodName = 'testUnlinkNonExistent.txt'

        testNod = os.path.join(self.testPoint, nodName)
        mountNod = os.path.join(self.mountPoint, nodName)

        try:
            os.unlink(testNod)
        except OSError, e:
            A = e.args[0]
        try:
            os.unlink(mountNod)
        except OSError, e:
            B = e.args[0]
        
        assert A == B

    def testUnlinkDir(self):
        ''' trying to delete directory generates error '''
        dirName = 'testUnlinkDir'

        mountDir = os.path.join(self.mountPoint, dirName)
        testDir = os.path.join(self.testPoint, dirName)

        os.mkdir(testDir)
        os.mkdir(mountDir)

        try:
            os.unlink(testDir)
        except OSError, e:
            A = e.args[0]
        try:
            os.unlink(mountDir)
        except OSError, e:
            B = e.args[0]
        
        assert A == B

    def testRmdir(self):
        ''' create and delete a directory '''
        dirName = 'testRmdir'

        mountDir = os.path.join(self.mountPoint, dirName)
        testDir = os.path.join(self.testPoint, dirName)

        os.mkdir(testDir)
        os.mkdir(mountDir)

        os.rmdir(testDir)
        os.rmdir(mountDir)

    def testRmdirNonExistent(self):
        ''' trying to delete a non-existent directory generates error '''
        dirName = 'testRmdirNonExistent'

        mountDir = os.path.join(self.mountPoint, dirName)
        testDir = os.path.join(self.testPoint, dirName)
        
        try:
            os.rmdir(testDir)
        except OSError, e:
            A = e.args[0]
        try:
            os.rmdir(mountDir)
        except OSError, e:
            B = e.args[0]

        assert A == B

    def testReaddir(self):
        ''' directory can be read '''
        parentDir = 'testReadDir'
        dirs = ['one', 'two', 'three']

        os.mkdir(os.path.join(self.testPoint, parentDir))
        os.mkdir(os.path.join(self.mountPoint, parentDir))

        for dir in dirs:
            os.mkdir(os.path.join(self.testPoint, parentDir, dir))
            os.mkdir(os.path.join(self.mountPoint, parentDir, dir))

        A = set(os.listdir(os.path.join(self.testPoint, parentDir)))
        B = set(os.listdir(os.path.join(self.mountPoint, parentDir)))

        assert A == B

    def testReaddirNonExistent(self):
        ''' trying to list non-existent directory generates error '''
        nonExistentDirectory = 'bleh'

        testDir = os.path.join(self.testPoint, nonExistentDirectory)
        mountDir = os.path.join(self.mountPoint, nonExistentDirectory)

        try:
            os.listdir(testDir)
        except OSError, e:
            A = e.args[0]
        try:
            os.listdir(mountDir)
        except OSError, e:
            B = e.args[0]

        assert A == B

    def testOpenCloseFile(self):
        ''' open a file '''
        fileName = 'testOpenFile.txt'

        os.mknod(os.path.join(self.testPoint, fileName))
        os.mknod(os.path.join(self.mountPoint, fileName))

        fd1 = os.open(os.path.join(self.testPoint, fileName), os.O_RDONLY)
        fd2 = os.open(os.path.join(self.mountPoint, fileName), os.O_RDONLY)

        os.close(fd1)
        os.close(fd2)

    def testOpenFileNonExistent(self):
        ''' trying to open a non-existent file generates error '''
        fileName = 'bleh'

        try:
            os.open(os.path.join(self.testPoint, fileName), os.O_RDONLY)
        except OSError, e:
            A = e.args[0]
        try:
            os.open(os.path.join(self.mountPoint, fileName), os.O_RDONLY)
        except OSError, e:
            B = e.args[0]

        assert A == B

    def testWriteRead(self):
        ''' write to file and then read '''
        fileName = 'testWriteRead.txt'
        content = 'whatever bruh'

        testFileName = os.path.join(self.testPoint, fileName)
        mountFileName = os.path.join(self.mountPoint, fileName)
        os.mknod(testFileName)
        os.mknod(mountFileName)

        
        testFD = os.open(testFileName, os.O_WRONLY)
        os.write(testFD, content)
        os.close(testFD)

        mountFD = os.open(mountFileName, os.O_WRONLY)
        os.write(mountFD, content)
        os.close(mountFD)

        testFD = os.open(testFileName, os.O_RDONLY)
        A = os.read(testFD, len(content))
        os.close(testFD)

        mountFD = os.open(mountFileName, os.O_RDONLY)
        B = os.read(mountFD, len(content))
        os.close(mountFD)

        assert A == B

    def testWriteReadMore(self):
        ''' write to file and then try to read more chars '''
        fileName = 'testWriteReadMore.txt'
        content = 'whatever bruh'

        testFileName = os.path.join(self.testPoint, fileName)
        mountFileName = os.path.join(self.mountPoint, fileName)
        os.mknod(testFileName)
        os.mknod(mountFileName)

        testFD = os.open(testFileName, os.O_WRONLY)
        os.write(testFD, content)
        os.close(testFD)

        mountFD = os.open(mountFileName, os.O_WRONLY)
        os.write(mountFD, content)
        os.close(mountFD)

        testFD = os.open(testFileName, os.O_RDONLY)
        A = os.read(testFD, len(content) * 2)
        os.close(testFD)

        mountFD = os.open(mountFileName, os.O_RDONLY)
        B = os.read(mountFD, len(content) * 2)
        os.close(mountFD)

        assert A == B

    #def testFileCreatedByRootForNonRoot(self):
    #    ''' create a file as root user for non-root user '''
    #    fileName = 'fileCreatedByRootForNonRoot.txt'
    #    content = 'testing whether file created by root can be accessed by non root'
    #    nonRootID = 1000

    #    testFileName = os.path.join(self.testPoint, fileName)
    #    mountFileName = os.path.join(self.mountPoint, fileName)
    #    os.mknod(testFileName)
    #    os.mknod(mountFileName)

    #    pid = os.fork()
    #    if pid == 0:
    #        try:
    #            os.setuid(nonRootID)
    #            testFD = os.open(testFileName, os.O_WRONLY)
    #            os.write(testFD, content)
    #            os.close(testFD)

    #            mountFD = os.open(mountFileName, os.O_WRONLY)
    #            os.write(mountFD, content)
    #            os.close(mountFD)

    #            testFD = os.open(testFileName, os.O_RDONLY)
    #            A = os.read(testFD, len(content))
    #            os.close(testFD)

    #            mountFD = os.open(mountFileName, os.O_RDONLY)
    #            B = os.read(mountFD, len(content))
    #            os.close(mountFD)

    #            print('{0} {1}'.format(A, B))
    #            assert A == B
    #        finally:
    #            os._exit(0)
    #    os.waitpid(pid, 0)
        
    #def testWriteTillYouDie(self):
    #    ''' try to write huge number of small size files '''
    #    x = 0
    #    while True:
    #        x += 1
    #        fileName = 'small-file-{0}.txt'.format(x)
    #        mountFileName = os.path.join(self.mountPoint, fileName)
    #        try:
    #            os.mknod(mountFileName)
    #        except OSError, e:
    #            print('<<<<<<<<<< OSError no. {0} ({1}) >>>>>>>>>>'.format(e.args[0], e.args[1]))
    #            break


    #def testWriteLargeFile(self):
    #    ''' write a very large file '''
    #    fileName = 'largeFile.txt'
    #    content = 'a' * (5 * 1024 * 1024 * 1024)

    #    testFileName = os.path.join(self.testPoint, fileName)
    #    mountFileName = os.path.join(self.mountPoint, fileName)
    #    os.mknod(testFileName)
    #    os.mknod(mountFileName)

    #    testFD = os.open(testFileName, os.O_WRONLY)
    #    os.write(testFD, content)
    #    os.close(testFD)

    #    mountFD = os.open(mountFileName, os.O_WRONLY)
    #    os.write(mountFD, content)
    #    os.close(mountFD)



if __name__ == "__main__":
    unittest.main()
