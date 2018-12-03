import os
import random
import shutil
import string

import unittest

class IntegrationTest(unittest.TestCase):

    def setUp(self):
        ''' setup '''
        self.mountPoint = '../build/mount'
        self.testPoint = '../../testpoint'
        # os.mkdir(self.testPoint)

    def tearDown(self):
        ''' teardown '''
        # shutil.rmtree(self.testPoint)
        pass

    def testSetup(self):
        ''' simple test '''
        assert self.mountPoint == '../build/mount'
        assert self.testPoint == '../../testpoint'

    def testMkdir(self):
        ''' directory can be created '''
        dirName = 'testDir'

        mountDir = os.path.join(self.mountPoint, dirName)
        testDir = os.path.join(self.testPoint, dirName)

        os.mkdir(testDir)
        os.mkdir(mountDir)

        assert os.path.isdir(mountDir) == os.path.isdir(testDir)

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

    def testMknod(self):
        ''' nod can be created '''
        nodName = 'hello.txt'

        testNod = os.path.join(self.testPoint, nodName)
        mountNod = os.path.join(self.mountPoint, nodName)

        os.mknod(mountNod)
        os.mknod(testNod)

        assert os.path.isfile(mountNod) == os.path.isfile(testNod)

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

    def testMknodUnlinkWrite(self):
        ''' creating, unlinking, and then trying to write '''
        nodName = 'testMknodUnlinkWrite.txt'
        content = 'random stuff'

        testNod = os.path.join(self.testPoint, nodName)
        mountNod = os.path.join(self.mountPoint, nodName)

        os.mknod(testNod)
        os.mknod(mountNod)

        os.unlink(testNod)
        os.unlink(mountNod)

        try:
            testFD = os.open(testNod, os.O_WRONLY)
            os.write(testFD, content)
            os.close(testFD)
        except OSError, e:
            A = e.args[0]

        try:
            mountFD = os.open(mountNod, os.O_WRONLY)
            os.write(mountFD, content)
            os.close(mountFD)
        except OSError, e:
            B = e.args[0]

        assert A == B

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
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

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

    def testOverwrite(self):
        ''' overwrite files '''
        fileName = 'testOverwrite.txt'
        content1 = 'first content'
        content2 = 'second content'

        testFileName = os.path.join(self.testPoint, fileName)
        mountFileName = os.path.join(self.mountPoint, fileName)
        os.mknod(testFileName)
        os.mknod(mountFileName)

        testFD = os.open(testFileName, os.O_WRONLY)
        os.write(testFD, content1)
        os.close(testFD)

        mountFD = os.open(mountFileName, os.O_WRONLY)
        os.write(mountFD, content1)
        os.close(mountFD)

        testFD = os.open(testFileName, os.O_WRONLY)
        os.write(testFD, content2)
        os.close(testFD)

        mountFD = os.open(mountFileName, os.O_WRONLY)
        os.write(mountFD, content2)
        os.close(mountFD)

        testFD = os.open(testFileName, os.O_RDONLY)
        A = os.read(testFD, len(content2))
        os.close(testFD)

        mountFD = os.open(mountFileName, os.O_RDONLY)
        B = os.read(mountFD, len(content2))
        os.close(mountFD)

        assert A == B

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

    def testFileCreatedByRootForNonRoot(self):
        ''' create a file as root user for non-root user '''
        fileName = 'fileCreatedByRootForNonRoot.txt'
        content = 'testing whether file created by root can be accessed by non root'
        nonRootID = 1000

        testFileName = os.path.join(self.testPoint, fileName)
        mountFileName = os.path.join(self.mountPoint, fileName)
        os.mknod(testFileName)
        os.mknod(mountFileName)

        os.seteuid(nonRootID)

        try:
            testFD = os.open(testFileName, os.O_WRONLY)
            os.write(testFD, content)
            os.close(testFD)
        except OSError, e:
            A = e.args[0]

        try:
            mountFD = os.open(mountFileName, os.O_WRONLY)
            os.write(mountFD, content)
            os.close(mountFD)
        except OSError, e:
            B = e.args[0]

        #print('A:{0} B:{1}'.format(A, B))
        assert A == B

        contentOfA = ''
        contentOfB = ''

        try:
            testFD = os.open(testFileName, os.O_RDONLY)
            contentOfA = os.read(testFD, len(content))
            os.close(testFD)
        except OSError, e:
            A = e.args[0]

        try:
            mountFD = os.open(mountFileName, os.O_RDONLY)
            contentOfB = os.read(mountFD, len(content))
            os.close(mountFD)
        except OSError, e:
            B = e.args[0]

        #print('A:{0} B:{1}'.format(A, B))
        assert A == B
        #assert contentOfA == content
        #assert contentOfB == content

        os.seteuid(os.getuid())

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B

    def testTryToFindDiskCorruption(self):
        ''' 
            try to write to two files intermittently
            in the middle try to create and delete directories
            something weird might happen like files being missing
        '''
        fileName1 = 'fileName1.txt'
        fileName2 = 'fileName2.txt'
        dirName = 'diskCorruption'

        counter = 1024 * 1024
        sizePerIter = 4096

        testFileName1 = os.path.join(self.testPoint, fileName1)
        testFileName2 = os.path.join(self.testPoint, fileName2)
        mountFileName1 = os.path.join(self.mountPoint, fileName1)
        mountFileName2 = os.path.join(self.mountPoint, fileName2)
        testDirName = os.path.join(self.testPoint, dirName)
        mountDirName = os.path.join(self.mountPoint, dirName)

        os.mknod(testFileName1)
        os.mknod(testFileName2)
        os.mknod(mountFileName1)
        os.mknod(mountFileName2)

        # write two files intermittently
        fd1 = os.open(testFileName1, os.O_WRONLY)
        fd2 = os.open(testFileName2, os.O_WRONLY)
        letters = string.letters
        for x in range(counter):
            letter = random.choice(letters)
            os.write(fd1, letter)
            if x % 4096 == 0:
                os.mkdir(os.path.join(self.testPoint, '{0}-{1}'.format(dirName, x)))
            #if x == (counter / 2) + 1:
            #    os.rmdir(testDirName)
            os.write(fd2, letter)
        os.close(fd1)
        os.close(fd2)

        fd1 = os.open(mountFileName1, os.O_WRONLY)
        fd2 = os.open(mountFileName2, os.O_WRONLY)
        letters = string.letters
        for x in range(counter):
            letter = random.choice(letters)
            os.write(fd1, letter)
            if x % 4096 == 0:
                os.mkdir(os.path.join(self.mountPoint, '{0}-{1}'.format(dirName, x)))
            #if x == (counter / 2) + 1:
            #    os.rmdir(mountDirName)
            os.write(fd2, letter)
        os.close(fd1)
        os.close(fd2)

        testFD1 = os.open(testFileName1, os.O_RDONLY)
        testFD2 = os.open(testFileName2, os.O_RDONLY)
        mountFD1 = os.open(mountFileName1, os.O_RDONLY)
        mountFD2 = os.open(mountFileName2, os.O_RDONLY)

        assert os.read(testFD1, counter) == os.read(testFD2, counter)
        assert os.read(mountFD1, counter) == os.read(mountFD2, counter)

        A = set(os.listdir(self.testPoint))
        B = set(os.listdir(self.mountPoint))
        assert A == B
        
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
