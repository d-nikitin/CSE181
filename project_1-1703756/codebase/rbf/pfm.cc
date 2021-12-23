#include "pfm.h"
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <iostream>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    if (access(fileName.c_str(), 0) == 0) { // Check if the file exists
        return -1; // File exists
    } else {
        FILE *filePtr = fopen(fileName.c_str(), "wb"); // Open the file

        if (filePtr == NULL) {
            return -1; // File failed to be opened
        }

        fclose(filePtr); // Close the file
        return 0; // File was created successfully
    }
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if ((access(fileName.c_str(), 0)) == 0) { // Check if the file exists
        FILE *filePtr = fopen(fileName.c_str(), "wb"); // Open the file
        
        if (filePtr == NULL) {
            return -1; // File failed to be opened
        }

        if (remove(fileName.c_str()) == 0) { // Remove the file
            return 0; // File was deleted successfully
        } else {
            return -1; // File failed to be deleted
        }
    } else {
        return -1; // File does not exist
    }
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (access(fileName.c_str(), 0) != 0) { // Check if the file doesn't exists
        return -1; // File doesn't exists
    } else {
        if (fileHandle.openFilePtr != NULL) {
            return -1; // File handler already has an open file
        }

        FILE *filePtr = fopen(fileName.c_str(), "rb+"); // Open the file

        if (filePtr == NULL) {
            return -1;  // File failed to open
        }

        fileHandle.setOpenFile(filePtr); // Set the open file for the FH
        return 0; // File was opened successfully
    }
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    // Check if the file handler already has an open file
    if (fileHandle.openFilePtr == NULL) {
        return -1; // File does not exist
    } else {
        if (fclose(fileHandle.openFilePtr) != 0) { // Close the file
            return -1; // File could not be closed
        }

        fileHandle.openFilePtr = NULL; // Set the open file for the FH to NULL
        return 0; // File was closed successfully
    }
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    pageCount = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum < pageCount) {
        // Jump to the offset where the page is located
        fseek(openFilePtr, pageNum * PAGE_SIZE, SEEK_SET);
        // Read the page
        size_t bytesRead = fread(data, 1, PAGE_SIZE, openFilePtr);

        if (bytesRead != PAGE_SIZE) {
            return -1; // Read error
        }

        readPageCounter++; // Increment the read pages count
        return 0; // The page was read from successfully
    } else {
        return -1; // Invalid page number
    }
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum < pageCount) {
        // Jump to the offset where the page is located
        fseek(openFilePtr, pageNum * PAGE_SIZE, SEEK_SET);
        // Write the data to the page
        size_t bytesWritten = fwrite(data, 1, PAGE_SIZE, openFilePtr);

        if (bytesWritten != PAGE_SIZE) {
            return -1; // Write error
        }

        if (fflush(openFilePtr) != 0) {
			return -1; // Written data was not flushed correctly
		}

        writePageCounter++; // Increment the written pages count
        return 0; // The page was written to successfully
    } else {
        return -1; // Invalid page number
    }
}


RC FileHandle::appendPage(const void *data)
{
    fseek(openFilePtr, 0, SEEK_END); // Jump to the end of the file
    // Write the data to the page
    size_t bytesWritten = fwrite(data, 1, PAGE_SIZE, openFilePtr);

    if (bytesWritten != PAGE_SIZE) {
        return -1; // Write error
    }

    if (fflush(openFilePtr) != 0) {
        return -1; // Written data was not flushed correctly
    }

    pageCount++; // Increment the total page count
    appendPageCounter++; // Increment the appended pages count
    return 0; // A new page was appended to the file successfully
}


unsigned FileHandle::getNumberOfPages()
{
    return pageCount;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

RC FileHandle::setOpenFile(FILE *filePtr)
{
    openFilePtr = filePtr;
    fseek(openFilePtr, 0, SEEK_END); // Jump to the end of the file
    int fileSize = ftell(openFilePtr); // Get the size of the file
    fseek(openFilePtr, 0, SEEK_SET); // Jump to the beginning of the file
    pageCount = fileSize / PAGE_SIZE; // Get the page count of the file
    return 0;
}
