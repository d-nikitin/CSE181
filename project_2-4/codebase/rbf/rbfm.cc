#include "rbfm.h"
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    bool forwarded = false;

    std::cout << "deleteRecord: pageNum is " << rid.pageNum << "\n";
    std::cout << "deleteRecord: pageSlot is " << rid.slotNum << "\n";


    //check if page exists 
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    unsigned pageNum = rid.pageNum;
    if(numberOfPages < pageNum)
        return -1;

    
    //read page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(pageNum, pageData))
        return RBFM_READ_FAILED;

        

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    int numRecords = slotHeader.recordEntriesNumber;
    if(numRecords <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

        
    
    // Length and offset of record to delete. Saved for future use.
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    unsigned deleteRecordOffset = recordEntry.offset;
    unsigned deleteRecordLength = recordEntry.length;

    //Checks if record is forwarded
    if(deleteRecordLength < 0){
        forwarded = true;
    }


    // If slot is forwarded, delete forwaded record slot, recur on (non forwarded) slot
    if(forwarded){
        // Delete forwarded record slot
        // "Shift back" each slot after the deleted record slot by 1
        std::cout << "record is forwarded \n";
        for(int i = rid.slotNum; i<= numRecords-1; i++){
            SlotDirectoryRecordEntry nextRecordEntry;
            recordEntry = getSlotDirectoryRecordEntry(pageData, i);
            nextRecordEntry = getSlotDirectoryRecordEntry(pageData, i+1);
            recordEntry.offset = nextRecordEntry.offset;
            recordEntry.length = nextRecordEntry.length;
        }
        RID ridCopy;
        ridCopy.pageNum = deleteRecordOffset;
        ridCopy.slotNum = -deleteRecordLength;
        deleteRecord(fileHandle, recordDescriptor, ridCopy);
    }
    else{
        // Updates slot directory for all records including and after the delete record
        unsigned recordShiftLength;
            // Reduce the offset of each following record by old record length
            // This loop ignores the record to delete
        for(int i = rid.slotNum+1; i<= numRecords; i++){
            recordEntry = getSlotDirectoryRecordEntry(pageData, i);
            recordEntry.offset -= deleteRecordLength;
            recordShiftLength += recordEntry.length; 
        }
            // "Shift back" each slot after the deleted record slot by 1
        // for(int i = rid.slotNum; i<= numRecords-1; i++){
        //     SlotDirectoryRecordEntry nextRecordEntry;
        //     recordEntry = getSlotDirectoryRecordEntry(pageData, i);
        //     nextRecordEntry = getSlotDirectoryRecordEntry(pageData, i+1);
        //     recordEntry.offset = nextRecordEntry.offset;
        //     recordEntry.length = nextRecordEntry.length;
        // }
            // REMINDER: This is the free space OFFSET, not the amount of free space
        slotHeader.freeSpaceOffset -= deleteRecordLength;
            // Update record entry number and free space offset
        slotHeader.recordEntriesNumber -= 1;
        
        // Shift all records after existing record if there are any to shift
        // Start from where deleted record begins
        // Copy starting from end of deleted record 
        // Copy length is the length of all records after the deleted record
        memcpy(pageData + deleteRecordOffset, 
            pageData + (deleteRecordOffset + deleteRecordLength), 
            recordShiftLength);

    }


    recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    recordEntry.offset = -1;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);

    std::cout << "segfault below \n";

    fileHandle.writePage(pageNum, pageData);
    // free(pageData);

    std::cout << "segfault check 4 \n";


    return SUCCESS;
}

  // Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){   

    bool forwarded = false;

    //check if page exists 
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    unsigned pageNum = rid.pageNum;
    if(numberOfPages < pageNum)
        return -1;

    //read page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    int numRecords = slotHeader.recordEntriesNumber;
    if(numRecords <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;
    
    // Length and offset of record to update
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    unsigned recordOffset = recordEntry.offset;
    unsigned recordLength = recordEntry.length;

    //Checks if record is forwarded
    if(recordLength < 0){
        forwarded = true;
    }

    // If slot is forwarded, recur on (non forwarded) slot
    if(forwarded){
        RID ridCopy;
        ridCopy.pageNum = recordOffset;
        ridCopy.slotNum = -recordLength;
        updateRecord(fileHandle, recordDescriptor, data, ridCopy);
    }

    // Gets the size of the new records.
    unsigned oldSlotNum = rid.slotNum;
    unsigned newRecordLength = getRecordSize(recordDescriptor, data);

    // determine if we need to replace record, change offsets, or forward record
    unsigned oldPageNum = rid.pageNum;
    void *oldPageData = malloc(PAGE_SIZE);
    if (oldPageData == NULL)
        return RBFM_MALLOC_FAILED;
    if (fileHandle.readPage(oldPageNum, oldPageData))
        return RBFM_READ_FAILED;
    unsigned pageFreeSpace = getPageFreeSpaceSize(oldPageData);


    // New record can overwrite old record without any other changes
    if(recordLength == newRecordLength){
        //replace old record
        setRecordAtOffset(oldPageData, recordOffset, recordDescriptor, data);
    }
    // New record needs to go on a new page
    else if ( newRecordLength > (pageFreeSpace + recordLength) ){ 
        std::cout << "forwarding update";
        // Delete old record
        //   Updates slot directory for all records after the delete record
        unsigned recordShiftLength;
        //   Reduce the offset of each following record by old record length
        //   This loop ignores the record to delete
        for(int i = rid.slotNum+1; i<= numRecords; i++){
            recordEntry = getSlotDirectoryRecordEntry(pageData, i);
            recordEntry.offset -= recordLength;
            recordShiftLength += recordEntry.length; 
        }
        //   REMINDER: This is the free space OFFSET, not the amount of free space
        slotHeader.freeSpaceOffset -= recordLength;
        memcpy(oldPageData + recordOffset, 
            oldPageData + (recordOffset + recordLength), 
            recordShiftLength);

        // Find a new page to insert new record into
        unsigned newPageNum = findInsertPage(fileHandle, newRecordLength);
        void *newPageData = malloc(PAGE_SIZE);
        if (newPageData == NULL)
            return RBFM_MALLOC_FAILED;
        if (fileHandle.readPage(newPageNum, newPageData))
            return RBFM_READ_FAILED;

        // Update new page slot directory
        SlotDirectoryHeader newSlotHeader = getSlotDirectoryHeader(newPageData);
        SlotDirectoryRecordEntry newRecordEntry;
        newRecordEntry.offset = newSlotHeader.freeSpaceOffset + recordLength;
        newRecordEntry.length = newRecordLength;
        unsigned newRecordSlot = newSlotHeader.recordEntriesNumber;
        setSlotDirectoryRecordEntry(newPageData, newRecordSlot, newRecordEntry);
        
        // Update old page 
        //    forward in old page slot directory
        SlotDirectoryHeader oldSlotHeader = getSlotDirectoryHeader(oldPageData);
        SlotDirectoryRecordEntry oldRecordEntry;
        oldRecordEntry.offset = newPageNum;
        oldRecordEntry.length = -newRecordSlot; //forwarded length is negative slot number
        setSlotDirectoryRecordEntry(oldPageData, oldSlotNum, oldRecordEntry);
        //    write and free old page
        fileHandle.writePage(oldPageNum, oldPageData);

        // Insert new record
        memcpy(newPageData + newSlotHeader.freeSpaceOffset, data, newRecordLength);
        //   Update new page slot header
        newSlotHeader.freeSpaceOffset += newRecordLength;
        newSlotHeader.recordEntriesNumber += 1;
        //   write and free new page
        fileHandle.writePage(newPageNum, newPageData);
        free(newPageData);
    }
    // New record data fits on same page but is a different length
    else{ 
        std::cout << "same page new size update \n";

        ///
        // for(int i = rid.slotNum; i< numRecords; i++){
        //     recordEntry = getSlotDirectoryRecordEntry(oldPageData, i);
        //     std::cout << "RECORD " << i << " old offset: " << recordEntry.offset << "\n";
        //     std::cout << "RECORD " << i << " old length: " << recordEntry.length << "\n";
        // }
        ///

        unsigned recordShiftLength = newRecordLength - recordLength;
        unsigned totalShiftLength = 0;
        for(int i = rid.slotNum+1; i< numRecords; i++){
            recordEntry = getSlotDirectoryRecordEntry(oldPageData, i);
            recordEntry.offset += recordShiftLength;
            setSlotDirectoryRecordEntry(oldPageData, i, recordEntry);
            totalShiftLength += recordEntry.length; 
        }

        //update record length (for the tuple to update)
        recordEntry = getSlotDirectoryRecordEntry(oldPageData, rid.slotNum);
        recordEntry.length = newRecordLength;
        setSlotDirectoryRecordEntry(oldPageData, rid.slotNum, recordEntry);
        
        // REMINDER: This is the free space OFFSET, not the amount of free space
        slotHeader.freeSpaceOffset += recordShiftLength;

        // Shift all records after existing record if there are any to shift
        // Destination: Where the new record will end
        // Source: The end of the existing record
        // Copy length is the length of all records after the record to update
        memmove(oldPageData + recordOffset + newRecordLength, 
                oldPageData + recordOffset + recordLength, 
                totalShiftLength);

        // Insert new record in place of old record (after having shifted for extra space)
        // memcpy(oldPageData + recordOffset, data, newRecordLength);
        setRecordAtOffset(oldPageData, recordOffset, recordDescriptor, data);
        std::cout << "record offset is " << recordOffset << "\n";


        fileHandle.writePage(rid.pageNum, oldPageData);

    }

    free(oldPageData);
    return SUCCESS;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting up the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);


    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    if(recordEntry.offset < 0){
        return RBFM_SLOT_DN_EXIST;
    }


    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    // • get the page and the record (careful if it is a forwarding address)
    // • get the index of the attributeName in the recordDescriptor (find_if and distance)
    // • get the attributes from the record by the offsets.

    // If slot is forwarded, recur on (non forwarded) slot
    ////BELOW COPIED FROM updateRecord
    bool forwarded = false;
    //check if page exists 
    unsigned numberOfPages = fileHandle.getNumberOfPages();
    unsigned pageNum = rid.pageNum;
    if(numberOfPages < pageNum)
        return -1;
    //read page
    void *pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(pageNum, pageData))
        return RBFM_READ_FAILED;
    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    int numRecords = slotHeader.recordEntriesNumber;
    if(numRecords <= rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Length and offset of record to delete. Saved for future use.
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    unsigned recordOffset = recordEntry.offset;
    unsigned recordLength = recordEntry.length;
    //Checks if record is forwarded
    if(recordLength < 0){
        forwarded = true;
    }
    if(forwarded){
        RID ridCopy;
        ridCopy.pageNum = recordOffset;
        ridCopy.slotNum = -recordLength;
        readAttribute(fileHandle, recordDescriptor, ridCopy, attributeName, data);
    }

    // get ID of attribute to read
    int attrID = -1;
    for(int i=0; i < recordDescriptor.size(); i++){
        if(recordDescriptor[i].name == attributeName){
            attrID = i;
        }
    }   

    std::cout << "RBFM: attr ID is " << attrID << "\n";

    if(attrID != -1){
        ///////////////////
        readRecord(fileHandle, recordDescriptor, rid, data);
        // sets byte value to 1 indicating the record is null
        char nullBit = 1;
        // Parse the null indicator and save it into an array
        int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
        char nullIndicator[nullIndicatorSize];
        memset(nullIndicator, 0, nullIndicatorSize);
        memcpy(nullIndicator, data, nullIndicatorSize);

        if(fieldIsNull(nullIndicator, attrID)){
            //return null data with null indicator
            memcpy(data, &nullBit, 1);
        }
        else{
            //add null bit
            nullBit = 0;
            memcpy(data, &nullBit, 1);
            // write attribute to data
            //// TAKEN FROM printRecord
            unsigned offset = nullIndicatorSize;
            for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
            {
                bool isNull = fieldIsNull(nullIndicator, i);
                if (isNull)
                {
                    continue;
                }
                switch (recordDescriptor[i].type)
                {
                    case TypeInt:
                        if(i == attrID){
                            memcpy(data + 1, ((char*) data + offset), INT_SIZE);
                        }
                        offset += INT_SIZE;
                    break;
                    case TypeReal:
                        if(i == attrID){
                            memcpy(data + 1, ((char*) data + offset), REAL_SIZE);
                        }
                        offset += REAL_SIZE;
                    break;
                    case TypeVarChar:
                        // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                        uint32_t varcharSize;
                        memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                        offset += VARCHAR_LENGTH_SIZE;

                        // Gets the actual string.
                        char *data_string = (char*) malloc(varcharSize + 1);
                        if (data_string == NULL)
                            return RBFM_MALLOC_FAILED;
                        if(i == attrID){
                            memcpy(data + 1, ((char*) data + offset), varcharSize);
                        }

                        offset += varcharSize;

                        free(data_string);
                    break;
                }
            }
        }
    }
    else{ // attribute does not exist
        std::cout << "ATTR DOESNT EXIST \n";
        return -1;
    }   
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }

    
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);
    

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,
    const void *value,
    const vector<string> &attributeNames,
    RBFM_ScanIterator &rbfm_ScanIterator)
{
    std::cout << "condition Attr: " << conditionAttribute << "\n";
    for(int i = 0; i < attributeNames.size(); ++i)
        std::cout << "Attr Name at index " << i << " is " << attributeNames[i] << "\n";

    unsigned numPages = fileHandle.getNumberOfPages();
    void *pageData = malloc(PAGE_SIZE);
    void *recordData = malloc(PAGE_SIZE);
    void *varCharData = malloc(PAGE_SIZE);
    unsigned numSlots;
    SlotDirectoryHeader pageHeader;
    SlotDirectoryRecordEntry recordEntry;
    unsigned tempOffset = 0;
    unsigned tempInt;
    int nullIndicatorSize;
    bool attributeMatch = false;
    

    for (unsigned i = 0; i < numPages; i++) {
        if (fileHandle.readPage(i, pageData)) {
            return RBFM_READ_FAILED;
        }
        pageHeader = getSlotDirectoryHeader(pageData);
        numSlots = pageHeader.recordEntriesNumber;
        for (unsigned j = 0; j < numSlots; j++) {   
            recordEntry = getSlotDirectoryRecordEntry(pageData, j);
            std::cout << "offset " << recordEntry.offset << "\n";
            std::cout << "length " << recordEntry.length << "\n";
            // check for forwarding
            if(recordEntry.offset > 0){
                getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, recordData);
                int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
                tempOffset = nullIndicatorSize;
                for (unsigned k = 0; k < recordDescriptor.size(); k++) {

                    if(recordDescriptor[k].name == conditionAttribute){
                        attributeMatch = true;
                    }


                    switch (recordDescriptor[k].type)
                    {
                        case TypeInt:
                            std::cout << "typeInt \n";
                            memcpy(&tempInt, (char*)recordData + tempOffset, INT_SIZE);
                            // std::cout << "table ID: " << tempInt << "\n";
                            tempOffset += INT_SIZE;
                            if(attributeMatch){
                                //
                                int value_integer;
                                memcpy (&value_integer, value, INT_SIZE);
                                // std::cout << "We want " << value_integer << " and we have " << tempInt << "\n";
                                //
                                if(conditionCheck((int)tempInt, compOp, value)){
                                    // std::cout << "They match! \n";
                                    RID newRID;
                                    newRID.pageNum = i;
                                    newRID.slotNum = j;
                                    rbfm_ScanIterator.rids.push_back(newRID);
                                }
                            }

                        break;
                        case TypeReal:
                            std::cout << "typeReal \n";
                            float data_real;
                            memcpy(&data_real, recordData + tempOffset, REAL_SIZE);
                            tempOffset += REAL_SIZE;
                            cout << "" << data_real << endl;
                            if(attributeMatch){
                                if(conditionCheck(data_real, compOp, value)){
                                    RID newRID;
                                    newRID.pageNum = i;
                                    newRID.slotNum = j;
                                    rbfm_ScanIterator.rids.push_back(newRID);
                                }
                            }
                        break;
                        case TypeVarChar:
                            std::cout << "typeVarChar \n";
                            unsigned varcharSize;
                            // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                            memcpy(&varcharSize, recordData + tempOffset, VARCHAR_LENGTH_SIZE);
                            tempOffset += VARCHAR_LENGTH_SIZE;
                            memcpy(varCharData, recordData + tempOffset, varcharSize);
                            char arr [varcharSize];
                            memcpy(arr, varCharData, varcharSize);
                            // std::cout << arr << "\n";
                            // We also have to account for the overhead given by that integer.
                            tempOffset += varcharSize;
                            if(attributeMatch){
                                if(conditionCheck(varCharData, compOp, value, varcharSize)){
                                    RID newRID;
                                    newRID.pageNum = i;
                                    newRID.slotNum = j;
                                    rbfm_ScanIterator.rids.push_back(newRID);
                                }
                            }

                        break;
                    }

                    attributeMatch = false;
                }
            }
        }
    }

    std::cout << "SCAN DONE \n";

    // for(int i = 0; i < rbfm_ScanIterator.rids.size(); i++){
    //     std::cout << "SCAN RID:" << rbfm_ScanIterator.rids.at(i).pageNum << ":" << rbfm_ScanIterator.rids.at(i).slotNum << "\n";
    // }

    free(pageData);

    return SUCCESS;
}

bool RecordBasedFileManager::conditionCheck(int data_integer, CompOp compOp, const void * value)
{
    int value_integer;
    memcpy (&value_integer, value, INT_SIZE);

    switch (compOp)
    {
        case EQ_OP:  // =
            return data_integer == value_integer;
        case LT_OP:  // <
            return data_integer < value_integer;
        case GT_OP:  // >
            return data_integer > value_integer;
        case LE_OP:  // <=
            return data_integer <= value_integer;
        case GE_OP:  // >=
            return data_integer >= value_integer;
        case NE_OP:  // !=
            return data_integer != value_integer;
        case NO_OP:  // No condition
            return true;
        default:
            return false;
    }
}

bool RecordBasedFileManager::conditionCheck(float data_real, CompOp compOp, const void * value)
{
    float value_real;
    memcpy (&value_real, value, REAL_SIZE);

    switch (compOp)
    {
        case EQ_OP:  // =
            return data_real == value_real;
        case LT_OP:  // <
            return data_real < value_real;
        case GT_OP:  // >
            return data_real > value_real;
        case LE_OP:  // <=
            return data_real <= value_real;
        case GE_OP:  // >=
            return data_real >= value_real;
        case NE_OP:  // !=
            return data_real != value_real;
        case NO_OP:  // No condition
            return true;
        default:
            return false;
    }
}

bool RecordBasedFileManager::conditionCheck(void * data_string, CompOp compOp, const void * value, unsigned compareLength)
{
    
    char temp1 [compareLength];
    char temp2 [compareLength];
    memcpy(&temp1, data_string, compareLength);
    memcpy(&temp2, value, compareLength);
    // std::cout << "data is : " << temp1 << "\n";
    // std::cout << "value we want is : " << temp2 << "\n";
    switch (compOp)
    {
        case EQ_OP:  // =
            return memcmp(data_string, value, compareLength) == 0;
        case LT_OP:  // <
            return memcmp(data_string, value, compareLength) < 0;
        case GT_OP:  // >
            return memcmp(data_string, value, compareLength) > 0;
        case LE_OP:  // <=
            return memcmp(data_string, value, compareLength) <= 0;
        case GE_OP:  // >=
            return memcmp(data_string, value, compareLength) >= 0;
        case NE_OP:  // !=
            return memcmp(data_string, value, compareLength) != 0;
        case NO_OP:  // No condition
            return true;
        default:
            return false;
    }
}

