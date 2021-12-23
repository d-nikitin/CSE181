#include "rbfm.h"
#include "pfm.h"
#include <stdio.h>
#include <string.h>
#include <tgmath.h>
#include <iostream>
#include <tuple> 

PagedFileManager* _pf_manager = PagedFileManager:: instance();
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

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
    return _pf_manager->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}



RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int tupleSize = sizeof(tuple<int,int>);
    int numFields = recordDescriptor.size(); // # of fields in record
    unsigned nullFieldsIndicatorSize = (unsigned) ceil(numFields / 8.0); // # of null indicating bits
    const char *dataCopy = (char *)data; // Used for checking null bit
    int nullCheck;
    int fieldLengths [numFields];
    int fieldCumLengths [numFields];
    int howManyNotNull = 0;
    int index = 0;
    int insertPage = -1;
    int numSlots;
    int freeOffset;
    int slotDirStart;
    int free_bytes;
    int recordLengthMini;
    int recordLengthPlus;
    int pageStart;
    int pageEnd;
    int slotInsert;
    tuple<int,int> tuple;
    char *buff = (char *)malloc(PAGE_SIZE);
    char *record = (char *)malloc(PAGE_SIZE);

    // RECORD FORMAT:
    // 1. Number of Fields
    // 2. Null byte(s)
    // 3. Ending byte of each field (none for NULL fields)
    // 4. Each field
    int recordOffset = sizeof(int); // Used to offset the record buffer
    memcpy(record, &numFields, sizeof(int)); // Copy null bit(s)
    memcpy(record + recordOffset, data, nullFieldsIndicatorSize); // Copy the data into the record
    recordOffset += nullFieldsIndicatorSize; // Offset for writing record
    int dataOffset = nullFieldsIndicatorSize; // Offset for reading data
    int cumulativeLength = 0; // Used for offsets of each field
    
    for (int i = 0; i < numFields; i++) { // Iterate over the fields in the record
        nullCheck = (dataCopy[i / 8] & (1 << (7 - i % 8)));
        
        if (!nullCheck) {
            if (recordDescriptor[i].type == 2) { // Field is a varchar
                int varLen; // Store length of varchar into varlen and fieldLengths array
                memcpy(&varLen, dataCopy + dataOffset, sizeof(int));
                fieldLengths[i] = varLen;
                dataOffset += sizeof(int) + varLen;
                cumulativeLength += varLen;
                fieldCumLengths[i] = cumulativeLength;
            } else {
                fieldLengths[i] = sizeof(int);
                dataOffset += sizeof(int);
                cumulativeLength += (sizeof(int));
                fieldCumLengths[i] = cumulativeLength;
            }
            howManyNotNull += 1;
        } else { // Field is a number
            fieldLengths[i] = -1; // If data is null, length = -1
            fieldCumLengths[i] = -1;
        }  
    }

    for (int i = 0; i < numFields; i++) {
        if (fieldLengths[i] > -1) {
            fieldCumLengths[i] += (howManyNotNull * 4) + sizeof(int) + nullFieldsIndicatorSize;
        }
    }

    // Store offsets into record
    recordOffset = nullFieldsIndicatorSize + sizeof(int);
    for (int i = 0; i < numFields; i++) { // Iterate over the fields in the record
        nullCheck = (dataCopy[i / 8] & (1 << (7 - i % 8)));
        
        if (!nullCheck) {
            memcpy(record + recordOffset, &fieldCumLengths[i], sizeof(int));
            recordOffset += sizeof(int);
        }
    }

    // Store data into record
    dataOffset = nullFieldsIndicatorSize;

    for (int i = 0; i < numFields; i++) { // Iterate over the fields in the record
        nullCheck = (dataCopy[i / 8] & (1 << (7 - i % 8)));
        
        if (!nullCheck) {
            if (recordDescriptor[i].type == 2) {
                dataOffset += sizeof(int);
                memcpy(record + recordOffset, dataCopy + dataOffset, fieldLengths[i]);
                dataOffset += fieldLengths[i];
                recordOffset += fieldLengths[i];
            } else {
                memcpy(record + recordOffset, dataCopy + dataOffset, sizeof(int));
                dataOffset += sizeof(int);
                recordOffset += sizeof(int);
            }
        }
    }

    // RecordOffset has the length of the entire record at this point
    recordLengthMini = recordOffset;
    recordLengthPlus = recordOffset + tupleSize;

    // While loop to determine which page to insert into
    while ((index < fileHandle.pageCount) && (insertPage < 0)) {
        fileHandle.readPage(index, buff);
        memcpy(&numSlots, buff + 4088, sizeof(int));
        memcpy(&freeOffset, buff + 4092, sizeof(int));
        slotDirStart = 4088 - numSlots * tupleSize; // Start of n'th slot
        free_bytes = slotDirStart - freeOffset - recordLengthPlus; // Number of free bytes in the page

        if (free_bytes < 0) {
            index++;
        } else {
            insertPage = index;
        }
    }

    // If a page with enough space does not already exist then create a new page
    if (insertPage < 0) {
        pageInit(fileHandle);
        insertPage = (fileHandle.pageCount) - 1;
    }

    pageStart = insertPage * PAGE_SIZE;
    pageEnd = ((insertPage + 1) * PAGE_SIZE);
    
    // Read page into buffer. Get # of records and the free space offset
    fileHandle.readPage(insertPage, buff);
    memcpy(&numSlots, buff + 4088, sizeof(int));
    memcpy(&freeOffset, buff + 4092, sizeof(int));
    slotDirStart = 4088 - (numSlots * tupleSize); // Start of nth slot
    free_bytes = slotDirStart - freeOffset - recordLengthPlus; // Number of free bytes in the page
    
    // If not enough space in page, return failure
    if (free_bytes < 0) {
        return -1;
    }

    slotInsert = slotDirStart - tupleSize; // Determine where to put the next slot directory entry

    // Insert tuple into page directory
    tuple = make_tuple(recordLengthMini, freeOffset);
    memcpy(buff + slotInsert, &tuple, tupleSize);
    
    // Insert record into the page buffer
    memcpy(buff + freeOffset, record, recordLengthMini);
    freeOffset += recordLengthMini;
    numSlots += 1;

    // Update RID
    rid.pageNum = insertPage;
    rid.slotNum = numSlots;
    memcpy(buff + 4092, &freeOffset, sizeof(int));
    memcpy(buff + 4088, &numSlots, sizeof(int));

    fileHandle.writePage(insertPage, buff); // Write the buffer to the page
    
    if (record != NULL) {
        free(record); // Free the memory for the record
        record = NULL;
    }

    if (buff != NULL) {
        free(buff); // Free the memory for the buffer
        buff = NULL;
    }

    return 0;
}

// Helper Function
int RecordBasedFileManager::pageInit(FileHandle &fileHandle){
    char *pageData = (char *)malloc(PAGE_SIZE);

    for (unsigned i = 0; i < PAGE_SIZE - 8; i++) {
        *((char *)pageData + i) = i % 94 + 32;
    }

    int numSlots = 0;
    int freeOffset = 0;
    memcpy(pageData + 4088, &numSlots, sizeof(int));
    memcpy(pageData + 4092, &freeOffset, sizeof(int));
    fileHandle.appendPage(pageData);
    
    if (pageData != NULL) {
        free(pageData); // Free the memory for the page data
        pageData = NULL;
    }

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    char *page = (char *)malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    int num_records = -1;
    memcpy(&num_records, page + 4088, sizeof(int));
    
    if (rid.slotNum > num_records) {
        return -1;
    }

    int tupleSize = sizeof(tuple<int,int>);
    tuple<int,int> tuple;
    tuple = make_tuple(0,0);
    int record_slot_offset = PAGE_SIZE - (sizeof(int) * 2) - (rid.slotNum * tupleSize);
    memcpy(&tuple, page + record_slot_offset, tupleSize);
    
    int recSize = get<0>(tuple);
    int record_offset = get<1>(tuple);
    int num_fields = -1;
    unsigned nullSize = (unsigned) ceil(recordDescriptor.size() / 8.0);
    memcpy(&num_fields, page + record_offset, sizeof(int));
    int field_offset = record_offset + sizeof(int); // Offset to the first field in the record 
    char *buff = (char *)malloc(recSize);
    memcpy(buff, page + field_offset, nullSize);
    int buff_off = nullSize;
    field_offset += nullSize; // Field offset is now at the end of null bit

    // Counts the number of null bits in the record
    const char *dataCopy = (char *)(page + record_offset + sizeof(int));
    int null_count = 0;
    for (int k = 0; k < num_fields; ++k) {
        int nullcheck = dataCopy[k / 8] & (1 << (7 - k % 8));
        
        if(nullcheck) {
            null_count++;
        }
    }

    // Loop to move thruough the record copying all needed data
    // Curr offset is the offset of the value in the record we stored
    // Inc field offset is the incrementing offset of the field for the loop
    int curr_offset = -1;
    int inc_field_offset = field_offset;
    int nullCheck;

    for (int i = 0; i < num_fields; ++i) {
        nullCheck = (dataCopy[i / 8] & (1 << (7 - i % 8)));

        if (!nullCheck) {
            memcpy(&curr_offset, page + inc_field_offset, sizeof(int));
            
            if (!nullCheck) { // If the field is not null
                if (recordDescriptor[i].type == TypeInt) { // If the field type is of type integer
                    curr_offset -= sizeof(int);
                    memcpy(buff + buff_off, page + record_offset + curr_offset, sizeof(int));
                    buff_off += sizeof(int);
                    inc_field_offset += sizeof(int);
                } else if (recordDescriptor[i].type == TypeReal) { // If the field type is of type real
                    curr_offset -= sizeof(float);
                    memcpy(buff + buff_off, page + record_offset + curr_offset, sizeof(float));
                    buff_off += sizeof(float);
                    inc_field_offset += sizeof(float);
                } else if (recordDescriptor[i].type == TypeVarChar) { // If the field type is of type varchar
                    // Arithmetic to find the start of the var char and its length
                    int temp = num_fields * sizeof(int) + nullSize + sizeof(int) - (sizeof(int) * null_count);
                    int varchar_tot_len = curr_offset - temp;
                    curr_offset -= varchar_tot_len;
                    memcpy(buff + buff_off, &varchar_tot_len, sizeof(int));
                    buff_off += sizeof(int);
                    memcpy(buff + buff_off, page + record_offset + curr_offset, varchar_tot_len);
                    // Increment buff offset by the size of the char bc that was was written in
                    buff_off += varchar_tot_len;
                    // Incrment the field offset by the size of an int
                    inc_field_offset += sizeof(int);
                }
            }
        }
    } 

    memcpy(data, buff, recSize);

    if (page != NULL) {
        free(page); // Free the memory for the page
        page = NULL;
    }

    if (buff != NULL) {
        free(buff); // Free the memory for the buffer
        buff = NULL;
    }

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int numFields = recordDescriptor.size(); // Number of fields in the record
    int dataOffset = (unsigned) ceil(numFields / 8.0); // Byte offset to the start of the record data
    const char *dataCopy = (char *)data; // Create a copy of the data and cast it to a char pointer
    int nullCheck;
    int intValue; // Variable to store the value of the field
    float realValue; // Variable to store the value of the field
    int varCharLength = 0; // Variable to store the length of the field value
    char *varCharValue = (char *)malloc(PAGE_SIZE); // Variable to store the value of the field
    
    for (int i = 0; i < numFields; i++) { // Iterate over the fields in the record
        nullCheck = (dataCopy[i / 8] & (1 << (7 - i % 8))); // Check if the null bit is set for the record 

        if (!nullCheck) { // If the field is not null
            if (recordDescriptor[i].type == TypeInt) { // If the field type is of type integer
                memcpy(&intValue, &dataCopy[dataOffset], sizeof(int)); // Copy the field value to the variable
                printf("%s: %d\n", recordDescriptor[i].name.c_str(), intValue); // Print the field value
                dataOffset += sizeof(int); // Increment the data offset
            } else if (recordDescriptor[i].type == TypeReal) { // If the field type is of type real
                memcpy(&realValue, &dataCopy[dataOffset], sizeof(float)); // Copy the field value to the variable
                printf("%s: %f\n", recordDescriptor[i].name.c_str(), realValue); // Print the field value
                dataOffset += sizeof(float); // Increment the data offset
            } else if (recordDescriptor[i].type == TypeVarChar) { // If the field type is of type varchar
                memcpy(&varCharLength, &dataCopy[dataOffset], sizeof(int)); // Copy the field value length to the variable
                memset(varCharValue, 0, PAGE_SIZE);
                dataOffset += sizeof(int); // Increment the data offset
                memcpy(varCharValue, &dataCopy[dataOffset], varCharLength); // Copy the field value to the variable
                varCharValue[varCharLength] = '\0'; // Add a null terminator to the end of the string
                printf("%s: %s\n", recordDescriptor[i].name.c_str(), varCharValue); // Print the field value
                dataOffset += varCharLength; // Increment the data offset
            }
        } else {
            printf("%s: NULL\n", recordDescriptor[i].name.c_str()); // Print "NULL" to indicate that the field value is null
        }
    }

    if (varCharValue != NULL) {
        free(varCharValue); // Free the memory for the varchar value
        varCharValue = NULL;
    }

    return 0;
}
