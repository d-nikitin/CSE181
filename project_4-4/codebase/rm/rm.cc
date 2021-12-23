
#include "rm.h"

#include <algorithm>
#include <cstring>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
: tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor()), indexDescriptor(createIndexDescriptor())
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Create both tables and columns tables, return error if either fails
    RC rc;
    rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
        return rc;
    

    // Add table entries for both Tables and Columns
    rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(INDEXES_TABLE_ID, 1, INDEXES_TABLE_NAME);
    if (rc)
        return rc;
    


    // Add entries for tables and columns to Columns table
    rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(INDEXES_TABLE_ID, indexDescriptor);
    if (rc)
        return rc;
    
    

    return SUCCESS;
}

// Just delete the the two catalog files
RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc;

    rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    // Create the rbfm file to store the table
    if ((rc = rbfm->createFile(getFileName(tableName))))
        return rc;

    // Get the table's ID
    int32_t id;
    rc = getNextTableID(id);
    if (rc)
        return rc;

    // Insert the table into the Tables table (0 means this is not a system table)
    rc = insertTable(id, 0, tableName);
    if (rc)
        return rc;

    // Insert the table's columns into the Columns table
    rc = insertColumns(id, attrs);
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot delete it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Delete the rbfm file holding this table's entries
    rc = rbfm->destroyFile(getFileName(tableName));
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open tables file
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty
    void *value = &id;

    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID rid;
    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete RID from table and close file
    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // Delete from Columns table
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the entries whose table-id equal this table's ID
    rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Clear out any old values
    attrs.clear();
    RC rc;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    void *value = &id;

    // We need to get the three values that make up an Attribute: name, type, length
    // We also need the position of each attribute in the row
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(COLUMNS_COL_COLUMN_NAME);
    projection.push_back(COLUMNS_COL_COLUMN_TYPE);
    projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
    projection.push_back(COLUMNS_COL_COLUMN_POSITION);

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Scan through the Column table for all entries whose table-id equals tableName's table id.
    rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

    // IndexedAttr is an attr with a position. The position will be used to sort the vector
    vector<IndexedAttr> iattrs;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // For each entry, create an IndexedAttr, and fill it with the 4 results
        IndexedAttr attr;
        unsigned offset = 0;

        // For the Columns table, there should never be a null column
        char null;
        memcpy(&null, data, 1);
        if (null)
            rc = RM_NULL_COLUMN;

        // Read in name
        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char*) data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        // read in type
        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        // Read in length
        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        // Read in position
        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }
    // Do cleanup
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);
    // If we ended on an error, return that error
    if (rc != RBFM_EOF)
        return rc;

    // Sort attributes by position ascending
    auto comp = [](IndexedAttr first, IndexedAttr second) 
        {return first.pos < second.pos;};
    sort(iattrs.begin(), iattrs.end(), comp);

    // Fill up our result with the Attributes in sorted order
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }

    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    FileHandle i_fh;
    // After a record is inserted to the record-based file, an entry should also be
    // inserted to the index file if not null.

    // If you insert a tuple into a table using RelationManager::insertTuple(),
    // the tuple should be inserted into the table (via the RBF layer) and a
    // corresponding entry should be inserted into each associated index of the
    // table (via the IX layer)

    // We only care about the table ID


    
    
    
    

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;


    // INSERT ENTRY INTO CORRESPONDING INDEX
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), i_fh);
    if (rc)
        return rc;

    vector<string> projection;
    projection.push_back(INDEXES_COL_ATTR);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + INDEXES_COL_ATTR_SIZE);

    //record descriptor
    vector<Attribute> rd;
    rc = getAttributes(tableName, rd);

    IndexManager* ix = IndexManager::instance();
    IXFileHandle ifh;
    ifh.fh = i_fh;

    std::cout << "RID (before) is " << rid.pageNum << ", " << rid.slotNum << "\n";
    RID ridCopy;
    ridCopy.slotNum = rid.slotNum;
    ridCopy.pageNum = rid.pageNum;

    RID trash;
    for(int i = 0; i<rd.size(); i++){
        Attribute attr = rd[i];
        int32_t name_len = (attr.name).length();
        memcpy(value, &name_len, INT_SIZE);
        memcpy((char*)value + INT_SIZE, (attr.name).c_str(), name_len);
        // Find the table entries whose index-attr field matches the attr in recordDescriptor
        RBFM_ScanIterator rbfm_si;
        void *trashData = malloc (69);
        rc = rbfm->scan(i_fh, indexDescriptor, INDEXES_COL_ATTR, EQ_OP, value, projection, rbfm_si);
        while ((rc = rbfm_si.getNextRecord(trash, trashData)) == (SUCCESS)){
                // std::cout << "Found matching index for attr " << attr.name << "\n";            
            // Insert entry into corresponding index
            void* key = malloc(PAGE_SIZE);
            rbfm -> readAttribute(fileHandle, rd, ridCopy, attr.name, key);
            int keyF;
            memcpy(&keyF, key+1, sizeof(int));
                // std::cout << "Key is " << keyF << " and ridCopy is " << ridCopy.pageNum << ", " << ridCopy.slotNum << "\n";
                // std::cout << "Key is " << keyF << " and rid is " << rid.pageNum << ", " << rid.slotNum << "\n";
            //  insert (key, rid) into index
            ix -> insertEntry(ifh, attr, key, ridCopy); 


        }
    }

    // Let rbfm do all the work
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;


    // DELETE ENTRY FROM CORRESPONDING INDEX
    FileHandle i_fh;
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), i_fh);
    if (rc)
        return rc;

    vector<string> projection;
    projection.push_back(INDEXES_COL_ATTR);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + INDEXES_COL_ATTR_SIZE);

    //record descriptor
    vector<Attribute> rd;
    rc = getAttributes(tableName, rd);

    IndexManager* ix = IndexManager::instance();
    IXFileHandle ifh;
    ifh.fh = i_fh;

    RID ridCopy;
    ridCopy.slotNum = rid.slotNum;
    ridCopy.pageNum = rid.pageNum;

    RID trash;
    for(int i = 0; i<rd.size(); i++){
        Attribute attr = rd[i];
        int32_t name_len = (attr.name).length();
        memcpy(value, &name_len, INT_SIZE);
        memcpy((char*)value + INT_SIZE, (attr.name).c_str(), name_len);
        // Find the table entries whose index-attr field matches the attr in recordDescriptor
        RBFM_ScanIterator rbfm_si;
        void *trashData = malloc (69);
        rc = rbfm->scan(i_fh, indexDescriptor, INDEXES_COL_ATTR, EQ_OP, value, projection, rbfm_si);
        while ((rc = rbfm_si.getNextRecord(trash, trashData)) == (SUCCESS)){
                // std::cout << "Found matching index for attr " << attr.name << "\n";            
            // delete entry from corresponding index
            void* key = malloc(PAGE_SIZE);
            rbfm -> readAttribute(fileHandle, rd, ridCopy, attr.name, key);
            int keyF;
            memcpy(&keyF, key+1, sizeof(int));
                // std::cout << "Key is " << keyF << " and ridCopy is " << ridCopy.pageNum << ", " << ridCopy.slotNum << "\n";
                // std::cout << "Key is " << keyF << " and rid is " << rid.pageNum << ", " << rid.slotNum << "\n";
            //  delete (key, rid) from index
            ix -> deleteEntry(ifh, attr, key, rid);
        }
    }




    // Let rbfm do all the work
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    ///////////////////////////////////////////////////

    // DELETE ENTRY FROM CORRESPONDING INDEX
    FileHandle i_fh;
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), i_fh);
    if (rc)
        return rc;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    vector<string> projection;
    projection.push_back(INDEXES_COL_ATTR);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + INDEXES_COL_ATTR_SIZE);

    //record descriptor
    vector<Attribute> rd;
    rc = getAttributes(tableName, rd);

    IndexManager* ix = IndexManager::instance();
    IXFileHandle ifh;
    ifh.fh = i_fh;

    RID ridCopy;
    ridCopy.slotNum = rid.slotNum;
    ridCopy.pageNum = rid.pageNum;

    RID trash;
    for(int i = 0; i<rd.size(); i++){
        Attribute attr = rd[i];
        int32_t name_len = (attr.name).length();
        memcpy(value, &name_len, INT_SIZE);
        memcpy((char*)value + INT_SIZE, (attr.name).c_str(), name_len);
        // Find the table entries whose index-attr field matches the attr in recordDescriptor
        RBFM_ScanIterator rbfm_si;
        void *trashData = malloc (69);
        rc = rbfm->scan(i_fh, indexDescriptor, INDEXES_COL_ATTR, EQ_OP, value, projection, rbfm_si);
        while ((rc = rbfm_si.getNextRecord(trash, trashData)) == (SUCCESS)){
                // std::cout << "Found matching index for attr " << attr.name << "\n";            
            // delete entry from corresponding index
            void* key = malloc(PAGE_SIZE);
            rbfm -> readAttribute(fileHandle, rd, ridCopy, attr.name, key);
            int keyF;
            memcpy(&keyF, key+1, sizeof(int));
            std::cout << "OLD Key is " << keyF << "  rid is " << rid.pageNum << ", " << rid.slotNum << "\n";
            
            //  delete (key, rid) from index
            ix -> deleteEntry(ifh, attr, key, rid);


            memcpy(&keyF, data+1, sizeof(int));
            std::cout << "NEW Key is " << keyF << " and rid is " << rid.pageNum << ", " << rid.slotNum << "\n";
            ix -> insertEntry(ifh, attr, data, rid);
        }
    }
    /////////////



    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;


    // Let rbfm do all the work
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    // Get record descriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

// Let rbfm do all the work
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;

    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

string RelationManager::getFileName(const char *tableName)
{
    return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
    return tableName + string(TABLE_FILE_EXTENSION);
}

vector<Attribute> RelationManager::createTableDescriptor()
{
    vector<Attribute> td;

    Attribute attr;
    attr.name = TABLES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_SYSTEM;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = COLUMNS_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    return cd;
}

vector<Attribute> RelationManager::createIndexDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = INDEXES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = INDEXES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEXES_COL_FILE_NAME_SIZE;
    cd.push_back(attr);

    attr.name = INDEXES_COL_ATTR;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEXES_COL_ATTR_SIZE;
    cd.push_back(attr);

    return cd;
}

// Creates the Tables table entry for the given id and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();

    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    int32_t is_system = system ? 1 : 0;

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char*) data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar table name
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar file name
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len; 
    // Copy in system indicator
    memcpy((char*) data + offset, &is_system, INT_SIZE);
    offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
    unsigned offset = 0;
    int32_t name_len = attr.name.length();

    // None will ever be null
    char null = 0;

    memcpy((char*) data + offset, &null, 1);
    offset += 1;

    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attr.name.c_str(), name_len);
    offset += name_len;

    int32_t type = attr.type;
    memcpy((char*) data + offset, &type, INT_SIZE);
    offset += INT_SIZE;

    int32_t len = attr.length;
    memcpy((char*) data + offset, &len, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &pos, INT_SIZE);
    offset += INT_SIZE;
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareIndexesRecordData(int32_t id, const string &tableName, const string &attrName, void *data)
{
    unsigned offset = 0;
    
    // table_file_name = tableName + attrName
    string table_file_name = getFileName(tableName+attrName);
    int32_t file_name_len = table_file_name.length();

    int32_t attr_len = attrName.length();

    std::cout << "Preparing to insert " << table_file_name << " into " << getFileName(INDEXES_TABLE_NAME) << "\n";

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char*) data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar file name
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len; 
    // Copy in varchar attr name
    memcpy((char*) data + offset, &attr_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attrName.c_str(), file_name_len);
    offset += file_name_len; 


}

// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i+1;
        prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
        rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
        if (rc)
            return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return SUCCESS;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *tableData = malloc (TABLES_RECORD_DATA_SIZE);
    prepareTablesRecordData(id, system, tableName, tableData);
    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);

    rbfm->closeFile(fileHandle);
    free (tableData);
    return rc;
}

RC RelationManager::insertIndex(int32_t id, const string &tableName, const string &attrName)
{
    FileHandle fileHandle;
    RC rc;
    RID rid;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    
    void *tableData = malloc (INDEXES_RECORD_DATA_SIZE);
    prepareIndexesRecordData(id, tableName, attrName, tableData);
    rc = rbfm->insertRecord(fileHandle, indexDescriptor, tableData, rid);

    rbfm->closeFile(fileHandle);
    free (tableData);
    return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Grab only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Scan through all tables to get largest ID value
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    int32_t max_table_id = 0;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == (SUCCESS))
    {
        // Parse out the table id, compare it with the current max
        int32_t tid;
        fromAPI(tid, data);
        if (tid > max_table_id)
            max_table_id = tid;
    }
    // If we ended on eof, then we were successful
    if (rc == RM_EOF)
        rc = SUCCESS;

    free(data);
    // Next table ID is 1 more than largest table id
    table_id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find the table entries whose table-name field matches tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    // There will only be one such entry, so we use if rather than while
    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        int32_t tid;
        fromAPI(tid, data);
        tableID = tid;
    }

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about system column
    vector<string> projection;
    projection.push_back(TABLES_COL_SYSTEM);

    // Set up value to be tableName in API format (without null indicator)
    void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find table whose table-name is equal to tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse the system field from that table entry
        int32_t tmp;
        fromAPI(tmp, data);
        system = tmp == 1;
    }
    if (rc == RBFM_EOF)
        rc = SUCCESS;

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;   
}

void RelationManager::toAPI(const string &str, void *data)
{
    int32_t len = str.length();
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &len, INT_SIZE);
    memcpy((char*) data + 1 + INT_SIZE, str.c_str(), len);
}

void RelationManager::toAPI(const int32_t integer, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &integer, INT_SIZE);
}

void RelationManager::toAPI(const float real, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &real, REAL_SIZE);
}

void RelationManager::fromAPI(string &str, void *data)
{
    char null = 0;
    int32_t len;

    memcpy(&null, data, 1);
    if (null)
        return;

    memcpy(&len, (char*) data + 1, INT_SIZE);

    char tmp[len + 1];
    tmp[len] = '\0';
    memcpy(tmp, (char*) data + 5, len);

    str = string(tmp);
}

void RelationManager::fromAPI(int32_t &integer, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    int32_t tmp;
    memcpy(&tmp, (char*) data + 1, INT_SIZE);

    integer = tmp;
}

void RelationManager::fromAPI(float &real, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    float tmp;
    memcpy(&tmp, (char*) data + 1, REAL_SIZE);
    
    real = tmp;
}

// RM_ScanIterator ///////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Open the file for the given tableName
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
    if (rc)
        return rc;

    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Use the underlying rbfm_scaniterator to do all the work
    rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                     compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
    if (rc)
        return rc;

    return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName){
    std::cout << "Creating index on table: " << tableName << " on attribute: " << attributeName << "\n";
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // filehandle for grabbing existing records to insert into index
    FileHandle f_fh;
    // filehandle for index file
    FileHandle i_fh;

    //This method should return an error if no table the specified name exists
    FileHandle temp_fh;
    if(rbfm->openFile(getFileName(tableName), temp_fh) != 0){
        printf("No table of the specified name exists\n");
        fflush(stdout);
        return -1;
    }

    
    
    // create index files
    rbfm -> createFile( getFileName(tableName+attributeName));
    rbfm -> openFile(getFileName(tableName+attributeName), i_fh);
    
    // Insert the key,rid pairs into the newly created index
    rbfm -> openFile(getFileName(tableName), f_fh);
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    string conditionAttribute;
    vector<string> attributeNames;
    int attrID = -1;
    AttrType attrType;
    for(int i = 0; i < recordDescriptor.size(); i++){
        Attribute attr = recordDescriptor[i];
        attributeNames.push_back(attr.name);
        std::cout << "TYPE OF ATTRIBUTE " << attr.name << " is " << attr.type << "\n";
        // If the current attribute in the record descriptor matches
        // attributeName
        if(attr.name == attributeName){
            attrID = i;
            attrType = attr.type;
        }
    }

    //if attr was not found in the specified table, return error
    if(attrID == -1){
        printf("The attr does not exist in this table \n");
        fflush(stdout);
        return -1;
    }

    std::cout << "RBFM: attr ID is " << attrID << "\n";
    std::cout << "RBFM: attr type is " << attrType << "\n";
    RBFM_ScanIterator rbfm_si;
    rbfm->scan(f_fh, recordDescriptor, conditionAttribute, NO_OP, NULL, attributeNames, rbfm_si);
    RID rid;
    void* recordData = malloc(PAGE_SIZE);
    int counter = 0;
    
    IndexManager* ix = IndexManager::instance();
    IXFileHandle ifh;
    ifh.fh = i_fh;

    //Insert the key,rid pairs into the newly created index
    while( rbfm_si.getNextRecord(rid, recordData) != RBFM_EOF){
        void* key = malloc(PAGE_SIZE);
        std::cout << "Attr: " << attributeName << ". Found record " << counter++ << " in table " << tableName << "\n";

        // rbfm -> getAttributeFromRecord(recordData, 0, attrID, attrType, attrData);
        rbfm -> readAttribute(f_fh, recordDescriptor, rid, attributeName, key);
        float keyF;
        memcpy(&keyF, key+1, sizeof(float));
        std::cout << "Key is " << keyF << " and RID is " << rid.pageNum << ", " << rid.slotNum << "\n";

        //insert (key, rid) into index
        ix -> insertEntry(ifh, recordDescriptor[attrID], key, rid);    
    }

    //insert new index into catalog table Indexes
    int32_t id;
    getTableID(tableName, id);
    insertIndex(id, tableName, attributeName);
    
    return SUCCESS;
}

//This method destroys an index on a given attribute of a given table. 
//It should also delete the entry for that index from the catalog.
RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
    

    string indexTableName = tableName + attributeName;
    string table_file_name = getFileName(tableName+attributeName);
    int32_t file_name_len = table_file_name.length();
    
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    

    // Delete the rbfm file holding this table's entries
    rc = rbfm->destroyFile(getFileName(indexTableName));
    if (rc)
        return rc;

    // Open indexes file
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same file name (tableName + attr)
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty

    
    // Copy in varchar file name
    void *value = malloc(100);
    memcpy((char*) value, &file_name_len, VARCHAR_LENGTH_SIZE);
    memcpy((char*) value + VARCHAR_LENGTH_SIZE, table_file_name.c_str(), file_name_len);

    rc = rbfm->scan(fileHandle, indexDescriptor, INDEXES_COL_FILE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete RID from indexes and close file
    rbfm->deleteRecord(fileHandle, indexDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    return SUCCESS;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
    //• Gather enough info to use IndexManager::scan
    //• Use IndexManager::scan
    IndexManager* ix = IndexManager::instance();
    IXFileHandle ifh;

    std::cout << "table name is " << tableName << "\n";
    std::cout << "attribute name is " << attributeName << "\n";


    std::cout << "LESSGO_0\n";

    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    int attrIndex = -1;
    std::cout << "attrs size is " << attrs.size() << "\n";
    for(int i = 0; i < attrs.size(); i++){
        Attribute attr = attrs[i];
        std::cout << "loop attr name is " << attr.name << "\n";
        if(attr.name == attributeName){
            attrIndex = i;
            break;
        }
    }
    std::cout << "LESSGO_1\n";
    fflush(stdout);
    std::cout << "attrIndex is " << attrIndex << "\n";
    if(attrIndex == -1){
        return -1;
    }
    std::cout << "LESSGO_5\n";
    fflush(stdout);

    IX_ScanIterator ix_si;
    ix -> scan(ifh, attrs[attrIndex], lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_si);
    
    rm_IndexScanIterator.ix_si = ix_si;


	return SUCCESS;
}