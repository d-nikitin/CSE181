
#include "rm.h"
#include <math.h>



//TODO: INSERT COLUMNS FIX



RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
    
}

RelationManager::RelationManager()
{
    Attribute attr; 

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tableAttributes.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableAttributes.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tableAttributes.push_back(attr);

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttributes.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnAttributes.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttributes.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttributes.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnAttributes.push_back(attr);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    

    if (rbfm->createFile("Tables.tbl")) {
        return -1;
    }

    if (rbfm->createFile("Columns.tbl")) {
        return -1;
    }

    if (insertTable("Tables", 1)) {
        return -1;
    }

    if (insertColumns(1, tableAttributes)) {
        return -1;
    }

    if (insertTable("Columns", 2)) {
        return -1;
    }

    if (insertColumns(2, columnAttributes)) {
        return -1;
    }

    return 0;
}

RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    if (rbfm->destroyFile("Tables.tbl")) {
        return -1;
    }

    if (rbfm->destroyFile("Columns.tbl")) {
        return -1;
    }

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<string> columns;
    RBFM_ScanIterator iterator;
    void *data = malloc(INT_SIZE + 1);
    int32_t id;
    int32_t maxTableId = 0;
    char nullByte;
    RID rid;
    string tableFileName = tableName + ".tbl";
    int32_t tid;

    // Create the table file
    if (rbfm->createFile(tableFileName)) {
        cout << "Create error" << endl;
        return -1;
    }

    // Open the tables file
    if (rbfm->openFile("Tables.tbl", fileHandle)) {
        cout << "Open error" << endl;
        return -1;
    } 

    //insert table
    if (insertTable(tableName, tableID)) {
        return -1;
    }

    
    //insert columns
    if (insertColumns(tableID, attrs)) {
        return -1;
    }
    std::cout << "table id is : " << tableID << "\n";
    tableID ++;    

    return SUCCESS;
}



RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<string> columns;
    RBFM_ScanIterator iterator;
    int32_t id;
    RID rid;
    string tableFileName;
    void *value;

    if (rbfm->openFile("Tables.tbl", fileHandle) != 0) {
        return -1;
    }

    if ((tableName == "Tables") || (tableName == "Columns")) {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->destroyFile(tableName + ".tbl") != 0) {
        return -1;
    }

    if (getTableID(tableName, id) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    value = &id;

    if (rbfm->scan(fileHandle, tableAttributes, "table-id", EQ_OP, value, columns, iterator)) {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (iterator.checkCurrent() != 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rid = iterator.getRID();

    std::cout << "\n";
    deleteTuple("Tables", rid);
    // rbfm->deleteRecord(fileHandle, tableAttributes, rid);
    
    rbfm->closeFile(fileHandle);
    iterator.close();

    if (rbfm->openFile("Columns.tbl", fileHandle)) {
        return -1;
    }


    
    if (rbfm->scan(fileHandle, columnAttributes, "table-id", EQ_OP, value, columns, iterator)) {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    
    if (iterator.checkCurrent() != 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    while (iterator.checkCurrent() == 0) {
        rid = iterator.getRID();
        deleteTuple("Columns", rid);
        ++iterator;
    }
    std::cout << "deleting columns done \n \n \n \n \n";

    rbfm->closeFile(fileHandle);
    iterator.close();
    return SUCCESS;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    int32_t tableID2;
    RBFM_ScanIterator iterator;
    RID rid;
    vector<string> columns;
    uint8_t nullBytes = 0;
    void* data = malloc(PAGE_SIZE);


    std::cout << "tableID: " << tableID2 << "\n";
    getTableID(tableName, tableID2);

    std::cout << "tableID: " << tableID2 << "\n";

    // Open the columns file
    if (rbfm->openFile("Columns.tbl", fileHandle) != 0) {
        return -1;
    }

    columns.push_back("table-id");

    if(rbfm->scan(fileHandle, tableAttributes, "table-id", EQ_OP, &tableID2, columns, iterator) != 0) {
        return -1;
    }

    


    //  (1, "table-id", TypeInt, 4 , 1)
    Attribute attr;
    int intA;
    int intB;
    int intC;
    char *varChar = (char *)malloc(PAGE_SIZE);
    std::cout << "iterator rids size " << iterator.rids.size() << "\n";
    
    //get table id from record from iterator
    if(iterator.checkCurrent() != 0){
        return -1;
    }
    std::cout << "TEST 6 checkpoint\n";
    // std::cout << "CHECK CURRENT: " << iterator.checkCurrent() << "\n";
    while(iterator.checkCurrent() == 0){
        // std::cout << "IN LOOP \n";
        unsigned offset = 5;
        int varCharLength;
        rid = iterator.getRID();
        rbfm -> readRecord(fileHandle, columnAttributes, rid, data);
        memcpy(&varCharLength, data + offset, INT_SIZE);
        offset += INT_SIZE;
        memset(varChar, 0, PAGE_SIZE);
        memcpy(varChar, data + offset, varCharLength);
        varChar[varCharLength] = '\0';
        offset += varCharLength;
        memcpy(&intA, data + offset, INT_SIZE);
        offset += INT_SIZE;
        memcpy(&intB, data + offset, INT_SIZE);
        offset += INT_SIZE;
        memcpy(&intC, data + offset, INT_SIZE);
        offset += INT_SIZE;

        // std::cout << "RID page iterator: " << rid.pageNum << "\n";
        // std::cout << "RID slot iterator: " << rid.slotNum << "\n";
        // std::cout << "Varchar len: " << varCharLength << "\n";

        attr.name = std::string(varChar);
        // std::cout << "attr.name: " << attr.name << "\n";
        attr.type = (AttrType) intA;
        attr.length = intB;
        attrs.push_back(attr);

        ++iterator;
    }

    std::cout << "DONE! \n" ;

    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Get record descriptor for tuple
    vector<Attribute> tupleAttrs; 
    if( getAttributes(tableName, tupleAttrs) != 0){
        return -1;
    }

    // Open the tableName file
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + ".tbl", fileHandle)  != 0) {
        return -1;
    }

    // insert tuple into table
    if( rbfm->insertRecord(fileHandle, tupleAttrs, data, rid)  != 0){
        return -1;
    }
    
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Open the tableName file
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + ".tbl", fileHandle)) {
        return -1;
    }

    // Get record descriptor for tuple
    vector<Attribute> tableAttrs; 
    if( getAttributes(tableName, tableAttrs) ){
        return -1;
    }
    std::cout << "segfault below!!\n";

    // delete tuple from table
    if( rbfm->deleteRecord(fileHandle, tableAttrs, rid) ){
        return -1;
    }
    

    return SUCCESS;
    
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Open the tableName file
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + ".tbl", fileHandle)) {
        return -1;
    }

    // Get record descriptor for tuple
    vector<Attribute> tableAttrs; 
    if( getAttributes(tableName, tableAttrs) ){
        return -1;
    }

    // update tuple in table
    if( rbfm->updateRecord(fileHandle, tableAttrs, data, rid) ){
        return -1;
    }

    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Open the tableName file
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + ".tbl", fileHandle)) {
        return -1;
    }

    // Get record descriptor for tuple
    vector<Attribute> tableAttrs; 
    if( getAttributes(tableName, tableAttrs) ){
        return -1;
    }

    // read tuple from table
    if( rbfm->readRecord(fileHandle, tableAttrs, rid, data) ){
        return -1;
    }

    return SUCCESS;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // print tuple 
    if( rbfm->printRecord(attrs, data) ){
        return -1;
    }

    return 1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    // RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Open the tableName file
    FileHandle fileHandle;
    if (rbfm->openFile(tableName + ".tbl", fileHandle)) {
        return -1;
    }

    // Get record descriptor for tuple
    vector<Attribute> tableAttrs; 
    if( getAttributes(tableName, tableAttrs) ){
        return -1;
    }

    // read attribute from record
    if( rbfm->readAttribute(fileHandle, tableAttrs, rid, attributeName, data)){
        return -1;
    }


    return SUCCESS;
}


RC RelationManager::insertTable(const string &tableName, int32_t id)
{
    FileHandle fileHandle;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    void *tableData = malloc(117);
    uint8_t nullBytes = 0;
    int32_t num;
    unsigned offset = 0;
    RID rid;
    string tableFileName;
    int32_t tableFileNameLength;
    int32_t tableNameLength;

    if (rbfm->openFile("Tables.tbl", fileHandle) != 0)
    {
        return -1;
    }

    memcpy((char*)tableData, &nullBytes, 1);
    offset += 1;
    memcpy((char*)tableData + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    tableNameLength = tableName.length();
    memcpy((char*)tableData + offset, &tableNameLength, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*)tableData + offset, tableName.c_str(), tableNameLength);
    offset += tableNameLength;
    tableFileName = tableName + ".tbl";
    tableFileNameLength = tableFileName.length();
    memcpy((char*)tableData + offset, &tableFileNameLength, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*)tableData + offset, tableFileName.c_str(), tableFileNameLength);
    offset += tableFileNameLength;

    // Insert the table data into the tables file
    if (rbfm->insertRecord(fileHandle, tableAttributes, tableData, rid)) {
        rbfm->closeFile(fileHandle);
        free(tableData);
        return -1;
    }

    rbfm->closeFile(fileHandle);
    free(tableData);
    return SUCCESS;
}

RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<string> columns;
    RBFM_ScanIterator iterator;
    void *columnData = malloc(PAGE_SIZE);
    int32_t attributeLength;
    int32_t attributeNameLength;
    int32_t attributeType;
    char null;
    unsigned offset = 0;
    int32_t position;
    RID rid;
    
    // Open the columns file
    if (rbfm->openFile("Columns.tbl", fileHandle)) {
        return -1;
    }

    // Iterate over the attributes of the table
    for (unsigned i = 0; i < attrs.size(); i++) {
        position = i + 1;
        offset = 0;
        null = 0;
        attributeNameLength = attrs[i].name.length();
        memcpy((char*) columnData + offset, &null, 1);
        offset += 1;
        memcpy((char*) columnData + offset, &id, INT_SIZE);
        offset += INT_SIZE;
        std::cout << "ID IS: " << id << "\n";
        memcpy((char*) columnData + offset, &attributeNameLength, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        memcpy((char*) columnData + offset, attrs[i].name.c_str(), attributeNameLength);
        offset += attributeNameLength;
        attributeType = attrs[i].type;
        memcpy((char*) columnData + offset, &attributeType, INT_SIZE);
        offset += INT_SIZE;
        attributeLength = attrs[i].length;
        memcpy((char*) columnData + offset, &attributeLength, INT_SIZE);
        offset += INT_SIZE;
        memcpy((char*) columnData + offset, &position, INT_SIZE);
        offset += INT_SIZE;

        // Insert the column data into the columns file
        if (rbfm->insertRecord(fileHandle, columnAttributes, columnData, rid)) {
            rbfm->closeFile(fileHandle);
            free(columnData);
            return -1;
        }
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return 0;
}

// Get the ID of a table
RC RelationManager::getTableID(const string &tableName, int32_t &returnTableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<string> columns;
    RBFM_ScanIterator iterator;
    void *data = malloc(PAGE_SIZE);
    uint8_t nullBytes = 0;
    RID rid;
    string tableFileName;
    int32_t tableNameLength;
    int32_t tid;
    void *value = malloc(PAGE_SIZE);

    // Open the tables file
    if (rbfm->openFile("Tables.tbl", fileHandle)) {
        return -1;
    }

    columns.push_back("table-id");
    
    tableNameLength = tableName.length();
    memcpy(value, tableName.c_str(), tableNameLength);

    // Find the table entries whose name matches tableName
    printf("getTableID: table attributes size : %d\n", tableAttributes.size());
    if (rbfm->scan(fileHandle, tableAttributes, "table-name", EQ_OP, value, columns, iterator)) {
        rbfm->closeFile(fileHandle);
        iterator.close();
        return -1;
    }

    //get table id from record from iterator
    if(iterator.checkCurrent() != 0){
        return -1;
    }
    rid = iterator.getRID();
    rbfm -> readRecord(fileHandle, tableAttributes, rid, data);
    memcpy(&nullBytes, data, 1);
    memcpy(&tid, data + 1, INT_SIZE);
    returnTableID = tid; 

    free(data);
    free(value);
    iterator.close();
    return SUCCESS;
}

unsigned RecordBasedFileManager::findInsertPage(FileHandle &fileHandle, unsigned recordSize){
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


        return i;
} 

void itercpy(RBFM_ScanIterator &rb_iter, RM_ScanIterator &rm_iter){
    rm_iter.count = rb_iter.count;
    for(int i = 0; i < rb_iter.rids.size(); ++i)
        rm_iter.rids.push_back(rb_iter.rids.at(i));
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RBFM_ScanIterator iter; 
    FileHandle fileHandle;
    vector<Attribute> attrs;
    // Open the tables file
    if (rbfm->openFile(tableName + ".tbl", fileHandle)) {
        return -1;
    }
    rm_ScanIterator.fileHandle = fileHandle;
    getAttributes(tableName, attrs);
    rbfm->scan(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames, iter);
    itercpy(iter, rm_ScanIterator);
    rm_ScanIterator.tableName = tableName;
    return SUCCESS;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RelationManager *rm = RelationManager::instance();
    vector<Attribute> attrs;
    rm->getAttributes(tableName, attrs);
    if(checkCurrent() == 0){
        rid = getRID();
        count ++;
        rbfm->readRecord(fileHandle, attrs, rid, data);
    }
    else{
        return RM_EOF;
    }
    return SUCCESS;
    // if (rbfm->openFile(rid. + ".tbl", fileHandle)) {
    //     return -1;
    // }
}