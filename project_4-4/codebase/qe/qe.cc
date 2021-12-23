
#include "qe.h"

Filter::Filter(TableScan* input, const Condition &condition) {
    //input -> type = 69;
    //std::cout<<"filter is of type: "<<typeid(input).name()<<std::endl; 

    std::cout << "table scan\n";
    std::cout << "table name is " << input->tableName << "\n";
    //rm_iter for scan bois

        // RelationManager &rm;
        // RM_ScanIterator *iter;
        // string tableName;
        // vector<Attribute> attrs;
        // vector<string> attrNames;
        // RID rid;

    // struct Condition {
    // string  lhsAttr;        // left-hand side attribute
    // CompOp  op;             // comparison operator
    // bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    // string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    // Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
    // };

        
    //   RC scan(const string &tableName,
    //   const string &conditionAttribute,
    //   const CompOp compOp,                  // comparison type such as "<" and "="
    //   const void *value,                    // used in the comparison
    //   const vector<string> &attributeNames, // a list of projected attributes
    //   RM_ScanIterator &rm_ScanIterator);
    
    // std::cout << "scan inputs:\n";
    std::cout << "Condition lhsAttr: " << condition.lhsAttr << "\n";
    // std::cout << "Condition op: " << condition.op << "\n";
    // std::cout << "Condition rhsValue attrType : " << condition.rhsValue.type << "\n";

    // int temp;
    // memcpy(&temp, condition.rhsValue.data, INT_SIZE);
    // std::cout << "Condition rhsValue data : " << temp << "\n";

    this->type = 0;

    for(int i = 0; i < (input->attrNames).size(); i++){
        std::cout << "Input attrnames : " << i << " " << (input->attrNames)[i] << "\n";
        if(((input->attrNames)[i]) == "B"){
            printf("MATCH\n");
        }
    }

    if(condition.bRhsIsAttr != true){
        printf("bRhsIsAttr is not true\n");
        string x= condition.lhsAttr;
        int pos = x.find(".");
        string sub = x.substr (pos+1);
        std::cout << "SUB IS" << sub << "\n";
        input->rm.scan(input->tableName, sub, condition.op, condition.rhsValue.data, input->attrNames, this->rm_iter);
    }
    else{
        printf("not programmed yet?");
    }


    //fflush(stdout);
    // Output 'b is of type someClass'
    // This filter class is initialized by an input iterator and3QZSRF2ed! a selection
    // condition. It filters the tuples from the input iterator by applying
    // the filter predicate condition on them. For simplicity, we assume this
    // filter only has a single selection condition. The schema of the returned
    // tuples should be the same as the input tuples from the iterator.
    
}
Filter::Filter(IndexScan* input, const Condition &condition) {
    //input -> type = 69;
    //std::cout<<"filter is of type: "<<typeid(input).name()<<std::endl; 
    std::cout << "index scan\n";
    std::cout << "table name is " << input->tableName << "\n";
    fflush(stdout);

    // struct Condition {
    // string  lhsAttr;        // left-hand side attribute
    // CompOp  op;             // comparison operator
    // bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    // string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    // Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
    // };

    // RelationManager &rm;
    //     RM_IndexScanIterator *iter;
    //     string tableName;
    //     string attrName;
    //     vector<Attribute> attrs;
    //     char key[PAGE_SIZE];
    //     RID rid;

    // Condition cond;
    // cond.lhsAttr = "right.C";
    // cond.op = GE_OP;
    // cond.bRhsIsAttr = false;
    // Value value;
    // value.type = TypeReal;
    // value.data = malloc(bufSize);
    // *(float *) value.data = compVal;
    // cond.rhsValue = value;

    // EQ_OP = 0,  // no condition// = 
    // LT_OP,      // <
    // LE_OP,      // <=
    // GT_OP,      // >
    // GE_OP,      // >=
    // NE_OP,      // !=
    // NO_OP       // no condition
    
    // RelationManager &rm;
    // RM_IndexScanIterator *iter;
    // string tableName;
    // string attrName;
    // vector<Attribute> attrs;
    // char key[PAGE_SIZE];
    // RID rid;

    this->type = 1;
    this->op = condition.op;
    this->tableName = input->tableName;
    this->attrName = input->attrName;

    vector<string> attrNames;
    attrNames.push_back(input->attrName);
    input->rm.scan(input->tableName, "", NO_OP, condition.rhsValue.data, attrNames, this->rm_iter);

    // Output 'b is of type someClass'
    // This filter class is initialized by an input iterator and a selection
    // condition. It filters the tuples from the input iterator by applying
    // the filter predicate condition on them. For simplicity, we assume this
    // filter only has a single selection condition. The schema of the returned
    // tuples should be the same as the input tuples from the iterator.
    
}

Filter::Filter(Iterator* input, const Condition &condition) {
    //input -> type = 69;
    //std::cout<<"filter is of type: "<<typeid(input).name()<<std::endl; 
    Filter((IndexScan*) input, condition);
    //std::cout << "table name is " << input->tableName << "\n";

    // Output 'b is of type someClass'
    // This filter class is initialized by an input iterator and a selection
    // condition. It filters the tuples from the input iterator by applying
    // the filter predicate condition on them. For simplicity, we assume this
    // filter only has a single selection condition. The schema of the returned
    // tuples should be the same as the input tuples from the iterator.
}

bool compareData(void* key, void* val, CompOp op, AttrType type){
    //TypeInt = 0, TypeReal, TypeVarChar   
    if(type == 0){
        int var;
        int ker;
        memcpy(&var, val,  sizeof(int));
        memcpy(&ker, key,  sizeof(int));
        switch (op){
            case EQ_OP:
                if(var == ker) return true;
                else return false;
            case LT_OP:
                if(var < ker)  return true;
                else return false;
            case LE_OP:
                if(var <= ker)  return true;
                else return false;
            case GT_OP:
                if(var > ker)  return true;
                else return false;
            case GE_OP:
                if(var >= ker)  return true;
                else return false;
            case NE_OP: //check later
                if(var != ker)  return true;
                else return false;
            case NO_OP:
                return true;
        }
    }
    else if(type == 1){
        float var;
        float ker;
        memcpy(&var, val,  sizeof(float));
        memcpy(&ker, key,  sizeof(float));
        switch (op){
            case EQ_OP:
                if(var == ker) return true;
                else return false;
            case LT_OP:
                if(var < ker)  return true;
                else return false;
            case LE_OP:
                if(var <= ker)  return true;
                else return false;
            case GT_OP:
                if(var > ker)  return true;
                else return false;
            case GE_OP:
                if(var >= ker)  return true;
                else return false;
            case NE_OP: //check later
                if(var != ker)  return true;
                else return false;
            case NO_OP:
                return true;
        }
    }
    else { // varchar
        char* var;
        char* ker; 
        memcpy(&var, val, 99);
        memcpy(&ker, key, 99);
        switch (op){
            case EQ_OP:
                if(var == ker) return true;
                else return false;
            case LT_OP:
                if(var < ker)  return true;
                else return false;
            case LE_OP:
                if(var <= ker)  return true;
                else return false;
            case GT_OP:
                if(var > ker)  return true;
                else return false;
            case GE_OP:
                if(var >= ker)  return true;
                else return false;
            case NE_OP: //check later
                if(var != ker)  return true;
                else return false;
            case NO_OP:
                return true;
        }
    }
}

RC Filter::getNextTuple(void* data){

    // • Parse out the attributes.
    // • Check if condition is valid
    // • Perform comparison
    // • Repeat until either compare succeeds or we hit an error


    RID rid;
    std::cout << "check\n";
    if(this->type == 0){
        if(this->rm_iter.getNextTuple(rid, data) == 0)
            return SUCCESS;
    }
    // else if(this->type == 1){
    //     printf("bussy\n");
    //     fflush(stdout);
    //     RelationManager *rm = RelationManager::instance();
    //     printf("IX iter getNextEntry\n");
    //     fflush(stdout);
    //     if(this->ix_iter.getNextEntry(rid, key) == 0){
    //         std::cout << "RID IS " << rid.pageNum << ", " << rid.slotNum << "\n";
    //         fflush(stdout);
    //         int rc = rm->readTuple(this->tableName, rid, data);
    //         free(key);
    //         return SUCCESS;
    //     }
    // }
    else if(this->type == 1){
        // RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        // RelationManager *rm = RelationManager::instance();
        
        // void* attrData = malloc(PAGE_SIZE);
        // vector <Attribute> rd;
        // rm->getAttributes(this->tableName, rd);
        // FileHandle fh;
        // string fileName = (this->tableName) + ".t";
        // RC rc;
        // rc = rbfm->openFile(fileName, fh);
        // if(rc != 0){
        //     printf("dummy");
        // }
        // if(this->rm_iter.getNextTuple(rid, data) == 0){
        //     std::cout << "RID IS " << rid.pageNum << ", " << rid.slotNum << "\n";
        // }
        // rbfm->readAttribute(fh, rd, rid, this->attrName, attrData);
        
        while(this->rm_iter.getNextTuple(rid, data) == 0){
            std::cout << "RID IS " << rid.pageNum << ", " << rid.slotNum << "\n";
            fflush(stdout);
        }
    }
    else{
        std::cout << "Error type not found.\n";
    }
    return QE_EOF;
}

// ... the rest of your implementations go here

Project::Project(TableScan *input, const vector<string> &attrNames){
    printf("In Project: TableScan\n");
    // RelationManager &rm;
    // RM_ScanIterator *iter;
    // string tableName;
    // vector<Attribute> attrs;
    // vector<string> attrNames;
    // RID rid;

    vector<string> newAttrNames;
    string tableName = input->tableName;
    // recordDescriptor = input -> attrs;
    
    for(string attrName : attrNames){
        std::cout << "Attribute Name is: " << attrName << "\n";
        int pos = attrName.find(".");
        string sub = attrName.substr (pos+1);
        newAttrNames.push_back(sub);
    }
    for(string attrName : newAttrNames){
        std::cout << "New Attribute Name is: " << attrName << "\n";
    }
    std::cout << "The table name is " << tableName << "\n";
    

    
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc;
    string fileName = (input->tableName) + ".t";
    rc = rbfm->openFile(fileName, this->fh);
    if(rc != 0){
        printf("we goofed");
        exit(-1);
    }
    //condition attr is ignored when doing NO_OP
    rbfm->scan(this->fh, input->attrs, "", NO_OP, NULL, newAttrNames, this->iter);

    this->rd = input->attrs;
    this->attrName = newAttrNames;

    
    //   RC scan(FileHandle &fileHandle,
    //   const vector<Attribute> &recordDescriptor,
    //   const string &conditionAttribute,
    //   const CompOp compOp,                  // comparision type such as "<" and "="
    //   const void *value,                    // used in the comparison
    //   const vector<string> &attributeNames, // a list of projected attributes
    //   RBFM_ScanIterator &rbfm_ScanIterator);
    
    fflush(stdout);
}

Project::Project(IndexScan *input, const vector<string> &attrNames){
    printf("In Project: IndexScan\n");
    fflush(stdout);
}

Project::Project(Iterator *input, const vector<string> &attrNames){
    printf("In Project: Iterator\n");
    fflush(stdout);
}

RC Project::getNextTuple(void *data) {
    RID rid;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    void* rec = malloc(PAGE_SIZE);
    // Grab next tuple using rbfm
    if(this->iter.getNextRecord(rid, rec) != 0){
        return QE_EOF;  
    }
    std::cout << "RID is: " << rid.pageNum << ", " << rid.slotNum << std::endl;

    unsigned offset = 0;
    for(int i = 0; i<1; i++){
        int zero = '0';
        memcpy(data+offset, &zero, 1);
        offset += 1;
    }
    //offset == 8;
    for(int i = 0; i < attrName.size(); ++i){
        rbfm->readAttribute(fh, rd, rid, attrName.at(i), rec);
        float temp; 
        memcpy(&temp, rec+1, 4);
        // std::cout << "val: " << temp << "\n";
        memcpy(data+offset, rec+1, 4);
        offset+=4;
    } 
    free(rec);
    return SUCCESS;
}


INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    //for each tuple l in L do
    //    for each tuple r in R do
    //        if l and r satisfy the join condition then
    //            add tuple <l,r> to the result
    printf("INLJoin: Iterator, IndexScan\n");
    fflush(stdout);
}

INLJoin::INLJoin(TableScan *leftIn, IndexScan *rightIn, const Condition &condition) {
    //for each tuple l in L do
    //    for each tuple r in R do
    //        if l and r satisfy the join condition then
    //            add tuple <l,r> to the result
    printf("INLJoin: TableScan, IndexScan\n");
    fflush(stdout);

    // TableScan:
    // RelationManager &rm;
    // RM_ScanIterator *iter;
    // string tableName;
    // vector<Attribute> attrs;
    // vector<string> attrNames;
    // RID rid;
    
    // IndexScan:
    // RelationManager &rm;
    // RM_IndexScanIterator *iter;
    // string tableName;
    // string attrName;
    // vector<Attribute> attrs;
    // char key[PAGE_SIZE];
    // RID rid;

    for(Attribute attr: leftIn->attrs){
        std::cout << "Left attr name is: " << attr.name << "\n";
    }
    for(Attribute attr: rightIn->attrs){
        std::cout << "Right attr name is: " << attr.name << "\n";
    }


}

INLJoin::INLJoin(IndexScan *leftIn, IndexScan *rightIn, const Condition &condition) {
    //for each tuple l in L do
    //    for each tuple r in R do
    //        if l and r satisfy the join condition then
    //            add tuple <l,r> to the result
    printf("INLJoin: IndexScan, IndexScan\n");
    fflush(stdout);
}

RC INLJoin::getNextTuple(void *data) {
    // • read in from left
    // • Grab the right tuple, if eof we try to read from the left again
    // • Parse the fields of the right tuple
    // • Perform the comparison
    // • If comparison is valid, we join the right tuple with our cached left tuple
    return QE_EOF;
}
