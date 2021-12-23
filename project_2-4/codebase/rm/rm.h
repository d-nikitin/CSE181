
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstring>
#include <iostream>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  unsigned count = 0;
  vector<RID> rids = {};
  FileHandle fileHandle;
  string tableName;
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextTuple(RID &rid, void *data) ;
  RC close() { return SUCCESS;};
  void operator++() {count++;};
  void operator--() {count--;};
  RID getRID(){return rids.at(count);};
  RC checkCurrent(){return count < rids.size() ? 0:RBFM_EOF;};
  RC checkNext(){return (count + 1) < rids.size() ? 0:RBFM_EOF;};
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  int tableID = 3;
  vector<Attribute> tableAttributes;
  vector<Attribute> columnAttributes;

  RC insertTable(const string &tableName, int32_t id);
  RC insertColumns(int32_t id, const vector<Attribute> &recordDescriptor);
  RC getTableID(const string &tableName, int32_t &tableID);
  RC systemTableCheck(const string &tableName);
};

#endif
