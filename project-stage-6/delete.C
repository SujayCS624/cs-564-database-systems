#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	Status status;
	// Create a new object of HeapFileScan class
	HeapFileScan* scan = new HeapFileScan(relation, status);

	// If no predicate, then delete all tuples in the relation
	if(attrValue == NULL) {
		status = scan->startScan(0, 0, STRING, attrValue, EQ);
		if(status != OK) {
			return status;
		}		
	}
	// If predicate is provided, initialize startScan with the appropriate arguments
	else {
		AttrDesc attrDesc;
		status = attrCat->getInfo(relation, attrName, attrDesc);
		if (status != OK)
		{
			return status;
		}
		int searchVal;
		float searchVal2;
		if(attrDesc.attrType == 1) {
			searchVal = atoi(attrValue);
			attrValue = (char*)&searchVal;
		}
		else if(attrDesc.attrType == 2) {
			searchVal2 = atof(attrValue);
			attrValue = (char*)&searchVal2;
		}
		status = scan->startScan(attrDesc.attrOffset, attrDesc.attrLen, (Datatype) attrDesc.attrType, attrValue, op);
		if(status != OK) {
			return status;
		}
	}

	RID rec2Rid;

	// Iterate all records satisfying the predicate
	while(scan->scanNext(rec2Rid) == OK) {
		// Delete the record
		status = scan->deleteRecord();
		if ((status != OK)  && ( status != NORECORDS))
		{
			return status;
		} 
	}

	return OK;
}


