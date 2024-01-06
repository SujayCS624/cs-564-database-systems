#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


/*
    Input : Address of integer variable frame
    Output : Populating the int frame with frameNumber using clock Algorithm and returns the appropriate status. Status : returns OK if we found a frame successfully, returns UNIXERR if unable to remove a page from hashtable,  or if unable to write a dirty page back to disk, returns BUFFEREXCEEDED if the clock algorithm is unable to find a frame in all iterations.
*/
const Status BufMgr::allocBuf(int & frame) 
{
    // Implementation of the clock Algorithm.
    for(int j = 0; j < 2; j++) {
        for(int i = 0; i < numBufs; i++) {
            // increase the clockHand
            advanceClock();
            // get meta data at that clockHand
            bool isValid = bufTable[clockHand].valid;
            bool isRefBit = bufTable[clockHand].refbit;
            int pinCnt = bufTable[clockHand].pinCnt;
            bool isDirty = bufTable[clockHand].dirty;
            // if valid bit is set.
            if(isValid){
                // if ref bit is set.
                if(isRefBit){
                    // resetting the ref bit.
                    bufTable[clockHand].refbit = false;
                    continue;
                }
                else{
                    // check if pinCnt is greater than zero.
                    if(pinCnt > 0){
                        continue;
                    }
                    else{
                        // check if Dirty bit is set
                        if(isDirty){
                            // write the page to the file.
                            if(bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[bufTable[clockHand].frameNo]) == OK) {
                                // remove the corresponding hash table entry.
                                if(hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo) == OK) {
                                    bufTable[clockHand].Clear();
                                    frame = clockHand;
                                    return OK;
                                }
                                else {
                                    return UNIXERR;
                                }
                            }
                            else {
                                return UNIXERR;
                            }
                        }
                        else {
                            // if dirty bit is not set, directly remove from hash table.
                            hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                            bufTable[clockHand].Clear();
                            frame = clockHand;
                            return OK;
                        }
                    }
                }
            }
            else{
                hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                frame = clockHand;
                return OK;
            }

        }
    }
    return BUFFEREXCEEDED;
}

/*
    Input : File pointer, Page number to be read from the file, page pointer that will be populated by read page.
    Output : Returns populated Page Pointer and appropriate status., Status : returns OK if page is read successfully, returns HASHTBLERROR if unable to insert this entry into hash table, returns UNIXERR if unable to read the page from file or if unable to allocate a buffer frame due to UNIXERR, returns BUFFEREXCEEDED if unable to allocate a frame after iterating through all the frames using clock algorithm.
*/
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    // check if the page is in hash table.
    Status hashTableLookupStatus = hashTable->lookup(file, PageNo, frameNo);
    if(hashTableLookupStatus == OK) {
        // set the ref bit, increment the pinCnt, return the page.
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }
    else {
        // if page is not in hash table, allocate a buffer frame.
        Status allocBufStatus = allocBuf(frameNo);
        if(allocBufStatus == OK) {
            // read page from file into that buffer frame.
            Status readPageStatus = file->readPage(PageNo,&bufPool[frameNo]);
            if(readPageStatus == OK) {
                // insert this entry into the hash table.
                Status hashTableInsertStatus = hashTable->insert(file, PageNo, frameNo);
                if(hashTableInsertStatus == OK) {
                    // populate page pointer.
                    bufTable[frameNo].Set(file, PageNo);
                    page = &bufPool[frameNo];
                    return OK;
                }
                else {
                    return HASHTBLERROR;
                }
            }
            else {
                return UNIXERR;
            }
        }
        else if(allocBufStatus == UNIXERR) {
            return UNIXERR;
        }
        else {
            return BUFFEREXCEEDED;
        }
    }
}

/*
    Input : File pointer, Page Number in the file, dirty bit to be set if true.
    Output : Returns OK status if unpin operation is successful, or returns PAGENOTPINNED if pin count was already 0, returns HASHNOTFOUND if the page itself isn't present in the hash table.
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    // check if the page is in the hash table.
    Status hashTableLookupStatus = hashTable->lookup(file, PageNo, frameNo);
    if(hashTableLookupStatus == OK) {
        // check if pinCnt is greater than zero.
        if(bufTable[frameNo].pinCnt > 0) {
            // set the dirty bit if true, and decrement the pinCnt.
            if(dirty == true) {
                bufTable[frameNo].dirty = true;
            }
            bufTable[frameNo].pinCnt--;
            return OK;
        }
        else {
            return PAGENOTPINNED;
        }
    }
    
    return HASHNOTFOUND;
}

/*
    Input : File pointer, Page Number to be populated, Page Pointer to be populated.
    Output : Populates Page Number using file->allocatePage(), populates page pointer to point to the frameNo in the bufferPool, returns the appropriate status. Status : returns OK if able to allocate a page successfully, returns HASHTBLERROR if we encounter an error when inserting (file, pageNo, frameNo) into the hash table, returns BUFFEREXCEEDED if unable to allocate a frame after iterating through all the frames using clock algorithm, returns UNIXERR if unable to allocate  a page or if unable to allocate a buffer frame due to UNIXERR.
*/
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    int frameNo;
    Status allocatePageStatus;
    Status allocBufStatus;
    Status hashTableInsertStatus;

    // allocate a new page in the file and populate pageNo.
    allocatePageStatus = file->allocatePage(pageNo);

    if(allocatePageStatus == OK) {
        // allocate a frame number for this page.
        allocBufStatus = allocBuf(frameNo);

        if(allocBufStatus == OK) {
            // insert this entry into hash table.
            hashTableInsertStatus = hashTable->insert(file, pageNo, frameNo);

            if(hashTableInsertStatus == OK) {
                // populate Page pointer.
                bufTable[frameNo].Set(file, pageNo);
                page = &bufPool[frameNo];
                return OK;
            }
            else if(hashTableInsertStatus == HASHTBLERROR) {
                return HASHTBLERROR;
            }
        }
        else if(allocBufStatus == BUFFEREXCEEDED) {
            return BUFFEREXCEEDED;
        }
        else if(allocBufStatus == UNIXERR) {
            return UNIXERR;
        }
    }
    return UNIXERR;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


