

#include <iostream>
#include <fstream>
#include "MV_OSTM.cpp"
#include <pthread.h>
#include <unistd.h>
#include <atomic>
#include <sys/time.h>
#define NUM_THREADS 2

#define NO_OF_TRANX 6400
#define NO_OF_OPS_PER_TRANX 10
#define MAX_KEY  1000000/*operation ket range OKR*/
#define INTRA_TRANX_DELAY 100

#define LOOKUP 10
#define INSERT 80
#define DELETE 10

using namespace std;

/*Creating an object of MV_OSTM class.*/
MV_OSTM* lib = new MV_OSTM();

/*Transaction Limit atomic counter to keep track of dynamic allocation os transactions.*/
atomic<int> TranxLimit;

/*Variables to keep track of Total aborts.*/
atomic<int> AbortCnt;

/*Files to log data in.*/
fstream file10runGTOD, filenumaborts;

/*To keep track of total time taken.*/
double timer[64];

/*Function to get time.*/
double timeRequest() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  double timevalue = tp.tv_sec + (tp.tv_usec/1000000.0);
  return timevalue;
}

/**
 * Function invoked by each thread to execute one transaction.
 **/
OPN_STATUS mainApplication()
{
    L_txlog* txlog;
    OPN_STATUS ops, commitStatus = ABORT;
    int* val = new int;
	bool retry =  true;
      
    while(retry == true)
    {
           
		txlog = lib->begin();
         
        // this_thread::sleep_for(std::chrono::milliseconds(rand()%exec_duration_ms));
         
        for(int i = 0; i < NO_OF_OPS_PER_TRANX; i++)
        {
            //generate randomly one of insert, lookup, delete.
            int opn = rand()%100;
           
            /*Look for the operation to perform.*/
			if(opn < INSERT) {
				opn  = 0;
            } else if(opn < (INSERT+DELETE)) {
                opn = 1;
			} else {
				opn = 2;
            }
            /*Find the key to be operated upon.*/
            int key = rand()%MAX_KEY;
                
            if(0 == opn) {
				/*INSERT*/
                ops = lib->tx_insert(txlog, key, 100);//inserting value = 100
            } else if(1 == opn) {
                /*DELETE*/
                ops = lib->tx_delete(txlog, key, val);
            } else {
                /*LOOKUP*/
                ops = lib->tx_lookup(txlog, key, val);
            }
		}
        /*If no aborts in performing operations then commit the transaction by making changes in the shared memory.*/
        if(ABORT != ops)
        {
            commitStatus = lib->tryCommit(txlog);
        }
        /*If commit method results in an abort then increase the abort count variable and execute the transaction again.*/
        if(ABORT == commitStatus)
        {
			AbortCnt++;
            retry = true;
        } else {
            retry = false;
        }
	}
    /*Return status to the thread.*/
    return commitStatus; 
}

/*Function called by each thread.*/
void* worker(void *ptr_id)
{
	int id = *((int*)ptr_id);
	double btime,etime;
	int transLmt = TranxLimit.fetch_add(1);
		
	// Execute this loop until the transLt number of transactions execute successfully
	while(transLmt < NO_OF_TRANX) {
		//begin time.
		btime = timeRequest();
		
		//critical section.
		mainApplication();
		
		//end time.
		etime = timeRequest();
		timer[id] = timer[id] + (etime - btime);
		transLmt = TranxLimit.fetch_add(1);
		// Sleep for random time between each operation. Simulates the thread performing some operation
		//usleep(rand()%INTRA_TRANX_DELAY);
	}// End for transLt
}

/*Mian function execution starts from here.*/
int main()
{
	double start = 0.0;
	double end = 0.0;
	double sumTime = 0.0;
    double totalTime = 0.0;
	long totalAborts = 0;
	int threadId[64], k = 0;
    int repeatForAvg = 0;
    
	while(k<64)
	{
		timer[k] = 0;
		threadId[k] = k;
		k++;
	}
    TranxLimit.store(0);
	AbortCnt.store(0);
	
	cout<<" INSERT:"<<INSERT<<"\n DELETE: "<<DELETE<<"\n LOOKUP: "<<LOOKUP<<"\n";
	
	pthread_t threads[NUM_THREADS];
	start = timeRequest();
//	while(repeatForAvg<5) {
  //      /*Assign each thread task to be done.*/
		for (int i=0; i < NUM_THREADS; i++) {
			pthread_create(&threads[i], NULL, worker, &threadId[i]);
		}
		
		//only after all the threads join, the parent has to exit
		for(int i=0; i< NUM_THREADS; i++) {
			  pthread_join(threads[i],NULL); 
		}
	end = timeRequest();	
		repeatForAvg++;
		k =0;
		totalTime = 0.0;
		while(k<64)
		{
			//cout<<timer[k]<<" ";
			totalTime += timer[k];
			timer[k] = 0;
			k++;
		}
		//totalTime /= NO_OF_TRANX;
		sumTime += totalTime;
		totalAborts += AbortCnt.load();
		TranxLimit.store(0);
		AbortCnt.store(0);
	//}
	cout<<"TOTAL TIME:"<<end-start<<endl;
    cout<<"TOTAL ABORTS:"<<totalAborts/1<<endl;
	
	/*Write the results in the respective files.*/
    filenumaborts.open("numaborts.txt", fstream::out | fstream::app);
    file10runGTOD.open("runGTOD.txt", fstream::out | fstream::app);
    filenumaborts<<totalAborts/5<<endl;
    file10runGTOD<<sumTime/5<<endl; // in sec
  
    pthread_exit(NULL);

}

