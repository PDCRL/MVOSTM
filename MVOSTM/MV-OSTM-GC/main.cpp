/*
*DESCRIPTION    :   Main Test Application
*COMPANY        :   IIT Hyderabad
*/

#include <iostream>
#include <fstream>
#include "MV_OSTM.cpp"
#include <pthread.h>
#include <atomic>
#define MAX_KEY 1000

using namespace std;
//Variables define to log abort counts.
atomic<bool> ready (false);
atomic<unsigned long int> txCount (0);
atomic<unsigned long int> rvaborts (0);
atomic<unsigned long int> updaborts (0);

MV_OSTM* lib = new MV_OSTM();
//Local variable to set the # of threads and I/D/L percentages.
uint_t num_threads = 64;
uint_t prinsert = 45, prlookup = 10, prdelete = 45;
uint_t num_op_per_tx = 10;
int testSize = 1;
thread *t;

/*************************Barrier code begins*****************************/
std::mutex mtx;
std::condition_variable cv;
bool launch = false;

/*
* Barrier to sychronize all threads after creation.
*/
void wait_for_launch()
{
	std::unique_lock<std::mutex> lck(mtx);
	while (!launch) cv.wait(lck);
}

/*
* Let threads execute their task after final thread has arrived.
*/
void shoot()
{
	std::unique_lock<std::mutex> lck(mtx);
	launch = true;
	cv.notify_all();
}

/*
 * The funstion which is invoked by each thread once all threads are notified.
 */
OPN_STATUS app1()
{
    L_txlog* txlog;
    OPN_STATUS ops, txs = ABORT;
    int* val = new int;
    
    int ts_idx = 0;
    while (ts_idx < testSize)
    {
        bool retry =  true;
        while(retry == true)
        {
			txlog = lib->begin();

			for(int i = 0; i < num_op_per_tx; i++)
            {
                //generate randomly one of insert, lookup, delete.
                int opn = rand()%100;
                if(opn < prinsert)
					opn  = 0;
                else if(opn < (prinsert+prdelete))
                    opn = 1;
                else
                    opn = 2;
                    
                int key = rand()%MAX_KEY;
				if(0 == opn){ 
                    /*INSERT*/
                    ops = lib->tx_insert(txlog, key, 100);//inserting value = 100
                } else if(1 == opn) {
					/*DELETE*/
                    ops = lib->tx_delete(txlog, key, val);
                } else {
                    /*LOOKUP*/
                    ops = lib->tx_lookup(txlog, key, val);
				}

                if(ABORT == ops) {
                     rvaborts++;
                     //cout<<"Aborted";
                     retry = true;
                     break;
                } 
            }

            if(ABORT != ops)
            {
                txs = lib->tryCommit(txlog);
                if(ABORT == txs) {
					//cout<<"Aborted";
                    retry = true;
                    updaborts++;
                } else {
                    retry = false;
                }
            }
        }
        txCount++;
        ts_idx++;
    }
return txs;
}

/*
* DESCP:	worker for threads that call ht's function as per their distribution.
*/
void worker(uint_t tid)
{
	//barrier to synchronise all threads for a coherent launch
	wait_for_launch();
	app1();
}

int main(int argc, char **argv)
{
    double duration;
	struct timeval start_time, end_time;

	try {   
	time_t tt;
	srand(time(&tt));

    t = new std::thread [num_threads];

	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i] = std::thread(worker, i);
	}

	gettimeofday(&start_time, NULL);
	//notify all threads to begin the worker();
	shoot(); 
    ready = true;

	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i].join();
	}
	gettimeofday(&end_time, NULL);
    cout<<"#total aborts      :"<<(rvaborts+updaborts)<<endl;
 
	duration = (end_time.tv_sec - start_time.tv_sec);
	duration += (end_time.tv_usec - start_time.tv_usec)/ 1000000.0;
	cout<<"time..: "<<duration<<" seconds"<<std::endl;
	cout<<"Memory Consumed : "<<G_cnt.load()<<endl;
	} catch(const std::system_error& e) {
        std::cout << "Caught system_error with code " << e.code() 
                  << " meaning " << e.what() << '\n';
    }
 	return 0;
}

