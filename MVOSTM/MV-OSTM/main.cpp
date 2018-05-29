

#include <iostream>
#include <fstream>
#include "MV_OSTM.cpp"
#include <pthread.h>
// #include <stdio.h>
//#include <stdlib.h>
#include <atomic>
//#include <chrono>



#define CMD_ARG 0
#define APP1 1
#define MULTI_OP 1
std::atomic<bool> ready (false);
std::atomic<unsigned long int> txCount (0);
std::atomic<unsigned long int> rvaborts (0);
std::atomic<unsigned long int> updaborts (0);
using namespace std;
MV_OSTM* lib = new MV_OSTM();
HashMap* hasht  = lib->hash_table;


//DEFAULT VALUES
#define MAX_KEY  1000 /*operation ket range OKR*/

uint_t num_threads = 2; /*multiple of 10 the better, for exact thread distribution */
uint_t prinsert = 25, prlookup = 50, prdelete = 25;
uint_t num_op_per_tx = 10;
int testSize = 1;

uint_t exec_duration_ms = 1000;
/*should sum upto 100*/
uint_t num_insert_percent = 0;
uint_t num_delete_percent = 0;
uint_t num_lookup_percent = 0;
uint_t num_move_percent = 100;

uint_t num_insert, num_delete, num_lookup, num_move;


std::thread *t;

std::mutex mtxc;
int num_del_aborts = 0, num_ins_aborts = 0, num_lookup_aborts = 0, num_mov_aborts = 0;
fstream file10runGTOD, filenumaborts, filenumaborts1, filenumaborts2, fileThroughput;

/*************************Barrier code begins*****************************/
std::mutex mtx;
std::mutex pmtx; // to print in concurrent scene
std::condition_variable cv;
bool launch = false;


void wait_for_launch()
{
	std::unique_lock<std::mutex> lck(mtx);
	//printf("locked-waiting\n");
	while (!launch) cv.wait(lck);
}


void shoot()
{
	std::unique_lock<std::mutex> lck(mtx);
	launch = true;
	//printf("main locked-notify\n");
	cv.notify_all();
}
/*************************Barrier code ends*****************************/



OPN_STATUS add_init(uint_t key, uint_t thid)
{
    L_txlog* txlog;
    OPN_STATUS ops, txs;
    txlog = lib->begin();

//    uint_t key = rand()%(MAX_KEY - 1) + 1;
    uint_t val = rand()%(MAX_KEY - 1) + 1;
    ops = lib->tx_insert(txlog, key, val);//inserting value = 100

//    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
//    pmtx.unlock();

    if(ABORT != ops)
        txs = lib->tryCommit(txlog);

    msg.clear();
//    pmtx.lock();
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
//    pmtx.unlock();

    return txs;
}


OPN_STATUS add(uint_t key, uint_t val, uint_t thid)
{
    L_txlog* txlog;
    OPN_STATUS ops, txs;

    txlog = lib->begin();

//    uint_t key = rand()%(MAX_KEY - 1) + 1;
    ops = lib->tx_insert(txlog, key, 100);//inserting value = 100

//    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
//    pmtx.unlock();

    if(ABORT != ops)
        txs = lib->tryCommit(txlog);

        msg.clear();
//    pmtx.lock();
//    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
//    pmtx.unlock();

    return txs;
}


OPN_STATUS look(uint_t key, uint_t thid)
{

    L_txlog* txlog;
    OPN_STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();

//    uint_t key = rand()%(MAX_KEY - 1) + 1;
    ops = lib->tx_lookup(txlog,key, val);

//    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase lookup [key:thid]       \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase lookup [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
//    pmtx.unlock();

    if(ABORT != ops) //execute only if all mths succeed in rv-method execution phase for this Tx
    {
        txs = lib->tryCommit(txlog);
    }

    msg.clear();
 //   stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();

    return txs;
}


OPN_STATUS del(uint_t key, uint_t thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    L_txlog* txlog;
    OPN_STATUS ops, txs = ABORT;
    int* val = new int;

    txlog = lib->begin();

//    uint_t key = rand()%(MAX_KEY - 1) + 1;
    ops = lib->tx_delete(txlog, key, val);//inserting value = 100

//    pmtx.lock();
    stringstream msg;
    msg<<" rv-phase delete [key:thid]       \t"<<key <<":"<<thid<<endl;
    msg<<" rv-phase delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
    cout<<msg.str();
//    pmtx.unlock();

    if(ABORT != ops)
    {
        txs = lib->tryCommit(txlog);
    }

    msg.clear();
//    pmtx.lock();
//    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
//    pmtx.unlock();

    return txs;

}


OPN_STATUS multilook(int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    L_txlog* txlog;
    OPN_STATUS ops, txs = ABORT;
    int* val = new int;
    bool retry =  true;

for(int j = 0; j < 100; j++)
{
    retry =  true;
    while(retry == true)
    {

        txlog = lib->begin();

        for(int i = 0; i < num_op_per_tx; i++)
        {

            uint_t key = rand()%(MAX_KEY - 1) + 1;
            ops = lib->tx_lookup(txlog, key, val);

        #if DEBUG_LOGS
        //      pmtx.lock();
            stringstream msg;
            msg<<" rv-phase lookup [key:thid]       \t"<<key <<":"<<thid<<endl;
            msg<<" rv-phase lookup [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
            cout<<msg.str();
        //       pmtx.unlock();
        #endif // DEBUG_LOGS
            if(ABORT == ops)
            {
                break;
            }

        }

        if(ABORT != ops) //execute only if all mths succeed in rv-method execution phase for this Tx
        {
            txs = lib->tryCommit(txlog);
        }

        if(ABORT == ops || ABORT == txs)
        {
            mtxc.lock();
            num_lookup_aborts++;
            mtxc.unlock();
            retry =  true;
        }
        else
        {
            retry = false;
        }
    }
}//number times the Tx per thread
    #if DEBUG_LOGS
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    #endif // DEBUG_LOGS

    return txs;
}

OPN_STATUS multiadd(int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    L_txlog* txlog;
    OPN_STATUS ops, txs;
    int* val = new int;
    bool retry =  true;

for(int j = 0; j < 100; j++)
{
    retry =  true;
    while(retry == true)
    {

        txlog = lib->begin();

        for(int i = 0; i < num_op_per_tx; i++)
        {

            uint_t key = rand()%(MAX_KEY - 1) + 1;

            ops = lib->tx_insert(txlog, key, 100);

     #if DEBUG_LOGS
     //       pmtx.lock();
            stringstream msg;
            msg<<" rv-phase insert [key:thid]       \t"<<key <<":"<<thid<<endl;
            msg<<" rv-phase insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
            cout<<msg.str();
     //       pmtx.unlock();
     #endif // DEBUG_LOGS
            if(ABORT == ops)
            {
                //continue;
                break;
            }

        }


        //  cout<<"op status "<<ops<<endl;

        if(ABORT != ops)
            txs = lib->tryCommit(txlog);

        if(ABORT == ops || ABORT == txs)
        {
            mtxc.lock();
            num_ins_aborts++;
            mtxc.unlock();
            retry = true;
        }
        else
        {
            retry = false;
        }
    }
}

    #if DEBUG_LOGS
    //pmtx.lock();
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
   // pmtx.unlock();
   #endif // DEBUG_LOGS

    return txs;
}

OPN_STATUS muldel(int thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    L_txlog* txlog;
    OPN_STATUS ops, txs = ABORT;
    int* val = new int;
    bool retry =  true;

for(int j = 0; j < 100; j++)
{
    retry =  true;
    while(retry == true)
    {

        txlog = lib->begin();

        for(int i = 0; i < num_op_per_tx; i++)
        {

            uint_t key = rand()%(MAX_KEY - 1) + 1;

//            ops = lib->t_lookup(txlog, 0, key, val);
//            if(OK == ops)
                ops = lib->tx_delete(txlog, key, val);//inserting value = 100

          #if DEBUG_LOGS
          //  pmtx.lock();
            stringstream msg;
            msg<<" rv-phase delete [key:thid]       \t"<<key <<":"<<thid<<endl;
            msg<<" rv-phase delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
            cout<<msg.str();
         //   pmtx.unlock();
          #endif
            if(ABORT == ops)
            {
                break;
            }

        }

        if(ABORT != ops)
        {
            txs = lib->tryCommit(txlog);
        }

        if(ABORT == ops || ABORT == txs)
        {
            mtxc.lock();
            num_del_aborts++;
            mtxc.unlock();
            retry = true;
        }
        else
        {
            retry = false;
        }
    }
}

   #if DEBUG_LOGS
   // pmtx.lock();
    stringstream msg;
    msg<<" commit [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
   // pmtx.unlock();
   #endif

return txs;

}

OPN_STATUS mov(uint_t key1, uint_t key2, uint_t val1, uint_t val2, uint_t thid)
{
//    pthread_mutex_lock(&print_mtx);
//    cout<<"\t\t key "<<key<<" thread id "<<thid<<"::"<<endl;
//    pthread_mutex_unlock(&print_mtx);

    L_txlog* txlog;
    OPN_STATUS ops = ABORT, txs = ABORT;
    int* val = new int;
    bool retry =  true;

   for(int i = 0 ; i < 100; i++)
   {
        uint_t key1 = rand()%(MAX_KEY - 1) + 1;
        uint_t val1 = rand()%(MAX_KEY - 1) + 1;

        uint_t key2 = rand()%(MAX_KEY - 1) + 1;
        uint_t val2 = rand()%(MAX_KEY - 1) + 1;
        retry =  true;
        ops = OK, txs = COMMIT;

    while(retry == true)
    {

        txlog = lib->begin();


        ops = lib->tx_delete(txlog, key1, val);//inserting value = 100

     #if DEBUG_LOGS
      //  pmtx.lock();
        stringstream msg;
        msg<<" rv-phase move-delete [key:thid]       \t"<<key1 <<":"<<thid<<endl;
        msg<<" rv-phase move-delete [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
        cout<<msg.str();
      //  pmtx.unlock();

        msg.clear();
          #endif // DEBUG_LOGS

        if(ABORT != ops && (FAIL != ops)) //check for ops Fail, if fail no need to insert.
        {
            ops = lib->tx_insert(txlog, key2, 100);//inserting value = 100

            #if DEBUG_LOGS
    //        pmtx.lock();
    //        stringstream msg;
            msg<<" rv-phase move-insert [key:thid]       \t"<<key2 <<":"<<thid<<endl;
            msg<<" rv-phase move-insert [op status: thid]\t "<<status(ops) <<":"<<thid <<endl;
            cout<<msg.str();
    //        pmtx.unlock();
    #endif // DEBUG_LOGS
        }


        if(ABORT != ops)
        {
            txs = lib->tryCommit(txlog);
        }

        if(ABORT == ops || ABORT == txs)
        {
            mtxc.lock();
            num_mov_aborts++;
            mtxc.unlock();
            retry = true;
        }
        else
        {
            retry = false;
        }
    }
   }

    #if DEBUG_LOGS
    msg.clear();
    //pmtx.lock();
//    stringstream msg;
    msg<<" commit move [op status: thid]\t "<< status(txs) <<":"<<thid <<endl<<endl;
    cout<<msg.str();
    //pmtx.unlock();
    #endif // DEBUG_LOGS

return txs;

}

OPN_STATUS app1()
{
    L_txlog* txlog;
    OPN_STATUS ops, txs = ABORT;
    int* val = new int;
    
    int ts_idx = 0;
   // while(!ready)
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
                {
                    if(opn < prinsert)
                        opn  = 0;
                    else if(opn < (prinsert+prdelete))
                        opn = 1;
                    else
                    {
                        opn = 2;
                    }

                }
   // cout<<"opn:"<<opn<<"\n";
    //            int opn = rand()%3;

                uint_t key = rand()%(MAX_KEY - 1) + 1;

                if(0 == opn)
                {
                    /*INSERT*/
                    ops = lib->tx_insert(txlog, key, 100);//inserting value = 100
                }
                else if(1 == opn)
                {
                    /*DELETE*/
                    ops = lib->tx_delete(txlog, key, val);
                }
                else
                {
                    /*LOOKUP*/
                    ops = lib->tx_lookup(txlog, key, val);

                }

                if(ABORT == ops)
                  {
                     rvaborts++;
                     retry = true;
                     break;
                  } 
            }

            if(ABORT != ops)
            {
                txs = lib->tryCommit(txlog);
                if(ABORT == txs)
                {
                    retry = true;
                    updaborts++;
                }
                else
                {
                    retry = false;
                }
            }

/*            if(ABORT == ops || ABORT == txs)
            {
                //mtxc.lock();
                //num_mov_aborts++;
                //mtxc.unlock();
                retry = true;
            }
            else
            {
                retry = false;
            }
            */
        }
        txCount++;
        ts_idx++;
    }

return txs;

}




void worker(uint_t tid)
{
	//barrier to synchronise all threads for a coherent launch :)
	wait_for_launch();

	if(tid < num_insert)
	{
        #if !(MULTI_OP)
        uint_t key = rand()%(MAX_KEY - 1) + 1;
        uint_t val = rand()%(MAX_KEY - 1) + 1;
        add(key, val, tid);
        std::cout<<"not multi--------------------------------------\n";
        #endif

		multiadd(tid);
	}
	else if(tid < (num_insert + num_delete ))
    {
        #if !(MULTI_OP)
        uint_t key = rand()%(MAX_KEY - 1) + 1;
        uint_t val;
        del(key, tid);
        #endif


        muldel(tid);
    }
	else if(tid < (num_insert + num_delete + num_lookup))//init for lookup threads
	{
        #if !(MULTI_OP)
        uint_t key = rand()%(MAX_KEY - 1) + 1;
        look(key, tid);
        #endif

		multilook(tid);
	}
	else if(tid < (num_insert + num_delete + num_lookup + num_move))//init for lookup threads
	{
        uint_t key1 = rand()%(MAX_KEY - 1) + 1;
        uint_t val1 = rand()%(MAX_KEY - 1) + 1;

        uint_t key2 = rand()%(MAX_KEY - 1) + 1;
        uint_t val2 = rand()%(MAX_KEY - 1) + 1;

		//mov(key1, key2, val1, val2, tid);
        //cout<<"app1"<<endl;
        //while(!ready)
        {
            app1();
          //  txCount++;
        }

	}
	else
	{
		std::cout<<"something wrong in thread distribution to operations"<<std::endl;
	}
}

int main(int argc, char **argv)
{

    double duration;
	struct timeval start_time, end_time;

	if((num_insert_percent + num_delete_percent + num_lookup_percent + num_move_percent) != 100)
	{
		std::cout<<"Oo LaLa! Seems you got arithmatic wrong :) #operations should sumup to 100" <<std::endl;
		return 0;
	}

	time_t tt;
	srand(time(&tt));

	num_insert = (uint_t)ceil((num_insert_percent*num_threads)/100);
	num_delete = (uint_t)ceil((num_delete_percent*num_threads)/100);
	num_lookup = (uint_t)ceil((num_lookup_percent*num_threads)/100);
	num_move = (uint_t)ceil((num_move_percent*num_threads)/100);


    #if CMD_ARG
    if(argc <= 6)
    {
        cout<<"enter #threads, #insert, #delete, #lookup, #mov"<<endl;
    }

    num_threads = atoi(argv[1]);


    #if APP1
    prinsert = (uint_t)ceil((atoi(argv[2])*num_threads)/100);
    prdelete = (uint_t)ceil((atoi(argv[3])*num_threads)/100);
    prlookup = (uint_t)ceil((atoi(argv[4])*num_threads)/100);
    num_move = (uint_t)ceil((atoi(argv[5])*num_threads)/100);
    #else
    num_insert = (uint_t)ceil((atoi(argv[2])*num_threads)/100);
    num_delete = (uint_t)ceil((atoi(argv[3])*num_threads)/100);
    num_lookup = (uint_t)ceil((atoi(argv[4])*num_threads)/100);
    num_move = (uint_t)ceil((atoi(argv[5])*num_threads)/100);
    #endif // APP1
    #endif // CMD_ARG

    t = new std::thread [num_threads];

	std::cout<<" num_insert:"<<num_insert<<"\n num_delete: "<<num_delete<<"\n num_lookup: "<<num_lookup<<"\n num_move: "<<num_move<<"\n";

	if((num_insert + num_delete + num_lookup + num_move) > num_threads)
	{
	//	std::cout<<"((insertNum + delNum + lookupNum) > number_of_threads)"<<std::endl;
		return 0;
	}

	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i] = std::thread(worker, i);
	}

    std::cout<<" intial table: \n";


    //hasht->printTable();
    //hasht->printBlueTable();

	std::cout <<"\n********STARTING...\n";
	gettimeofday(&start_time, NULL);
	shoot(); //notify all threads to begin the worker();

    //this_thread::sleep_for(std::chrono::milliseconds(exec_duration_ms));
    ready = true;

	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i].join();
	}
	gettimeofday(&end_time, NULL);
	std::cout <<"\n********STOPPING...\n";

	//hasht->printTable();
	//hasht->printBlueTable();

    //cout<<"#ins aborts      :"<<num_ins_aborts<<endl;
    cout<<"#rv aborts      :"<<rvaborts<<endl;
    cout<<"#updaborts   :"<<updaborts<<endl;
    //cout<<"#mov aborts      :"<<num_mov_aborts<<endl;
    cout<<"#total aborts      :"<<(rvaborts+updaborts)<<endl;
    //cout<<(exec_duration_ms/1000.0)<<endl;
    cout<<"#num_op_per_tx   :"<<num_op_per_tx<<endl;
    cout<<"#test size      :"<<testSize<<endl;

	duration = (end_time.tv_sec - start_time.tv_sec);
	duration += (end_time.tv_usec - start_time.tv_usec)/ 1000000.0;
	std::cout<<"time..: "<<duration<<"seconds"<<std::endl;
    std::cout<<"#threads: "<<num_threads<<"---> #insert:"<<num_insert<<" #delete: "<<num_delete<<" #lookup: "<<num_lookup<<" #move: "<<num_move<<"\n";
    cout<<"---> #prinsert:"<<prinsert<<" #prdelete: "<<prdelete<<" #lookup: "<<prlookup<<"\n";

    cout<<"\n\n#txCount :"<<fixed<<txCount <<" time:(sec) "<<duration<<" txCount/s: "<<(double)txCount/(double)(duration/*/1000.0*/)<<endl;// throughput per sec

	filenumaborts.open("numaborts.txt", fstream::out | fstream::app);
    filenumaborts1.open("numabortsrv.txt", fstream::out | fstream::app);
    filenumaborts2.open("numabortsupd.txt", fstream::out | fstream::app);
    file10runGTOD.open("runGTOD.txt", fstream::out | fstream::app);
    fileThroughput.open("runThroughput.txt", fstream::out | fstream::app);
   

    filenumaborts<<(rvaborts+updaborts)<<endl;
    filenumaborts1<<(rvaborts)<<endl;
    filenumaborts2<<(updaborts)<<endl;
    file10runGTOD<<fixed<<duration<<endl; // in sec
    fileThroughput<<fixed<<(double)txCount/(double)(duration/*/1000.0*/)<<endl;

	return 0;
}

