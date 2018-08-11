//
//  dataStructure.cpp
//  MV-OSTM
//
//  Created by Chirag Juyal on 4/5/18.
//  Copyright © 2018 IIT-HYD. All rights reserved.
//

#include "dataStructure.hpp"

/*
 * Constructor of the class G_node.
 */
G_node::G_node(int key)
{
	this->G_key = key;
    this->red_next = NULL;
    this->blue_next = NULL;
    this->G_mark = DEFAULT_MARKED;
    //push the default version back to the G_node.
    version *T0_version = new version;
    T0_version->G_ts = 0;
    T0_version->G_val = 0;
    this->G_vl->push_back(T0_version);
    G_cnt++;
}
/*
 * Method to compare two G_nodes on the basis of their keys.
 */
bool G_node::compareG_nodes(G_node* node)
{
    if(this->G_key == node->G_key)
    {
        return true;
    }
    return false;
}

/**********************************************************************/

/*
 * Constructor of the class HashMap.
 */
HashMap::HashMap()
{
    htable = new G_node* [TABLE_SIZE];
    /*Initialize head and tail sentinals nodes for all 
     * the buckets of HashMap.*/
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        htable[i] = NULL;
        htable[i] = new G_node(INT_MIN);
        htable[i]->red_next = new G_node(INT_MAX);
        htable[i]->blue_next = htable[i]->red_next;
    }
}

/*
 * Destructor of the class HashMap.
 */
HashMap::~HashMap()
{
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        if (htable[i] != NULL)
        {
            G_node *prev = NULL;
            G_node *entry = htable[i];
            while (entry != NULL)
            {
                prev = entry;
                entry = entry->red_next;
                delete prev;
            }
        }
    }
    delete[] htable;
}

/*
 * Function to define the object Id or bucket number.
 */
int HashMap::HashFunc(int key)
{
    return key % TABLE_SIZE;
}

/*
 *Insert Key-Node in the appropriate position with a default version.
 */
void HashMap::list_Ins(vector<G_node*> *lockedNodes, int L_key, int *L_val, G_node** G_preds, 
					   G_node** G_currs, LIST_TYPE list_type, 
					   int L_tx_id)
{
    //Inserting the node from redList to blueList
    if(RL_BL == list_type)
    { 
		//cout<<"INSERT 1";
		if(G_currs[0]->G_mark == true)
		{
			G_currs[0]->G_mark = false;
			G_currs[0]->blue_next = G_currs[1];
			G_preds[0]->blue_next = G_currs[0];
			assert(L_key == G_currs[0]->G_key);
		}
    }
    //Inserting the node in redlist only.
    else if(RL == list_type)
    {
		//cout<<"INSERT 2";
		G_node *node = new G_node(L_key);
        //After creating node lock it.
        node->lmutex.lock();
        //Add current transaction to the rv-list of the 0th version.
        node->G_vl->at(0)->G_rvl->push_back(L_tx_id);
        node->G_mark = true;
        node->red_next = G_currs[0];
        //node->blue_next = G_currs[1];
        //Find the node is locked or not ?
		for(int i=0;i<lockedNodes->size();i++)
		{
			if(lockedNodes->at(i)->G_key == G_currs[0]->G_key)
			{
				//If yes then erase it from the locked list.
				lockedNodes->erase(lockedNodes->begin()+i);
				//Unlock the G_node.
				G_currs[0]->lmutex.unlock();
				break;
			}
		}
        G_currs[0] = node;
        G_preds[1]->red_next = G_currs[0];
     }
    //Inserting the node in red as well as blue list.
    else
    {
		//cout<<"INSERT 3";
		G_node *node = new G_node(L_key);
        //After creating node lock it.
        node->lmutex.lock();
        //Add another version with current transaction's timestamp.
        version *T_key_version = new version;
        T_key_version->G_ts = L_tx_id;
        if(L_val != NULL)
            T_key_version->G_val = *L_val;
        else
            T_key_version->G_val = -1;
        //Push the current timestamp version to the node's RV list.
        node->G_vl->push_back(T_key_version);
        node->red_next = G_currs[0];
        node->blue_next = G_currs[1];
        G_preds[1]->red_next = node;
        G_preds[0]->blue_next = node;
    }
 }

/*
 * Funtion to determine preds and currs.
 */
OPN_STATUS HashMap::list_LookUp(vector<G_node*> *lockedNodes, int L_bucket_id, int L_key, 
								G_node** G_preds, G_node** G_currs)
{
    OPN_STATUS op_status = RETRY;
    G_node *head = NULL;
    
    //If key to search is not in the range b/w -infinity to +infinity.
    if((L_key <= INT_MIN) && (L_key >= INT_MAX))
    {
        assert((L_key > INT_MIN) && (L_key < INT_MAX));
    }
    
    //Run until status of operation doesn't change from RETRY.
    while(RETRY == op_status)
    {
        /*Get the head of the bucket in chaining hash-table with the 
         * help of L_obj_id and L_key.*/
        //if bucket is empty
        if (htable[L_bucket_id] == NULL)
        {
            cout<<"bucket is empty \n";
            return BUCKET_EMPTY;
        }
        else
        {
            head = htable[L_bucket_id];
        }
        
        G_preds[0] = head;
        G_currs[1] = G_preds[0]->blue_next;
        
        //search blue pred and curr
        while(G_currs[1]->G_key < L_key)
        {
            G_preds[0] = G_currs[1];
            G_currs[1] = G_currs[1]->blue_next;
        }
        G_preds[1] = G_preds[0];
        G_currs[0] = G_preds[0]->red_next;
        
        //search red pred and curr
        while(G_currs[0]->G_key < L_key)
        {
            G_preds[1] = G_currs[0];
            G_currs[0] = G_currs[0]->red_next;
        }
        
        //Acquire locks on all preds and currs.
        op_status = acquirePredCurrLocks(lockedNodes, G_preds, G_currs);
		
		if(RETRY == op_status)
        {
			continue;
		}
        //validation
        op_status = methodValidation(G_preds, G_currs);
        
        /*If op_status is still RETRY and not OK release all the aquired 
         * locks and try again.*/
        if(RETRY == op_status)
        {
            releasePredCurrLocks(lockedNodes, G_preds, G_currs);
        }
    }
    
    #if DEBUG_LOGS
    cout<<"list_LookUp:: nodes " <<G_preds[0]->G_key<<" "<< 
		G_preds[1]->G_key<<" "<<G_currs[0]->G_key<<" "<<
		G_currs[1]->G_key<<endl;
    #endif // DEBUG_LOGS
    return op_status;
}

/*
 * Identify the right version of a G_node that is largest but less than 
 * current transaction id.
 */
version* HashMap::find_lts(int L_tx_id, G_node *G_curr)
{
	bool flag = false;
    //Initialize the closest tuple.
    version *closest_tuple = new version();
    closest_tuple->G_ts = 0;
    
    /*For all the versions of G_currs[] identify the largest timestamp 
     * less than L_tx_id.*/
    for(int i=0; i<G_curr->G_vl->size();i++)
    {
        int p = G_curr->G_vl->at(i)->G_ts;
        if((p < L_tx_id) && (closest_tuple->G_ts <= p))
        {
            closest_tuple->G_ts = G_curr->G_vl->at(i)->G_ts;
            closest_tuple->G_val = G_curr->G_vl->at(i)->G_val;
            closest_tuple->G_rvl = G_curr->G_vl->at(i)->G_rvl;
            flag = true;
        }
    }
    //If no version is find to be read from.
    if(flag == false) 
    {
		closest_tuple = NULL;
	}
    return closest_tuple;
}

/*
 * This method does below steps ->
 * 1. Check if the key exists or not, if yes then check for the LTS 
 *    version to read from.
 * 2. Else create the key and its respective version to be read from.
 * 3. If version to be read from is found then add current transaction 
 * 	  to its RV list.
 */

OPN_STATUS HashMap:: commonLuNDel(vector<G_node*> *lockedNodes, int L_tx_id, int obj_id, int L_key, 
								  int* L_val, G_node** G_preds, 
								  G_node** G_currs)
{
    //Operation status to be returned.
    OPN_STATUS L_opn_status;
    /*If node corresponding to the key is not present in local log then 
     * search into underlying DS with the help of listLookUp().*/
    L_opn_status = list_LookUp(lockedNodes, obj_id,L_key,G_preds,G_currs);
    
    //If node corresponding to the key is part of BL.
    if(G_currs[1]->G_key == L_key)
    {
		if(G_currs[1]->G_vl->at(0)->G_ts > L_tx_id)
		{
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return ABORT;
		}
		
        version *closest_tuple = find_lts(L_tx_id,G_currs[1]);
        if(closest_tuple == NULL)
        { 
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return ABORT;
		}
        closest_tuple->G_rvl->push_back(L_tx_id);
        
        /*If the Key-Node mark field is TRUE then L_op_status and 
         * L_val set as FAIL and NULL otherwise set OK and value of 
         * closest_tuple respectively.*/
        if(closest_tuple->G_val == -1)
        {
			L_opn_status = FAIL;
            L_val = NULL;
        } else {
            L_opn_status = OK;
            L_val = &(closest_tuple->G_val);
        }
    }
    //If node corresponding to the key is part of RL.
    else if(G_currs[0]->G_key == L_key)
    {
		if(G_currs[0]->G_vl->at(0)->G_ts > L_tx_id)
		{
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);	
			return ABORT;
		}
        version *closest_tuple = find_lts(L_tx_id,G_currs[0]);
        if(closest_tuple == NULL)
        {
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return ABORT;
		}
        closest_tuple->G_rvl->push_back(L_tx_id);
        
        /*If the Key-Node mark field is TRUE then L_op_status and L_val 
         * set as FAIL and NULL otherwise set OK and value of 
         * closest_tuple respectively.*/
        if(closest_tuple->G_val == -1)
        {
			L_opn_status = FAIL;
            L_val = NULL;
        } else {
            L_opn_status = OK;
            L_val = &(closest_tuple->G_val);
        }
    }
    /*If node corresponding to the key is not part of RL as well as BL 
     * then create the node into RL with the help of list_Ins().*/
    else {
		//Insert the version tuple for transaction T0.
        list_Ins(lockedNodes, L_key, L_val, G_preds, G_currs, RL, L_tx_id);
        //Push the new node lock in the lock list record.
        isNodeLocked(lockedNodes, G_currs[0]);
        L_opn_status = OK;
        int x = -1;
        L_val = &x;
    }
    
    //Releasing the locks in the incresing order.
    releasePredCurrLocks(lockedNodes, G_preds, G_currs);
    return L_opn_status;
}

/*
 *Method to find the closest tuple created by transaction Tj with the 
 * largest timestamp smaller than L_ix_id.
 */
bool HashMap::check_version(int L_tx_id, G_node* G_curr)
{
    //Identify the tuple that has highest timestamp but less than itself.
    if(G_curr->G_vl->at(0)->G_ts > L_tx_id)
		 return false;
    version *closest_tuple = find_lts(L_tx_id,G_curr);
    if(closest_tuple == NULL)
    {
			return false;
	}
    if(closest_tuple != NULL)
    {
        for(int i=0; i<closest_tuple->G_rvl->size();i++)
        {
            int Tk_ts = closest_tuple->G_rvl->at(i);
            /*If in RV list there exists a transaction with higher ts 
             * then the current transaction then return false;*/
            if(L_tx_id < Tk_ts)
            {
                return false;
            }
        }
    }
    /*If in the rv list all the transactions are of lower ts then the 
     * current transaction then return true.*/
    return true;
}

/*
 * Method to add a version in the appropriate key_node version list in sorted order.
 */
void HashMap::insertVersion(int L_tx_id, int L_val, G_node* key_Node)
{
	cout<<"Inserting a version\n";
    version *newVersion = new version();
    newVersion->G_ts = L_tx_id;
    newVersion->G_val = L_val;

    /*Lock the live list.*/
    lockLiveList->lock();
    /*Check if there are more than zero elements in the live set, if yes then take the first element as it is the least live timestamp.*/
    if(liveList->size() != 0 && liveList->at(0) == L_tx_id)
    {
		G_cnt.fetch_sub(key_Node->G_vl->size());
		/*Clear all the versions and create a new version.*/
		key_Node->G_vl->clear();
	}
		
    for(int i=0;i<key_Node->G_vl->size();i++)
    {
        /*Look for an appropriate place to put the new version in orer to maintain the order of the version list.*/
        if(key_Node->G_vl->at(i)->G_ts > L_tx_id)
        {
            key_Node->G_vl->insert(key_Node->G_vl->begin()+i, newVersion);        
			/*unLock the live list.*/
			lockLiveList->unlock();
			G_cnt++;
            return;
        }
    }
    /*Else push at the last.*/
    key_Node->G_vl->push_back(newVersion);
    G_cnt++;
    /*unLock the live list.*/
	lockLiveList->unlock();
}


/*
 *Function to delete a node from blue link and place it in red 
 * link after marking it.
 */
void HashMap::list_Del(G_node** G_preds, G_node** G_currs)
{
    G_preds[0]->blue_next = G_currs[1]->blue_next;
    //G_currs[0]->blue_next = NULL;
}

/**
 * Print the current table contents.
 **/
void HashMap::printHashMap(int L_bucket_id)
{
	G_node *head = NULL;
	
	if (htable[L_bucket_id] == NULL)
    {
       elog("bucket is empty \n");
       return;
    }
    else
    {
       head = htable[L_bucket_id];
    }
       
    while(head->G_key < INT_MAX)
    {
		cout<<to_string(head->G_key)+"\n";
		head = head->red_next;
    }
}
/**********************************************************************/

/*
 *Function to acquire all locks taken during listLookUp().
 */
OPN_STATUS acquirePredCurrLocks(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs)
{
	if(isNodeLocked(lockedNodes, G_preds[0]) == false)
	{
		if(RETRY == methodValidation(G_preds, G_currs))
		{	
			removePredsCurrs(lockedNodes,G_preds[0]);	
			return RETRY;
		}
		G_preds[0]->lmutex.lock();
	}
    if(isNodeLocked(lockedNodes, G_preds[1]) == false)
    {
    	if(RETRY == methodValidation(G_preds, G_currs))
    	{
			removePredsCurrs(lockedNodes,G_preds[1]);
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return RETRY;
		}
		G_preds[1]->lmutex.lock();
	}
	if(isNodeLocked(lockedNodes, G_currs[0]) == false)	
	{
		if(RETRY == methodValidation(G_preds, G_currs))
    	{
			removePredsCurrs(lockedNodes,G_currs[0]);
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return RETRY;
		}
		G_currs[0]->lmutex.lock();
	}
	if(isNodeLocked(lockedNodes, G_currs[1]) == false)
	{
		if(RETRY == methodValidation(G_preds, G_currs))
    	{
			removePredsCurrs(lockedNodes,G_currs[1]);
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return RETRY;
		}
		G_currs[1]->lmutex.lock();
	}
	return OK;
}

/*
 *Function to release all locks taken during listLookUp().
 */
void releasePredCurrLocks(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs)
{
	removePredsCurrsFromLockList(lockedNodes,G_preds,G_currs);
}

/*
 *This method identifies the conflicts among the concurrent methods of 
 * different transactions.
 */
OPN_STATUS methodValidation(G_node** G_preds, G_node** G_currs)
{
    /*Validating g_pred[] and G_currs[].*/
    if((G_preds[0]->G_mark) || (G_currs[1]->G_mark) || 
		(G_preds[0]->blue_next->G_key != G_currs[1]->G_key) || 
		(G_preds[1]->red_next->G_key != G_currs[0]->G_key))
		return RETRY;
    else
        return OK;
}
/*
 * Function to find a node among locked nodes.
 * */
bool isNodeLocked(vector<G_node*> *lockedNodes, G_node* g_node)
{
	//Find the node is locked or not ?
	for(int i=0;i<lockedNodes->size();i++)
	{
		if(lockedNodes->at(i)->G_key == g_node->G_key)
		{
			return true;
		} else if(lockedNodes->at(i)->G_key > g_node->G_key)
		{
			lockedNodes->insert(lockedNodes->begin()+i,g_node);
			return false;
		}
	}
	//If node is not found to be locked then add it into the list and return false.
	lockedNodes->push_back(g_node);
	return false;
}
/*
 * Method to delete the preds and currs locked by the transaction 
 * from the locked list.
 * */
void removePredsCurrsFromLockList(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs)
{
	//Find the node is locked or not ?
	for(int i=0;i<lockedNodes->size();i++)
	{
		if(lockedNodes->at(i)->G_key == G_preds[0]->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			//Unlock the G_node.
			G_preds[0]->lmutex.unlock();
			i--;
			continue;
		}
		if(lockedNodes->at(i)->G_key == G_preds[1]->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			//Unlock the G_node.
			G_preds[1]->lmutex.unlock();
			i--;
			continue;
		}
		if(lockedNodes->at(i)->G_key == G_currs[0]->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			G_currs[0]->lmutex.unlock();
			i--;
			continue;
		}
		if(lockedNodes->at(i)->G_key == G_currs[1]->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			G_currs[1]->lmutex.unlock();
			i--;
			continue;
		}
	}
}

/*
 * Method to delete the preds and currs locked by the transaction 
 * from the locked list.
 * */
void removePredsCurrs(vector<G_node*> *lockedNodes, G_node* g_node)
{
	//Find the node is locked or not ?
	for(int i=0;i<lockedNodes->size();i++)
	{
		if(lockedNodes->at(i)->G_key == g_node->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			return;
		}
	}
}
